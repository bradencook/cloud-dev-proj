import time
import json
import boto3
import argparse
import logging

logging.basicConfig(filename="consumer.log", level=logging.INFO)

def log(message):
    print(message)
    logging.info(message)


class RequestSource:
    def get_next(self):
        raise NotImplementedError


class S3RequestSource(RequestSource):
    def __init__(self, client, bucket):
        self.client = client
        self.bucket = bucket

    def get_next(self):
        response = self.client.list_objects_v2(Bucket=self.bucket, MaxKeys=1)
        if "Contents" not in response:
            return None

        key = response["Contents"][0]["Key"]
        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        self.client.delete_object(Bucket=self.bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    

class SQSRequestSource(RequestSource):
    def __init__(self, sqs_client, queue_url):
        self.sqs_client = sqs_client
        self.queue_url = queue_url
        self.cache = []


    def _fill_cache(self):
        response = self.sqs_client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )
        msgs = response.get("Messages", [])
        self.cache.extend(msgs)


    def get_next(self):
        if not self.cache:
            self._fill_cache()
            if not self.cache:
                return None

        msg = self.cache.pop(0)
        body = json.loads(msg["Body"])
        body["_receipt_handle"] = msg["ReceiptHandle"]
        return body


    def delete_processed(self, receipt_handle):
        self.sqs_client.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle,
        )


def get_next_request(client, bucket):
    response = client.list_objects_v2(Bucket=bucket, MaxKeys=1)
    if "Contents" in response:
        key = response["Contents"][0]["Key"]
        obj = client.get_object(Bucket=bucket, Key=key)
        client.delete_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    else:
        return None


def transform_widget(widget):
    # flatten attributes, normalize keys
    for attr in widget["otherAttributes"]:
        widget[attr["name"]] = attr["value"]
    del widget["otherAttributes"]
    widget["id"] = widget["widgetId"]
    del widget["widgetId"]
    del widget["requestId"]
    del widget["type"]
    return widget


def update_dynamodb(table, widget):
    table.put_item(Item=widget)
    log(f"Stored widget id={widget['id']} in DynamoDB table: {table.name}")


def delete_dynamodb(table, widget_id):
    table.delete_item(Key={"id": widget_id})
    log(f"Deleted widget id={widget_id} from DynamoDB table: {table.name}")


def update_s3(client, bucket, widget):
    owner = widget["owner"].replace(" ", "-").lower()
    key = f"widgets/{owner}/{widget['id']}"
    client.put_object(
        Bucket=bucket, Key=key, Body=json.dumps(widget).encode("utf-8")
    )
    log(f"Stored widget in S3 bucket {key}")


def delete_s3(client, bucket, widget_id):
    prefix = f"widgets/"
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for item in resp.get("Contents", []):
        if item["Key"].endswith(widget_id):
            client.delete_object(Bucket=bucket, Key=item["Key"])
            log(f"Deleted widget object: {item['Key']}")


def process_widget(widget, storage_backend):
    wtype = widget["type"]

    if wtype == "create":
        widget = transform_widget(widget)
        storage_backend["update"](widget)
        return widget

    elif wtype == "update":
        widget = transform_widget(widget)
        storage_backend["update"](widget)
        log(f"Updated widget id={widget['id']}")
        return widget

    elif wtype == "delete":
        wid = widget["widgetId"]
        storage_backend["delete"](wid)
        log(f"Deleted widget id={wid}")
        return {"deleted": wid}

    else:
        log(f"Skipping widget {widget.get('widgetId')} of unknown type: {wtype}")
        return "skipped"

def cl_parse():
    parser = argparse.ArgumentParser()

    # request source
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-rb", "--request-bucket")
    group.add_argument("-sq", "--sqs-queue")

    # destination
    dest = parser.add_mutually_exclusive_group(required=True)
    dest.add_argument("-dwt", "--dynamodb-widget-table")
    dest.add_argument("-wb", "--widget-bucket")

    return parser.parse_args()


def main():
    args = cl_parse()

    s3client = boto3.client("s3", region_name="us-east-1")

    # request source
    if args.request_bucket:
        source = S3RequestSource(s3client, args.request_bucket)
        sqs_source = None
    else:
        sqs_client = boto3.client("sqs", region_name="us-east-1")
        queue_url = sqs_client.get_queue_url(QueueName=args.sqs_queue)["QueueUrl"]
        source = SQSRequestSource(sqs_client, queue_url)
        sqs_source = source

    # destination
    if args.dynamodb_widget_table:
        table = boto3.resource("dynamodb", region_name="us-east-1").Table(
            args.dynamodb_widget_table
        )
        storage = {
            "update": lambda w: update_dynamodb(table, w),
            "delete": lambda wid: delete_dynamodb(table, wid),
        }
    else:
        storage = {
            "update": lambda w: update_s3(s3client, args.widget_bucket, w),
            "delete": lambda wid: delete_s3(s3client, args.widget_bucket, wid),
        }

    log("Consumer started")

    try:
        while True:
            widget = source.get_next()
            if not widget:
                time.sleep(0.1)
                continue

            receipt = widget.pop("_receipt_handle", None)

            process_widget(widget, storage)

            if receipt and sqs_source:
                sqs_source.delete_processed(receipt)

    except KeyboardInterrupt:
        log("Consumer stopped by user")

if __name__ == "__main__":
    main()
