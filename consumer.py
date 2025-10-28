import time
import json
import boto3
import argparse
import logging

logging.basicConfig(filename="consumer.log", level=logging.INFO)

def log(message):
    print(message)
    logging.info(message)

def get_next_request(client, bucket):
    response = client.list_objects_v2(Bucket=bucket, MaxKeys=1) # only gets a single key (should be the smallest value)
    if "Contents" in response:
        key = response["Contents"][0]["Key"]
        obj = client.get_object(Bucket=bucket, Key=key)
        client.delete_object(Bucket=bucket, Key=key)  # delete request after retrieving it
        return json.loads(obj["Body"].read().decode("utf-8"))
    else:
        return None


def update_dynamodb(table, widget):
    table.put_item(Item=widget)
    log(f"Stored widget id={widget['id']} in DynamoDB table: {table}")


def update_s3(client, bucket, widget):
    owner = widget["owner"].replace(" ", "-").lower()
    key = f"widgets/{owner}/{widget['id']}"
    client.put_object(Bucket=bucket, Key=key,
                           Body=json.dumps(widget).encode("utf-8"))
    log(f"Stored widget in S3 bucket {key}")

def cl_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("-rb", "--request-bucket", required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-dwt", "--dynamodb-widget-table")
    group.add_argument("-wb", "--widget-bucket")

    return parser.parse_args()


if __name__ == "__main__":
    args = cl_parse()
    s3client = boto3.client("s3", region_name="us-east-1")
    table = None
    if args.dynamodb_widget_table:  # adding to DynomoDB table
        table = boto3.resource("dynamodb", region_name="us-east-1").Table(args.dynamodb_widget_table)

    log("Consumer started")
    try:
        while True:
            widget = get_next_request(s3client, args.request_bucket)
            if widget:
                req_type = widget['type']
                if req_type == 'create':
                    # flatten otherAttributes
                    for attr in widget["otherAttributes"]:
                        widget[attr["name"]] = attr["value"]
                    del widget["otherAttributes"]
                    widget["id"] = widget["widgetId"]
                    del widget["widgetId"]
                    del widget["requestId"]
                    del widget["type"]
                    if table:  # adding to DynomoDB table
                        update_dynamodb(table, widget)
                    else:  # adding to an S3 bucket
                        update_s3(s3client, args.widget_bucket, widget)
                else:
                    log(f"Skipping widget {widget['widgetId']} of type: {req_type}")
            else:  # no widgets currently available
                time.sleep(0.1)
    except KeyboardInterrupt:
        log("\tConsumer stopped by user")

