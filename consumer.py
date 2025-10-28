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

def update_s3(client, bucket, widget):
    owner = widget["owner"].replace(" ", "-").lower()
    key = f"widgets/{owner}/{widget['id']}"
    client.put_object(
        Bucket=bucket, Key=key, Body=json.dumps(widget).encode("utf-8")
    )
    log(f"Stored widget in S3 bucket {key}")

def process_next_widget(s3client, request_bucket, table=None, widget_bucket=None):
    widget = get_next_request(s3client, request_bucket)
    if not widget:
        return None

    if widget["type"] == "create":
        widget = transform_widget(widget)
        if table:
            update_dynamodb(table, widget)
        else:
            update_s3(s3client, widget_bucket, widget)
        return widget

    # skip non-create
    log(f"Skipping widget {widget.get('widgetId')} of type: {widget['type']}")
    return "skipped"

def cl_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("-rb", "--request-bucket", required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-dwt", "--dynamodb-widget-table")
    group.add_argument("-wb", "--widget-bucket")
    return parser.parse_args()

def main():
    args = cl_parse()
    s3client = boto3.client("s3", region_name="us-east-1")

    table = None
    if args.dynamodb_widget_table:
        table = boto3.resource("dynamodb", region_name="us-east-1") \
                     .Table(args.dynamodb_widget_table)

    log("Consumer started")
    try:
        while True:
            res = process_next_widget(
                s3client,
                args.request_bucket,
                table=table,
                widget_bucket=args.widget_bucket
            )
            if res is None:
                time.sleep(0.1)
    except KeyboardInterrupt:
        log("\tConsumer stopped by user")

if __name__ == "__main__":
    main()
