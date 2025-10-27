import json
import boto3
import argparse
import logging

logging.basicConfig(filename="consumer.log", level=logging.INFO)

def cl_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("-rb", "--request-bucket", required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-dwt", "--dynamodb-widget-table")
    group.add_argument("-wb", "--widget-bucket")

    return parser.parse_args()

def get_next_request(client, bucket):
    response = client.list_objects_v2(Bucket=bucket, MaxKeys=1)
    if "Contents" in response:
        key = response["Contents"][0]["Key"]
        obj = client.get_object(Bucket=bucket, Key=key)
        client.delete_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    else:
        return None


if __name__ == "__main__":
    args = cl_parse()
    s3client = boto3.client("s3", region_name="us-east-1")
    print(get_next_request(s3client, args.request_bucket))

