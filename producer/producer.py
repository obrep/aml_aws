import boto3
import time
import os
import json


FILENAME = "replay.csv"
REGION = "us-east-1"

input_columns = [
    "type",
    "amount",
    "nameOrig",
    "balance_source_old",
    "balance_source_new",
    "nameDest",
    "balance_dest_old",
    "balance_dest_new",
]


def get_records_from_s3(bucket: str, s3_path: str, local_path: str):
    print(f"Downloading from S3: {bucket}/{path} to {local_path}")
    s3_client = boto3.client("s3")
    s3_client.download_file(bucket, s3_path, local_path)


def main():
    ssm_client = boto3.client("ssm", REGION)
    bucket = (
        ssm_client.get_parameter(Name="/aml_project/bucket")
        .get("Parameter", {})
        .get("Value")
    )
    prefix = (
        ssm_client.get_parameter(Name="/aml_project/replay_prefix")
        .get("Parameter", {})
        .get("Value")
    )
    stream_name = (
        ssm_client.get_parameter(Name="/aml_project/stream_name")
        .get("Parameter", {})
        .get("Value")
    )

    if not os.path.isfile(FILENAME):
        print(f"Did not find {FILENAME} locally")
        get_records_from_s3(bucket, prefix, FILENAME)

    kinesis_client = boto3.client("kinesis", REGION)
    print("Setup completed. Starting event production\n")

    with open(FILENAME) as f:
        for _ in range(100):
            record = f.readline()
            input_dict = dict(zip(input_columns, record.strip("\n").split(",")))

            resp = kinesis_client.put_record(
                StreamName=stream_name, Data=json.dumps(input_dict), PartitionKey="1"
            )
            print(resp)
            time.sleep(0.1)


if __name__ == "__main__":
    main()
