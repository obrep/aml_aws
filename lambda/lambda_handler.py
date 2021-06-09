import base64
import json
from typing import Dict
import boto3

output_columns = [
    "amount",
    "balance_dest_new",
    "balance_dest_old",
    "balance_source_new",
    "balance_source_old",
    "is_cash_debit",
    "is_cash_in",
    "is_cash_out",
    "is_merchant_dest",
    "is_payment",
    "percentage_amount_source",
]

sagemaker_client = boto3.client("sagemaker-runtime")
ssm_client = boto3.client("ssm", "us-east-1")
sns_client = boto3.client("sns")
firehose_client = boto3.client("firehose")

endpoint_name = (
    ssm_client.get_parameter(Name="/aml_project/prediction_endpoint")
    .get("Parameter", {})
    .get("Value")
)
topic_arn = (
    ssm_client.get_parameter(Name="/aml_project/topic_arn")
    .get("Parameter", {})
    .get("Value")
)
firehose_name = (
    ssm_client.get_parameter(Name="/aml_project/firehose_name")
    .get("Parameter", {})
    .get("Value")
)


def create_empty_output_record() -> Dict:
    return dict(zip(output_columns, ["0"] * len(output_columns)))


def process_input(input_dict: dict) -> Dict:
    output_dict = create_empty_output_record()
    output_dict = {key: input_dict.get(key, val) for key, val in output_dict.items()}
    payment_type = input_dict["type"]
    if payment_type == "CASH_IN":
        output_dict["is_cash_in"] = "1"
    elif payment_type == "CASH_OUT":
        output_dict["is_cash_out"] = "1"
    elif payment_type == "PAYMENT":
        output_dict["is_payment"] = "1"
    elif payment_type == "DEBIT":
        output_dict["is_debit"] = "1"
    try:
        output_dict["percentage_amount_source"] = str(
            float(input_dict["amount"]) / float(input_dict["balance_source_old"])
        )
    except ZeroDivisionError:
        output_dict["percentage_amount_source"] = "0"

    output_dict["is_merchant_dest"] = "1" if input_dict["nameDest"][0] == "M" else "0"
    return output_dict


def get_fraud_probability(input_csv: str) -> float:
    response = sagemaker_client.invoke_endpoint(
        EndpointName=endpoint_name,
        Body=bytes(input_csv, encoding="utf-8"),
        ContentType="text/csv",
    )
    return float(json.loads(response["Body"].read()))


def lambda_handler(event, context):
    input_data = event["Records"][0]["kinesis"]["data"]
    input_dict = json.loads(base64.b64decode(input_data))

    output_dict = process_input(input_dict)

    prediction_input = ", ".join(output_dict.values())
    fraud_chance = get_fraud_probability(prediction_input)

    output_dict["fraud_chance"] = str(fraud_chance)
    output = json.dumps(output_dict)
    if fraud_chance > 0.95:
        print(f"High chance of fraud, sending notification. Chance: {fraud_chance}")
        sns_client.publish(
            TopicArn=topic_arn, Subject="Possible fraud detected", Message=output
        )
    csv_output = ", ".join(output_dict.values())
    firehose_client.put_record(
        DeliveryStreamName=firehose_name,
        Record={"Data": bytes(csv_output, encoding="utf-8")},
    )
