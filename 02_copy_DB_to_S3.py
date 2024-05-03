import boto3
import pandas as pd
from io import StringIO
from datetime import datetime



BUCKET_NAME = "weather-data1"
ROOT_FOLDER = "snowflake/"
s3 = boto3.client('s3')


def handle_insert(record):
    dict = {}

    for key, value in record['dynamodb']['NewImage'].items():
        for dt, col in value.items():
            dict.update({key: col})

    dff = pd.DataFrame([dict])
    return dff


def lambda_handler(event, context):
    df = pd.DataFrame()

    for record in event['Records']:
        table = record['eventSourceARN'].split("/")[1]

        if record['eventName'] == "INSERT":
            dff = handle_insert(record)
            df = pd.concat([df, dff])

    if not df.empty:
        print(df)

        csv_buffer = StringIO()
        df.to_csv(csv_buffer,index=False)

        date = str(datetime.now()).replace(" ", "_")
        key = ROOT_FOLDER + table + "_" + date + ".csv"
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=csv_buffer.getvalue())

    print('Successfully processed %s records.' % str(len(event['Records'])))