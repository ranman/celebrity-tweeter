import boto3
from fleece.xray import monkey_patch_botocore_for_xray
monkey_patch_botocore_for_xray()
from botocore.vendored import requests
import twitter
import base64
import os
import json

rek = boto3.client('rekognition')
ddb = boto3.resource('dynamodb').Table(os.getenv("DDB_TABLE"))
ssm = boto3.client('ssm')

# we grab our credentials from SSM
api = twitter.Api(*ssm.get_parameters(Names=[os.getenv("SSM_PARAMETER_NAME")])['Parameters'][0]['Value'].split(','))
# we grab our screen name from the API so we have som portability
TWITTER_SN = api.VerifyCredentials().screen_name


def validate_record(payload):
    # if this tweet is meant for us, it has a picture,
    # and it's not a retweet then it's good
    if (
        TWITTER_SN in payload.get('text', '') and
        payload.get('entities', {}).get('media') and
        'RT' not in payload.get('text')
    ):
        return True
    return False


def process_record(payload):
    # we grab the high res version of the picture
    img = requests.get(payload['entities']['media'][0]['media_url']+":large")
    # we pass that content to rekognition
    celebs = rek.recognize_celebrities(Image={'Bytes': img.content})['CelebrityFaces']
    handles = []
    for celeb in celebs:
        ddb_resp = ddb.get_item(Key={"id": celeb['Id']})
        if ddb_resp.get('Item'):
            handles.append(ddb_resp['Item']['handle'])
        else:
            handles.append(celeb['Name'])
    if handles:
        status = "@{} That looks like a picture of {}".format(payload['user']['screen_name'], ', '.join(handles))
    else:
        status = "@{} There was no one famous in your picture! #lame".format(payload['user']['screen_name'])
    api.PostUpdate(status, in_reply_to_status_id=payload['id'])


def lambda_handler(event, context):
    for record in event['Records']:
        # payloads from kinesis are base64 encoded json from stream.py
        payload = json.loads(base64.b64decode(record['kinesis']['data']))
        # we do a quick sanity check on the payload to make sure we want to respond to it
        if not validate_record(payload):
            return
        process_record(payload)
