import json
import boto3
import base64
import os
from datetime import datetime
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from botocore.exceptions import ClientError
from inflection import singularize

REGION = 'us-west-2'
HOST = 'search-photos-gtmh5ztmt7n6syg2qsk2ovtt3u.us-west-2.es.amazonaws.com'
INDEX = 'photos'
BUCKET_NAME = "cloud-computing-ass2-b2-photos"

# Define the client to interact with Lex
s3 = boto3.client('s3')
lex_client = boto3.client('lexv2-runtime')

# def plural_to_singular(word):
#     p = inflect.engine()
#     return p.singular_noun(word) or word

def lambda_handler(event, context):
    # TODO implement
    print("Updated Lambda v1.6")
    print("event is: ",event)
    # print("context is: ",context)
    
    # msg_from_user = "show me dogNo7"
    msg_from_user = ""
    if "queryStringParameters" in event:
        # Parse the 'body' as JSON
        parameters = event['queryStringParameters']
        print("parameters in event is: ",parameters)
        if "q" in parameters:
            msg_from_user = parameters["q"]
    print("message from user is: ",msg_from_user)
    
    # Extract objects from user message
    msg_from_lex = extract_objects(msg_from_user)
    print("message from lex is: ",msg_from_lex)
    objects = []

    if len(msg_from_lex) > 0:
        print("message from user is: ",msg_from_lex[0])
        objects = []
        if "content" in msg_from_lex[0]:
            objects = msg_from_lex[0]["content"].split(" ")
            objects = [singularize(item) for item in objects if item != "None"]
        print("The objects extracted from user message is ", objects)
    
    if objects != []:
        # Query data in OpenSearch index ("photos")
        results = query(" ".join(objects))
        print("The query results is ",results)
        
        urls = []
        for result in results:
            file_name = result.get("objectKey")
            print("The file name is: ",file_name)
            url = f"https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{file_name}"
            print("The url is: ", url)
            urls.append(url)

        # s3.generate_presigned_url(
        #     'get_object',
        #     Params={
        #         'Bucket': BUCKET_NAME,
        #         'Key': file_name
        #     },
        #     ExpiresIn=3600  # URL expiration time in seconds (adjust as needed)
        # )

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
            },
            'body': json.dumps({"urls": urls}),
        }

        # file_obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_name)
        # file_content = file_obj["Body"].read()
        # return {
        #     'statusCode': 200,
        #     # "headers": {
        #     #     "Content-Type": "application/png",
        #     #     "Content-Disposition": "attachment; filename={}".format(file_name)
        #     # },
        #     "body": base64.b64encode(file_content),
        #     "isBase64Encoded": True
        #     # 'body': json.dumps({'status':'Successfully query data from OpenSearch!', 'event':event})
        # }
    
    return {
            'statusCode': 403,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
            },
            'body': json.dumps({'Error':'Fail to query data from OpenSearch!'})
        }
    
def extract_objects(msg_from_user):
    # Initiate conversation with Lex
    response = lex_client.recognize_text(
            botId='M5H0MSR5QP', # MODIFY HERE
            botAliasId='2C1USRAGKN', # MODIFY HERE
            localeId='en_US',
            sessionId='testuser',
            text=msg_from_user)
    
    msg_from_lex = response.get('messages', [])
    return msg_from_lex

def query(term):
    q = {'size': 5, 'query': {'multi_match': {'query': term}}}

    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)

    res = client.search(index=INDEX, body=q)

    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source'])

    return results
                        
def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)