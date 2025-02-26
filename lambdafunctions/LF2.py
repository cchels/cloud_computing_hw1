import json
import boto3
import random
import logging
import requests
import base64
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/202533503880/Q1"
DYNAMODB_TABLE_NAME = "yelp-restaurants"
ES_ENDPOINT = "https://search-restaurants-opensearch-7xzru44w4rv2q4pr4rsw5kkkb4.us-east-1.es.amazonaws.com" 
ES_INDEX = "restaurants"
REGION = "us-east-1"
SOURCE_EMAIL = "yirongwang03@gmail.com"

ES_USERNAME = "*******"
ES_PASSWORD = "*******"

sqs_client = boto3.client('sqs', region_name=REGION)
dynamodb = boto3.resource('dynamodb', region_name=REGION)
ses_client = boto3.client('ses', region_name=REGION)


def lambda_handler(event, context):
    logger.info("LF2 invoked. Checking SQS for new messages...")

    # Pull message from the SQS queue (Q1)
    response = sqs_client.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=5,
        WaitTimeSeconds=0,
        VisibilityTimeout=30
    )
    messages = response.get("Messages", [])
    if not messages:
        logger.info("No messages in Q1 right now.")
        return {"status": "No messages to process"}

    logger.info("Found %d messages in Q1", len(messages))

    # Process the message
    for msg in messages:
        receipt_handle = msg["ReceiptHandle"]
        try:
            body = json.loads(msg["Body"])
        except json.JSONDecodeError:
            logger.error("Invalid JSON in message body: %s", msg["Body"])
            sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            continue

        logger.info("Message body: %s", body)

        cuisine = body.get("cuisine")
        user_email = body.get("email")
        number_of_people = body.get("number_of_people")
        dining_date = body.get("dining_date")
        dining_time = body.get("dining_time")

        if not cuisine or not user_email:
            logger.warning("Missing cuisine or email in message: %s", body)
            sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            continue

        # Query OpenSearch for restaurants in the user selected cuisine
        restaurant_ids = query_es_for_cuisine(cuisine)
        if not restaurant_ids:
            logger.info("No restaurants found in ES for cuisine: %s", cuisine)
            sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            continue

        # Pick 3 random restaurants from the query
        suggestions = random.sample(restaurant_ids, min(3, len(restaurant_ids)))

        # Fetch full details for the suggested restaurants from DynamoDB
        details_list = []
        for rid in suggestions:
            item = get_restaurant_details(rid)
            if item:
                details_list.append(item)

        # Format the email body
        email_body = format_suggestions_email(
            cuisine, 
            details_list, 
            number_of_people, 
            dining_date, 
            dining_time
        )

        # Send restaurant suggestions from SOURCE_EMAIL to user_email using SES
        try:
            ses_resp = ses_client.send_email(
                Source=SOURCE_EMAIL,
                Destination={"ToAddresses": [user_email]},
                Message={
                    "Subject": {"Data": f"{cuisine} Restaurant Suggestions"},
                    "Body": {
                        "Text": {"Data": email_body}
                    }
                }
            )
            logger.info("Email sent to %s. SES MessageId: %s", user_email, ses_resp["MessageId"])
        except ClientError as e:
            logger.error("SES error sending to %s: %s", user_email, e)

        # Delete the message from SQS
        sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
        logger.info("Deleted message from SQS for user: %s", user_email)

    return {"status": "Processing complete"}

# ------ Helper Functions ------

def get_es_headers():
    """Returns HTTP headers with OpenSearch Basic Authentication"""
    credentials = f"{ES_USERNAME}:{ES_PASSWORD}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    return {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/json"
    }

def query_es_for_cuisine(cuisine):
    """
    Query OpenSearch for all restaurants matching the given cuisine.
    Return a list of restaurant IDs.
    """
    query = {
        "size": 1000,
        "query": {
            "match": {
                "Cuisine": cuisine
            }
        }
    }
    url = f"{ES_ENDPOINT}/{ES_INDEX}/_search"

    headers = get_es_headers()
    try:
        resp = requests.get(url, headers=headers, json=query)
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        ids = [hit["_id"] for hit in hits]
        logger.info("Found %d restaurants in ES for cuisine %s", len(ids), cuisine)
        return ids
    except Exception as e:
        logger.error("Error querying ES: %s", e)
        return []

def get_restaurant_details(business_id):
    """
    Fetch the restaurant details from DynamoDB (yelp-restaurants).
    Return the item (dict) or None if not found.
    """
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    try:
        resp = table.get_item(Key={"business_id": business_id})
        return resp.get("Item")
    except ClientError as e:
        logger.error("DynamoDB get_item error: %s", e)
        return None

def format_suggestions_email(cuisine, details_list, number_of_people, dining_date, dining_time):
    """
    Format the email body for the suggestions email. 
    """
    lines = [
        f"Hello!\n\nHere are my {cuisine} restaurant suggestions for {number_of_people} people, "
        f"for {dining_date} at {dining_time}:\n"
    ]
    if not details_list:
        lines.append("Sorry, no suggestions found.\n")
    else:
        for i, rest in enumerate(details_list, 1):
            name = rest.get("name", "Unknown")
            address = rest.get("address", "Unknown address")
            lines.append(f"{i}. {name}, located at {address}")
        lines.append("\nEnjoy your meal!")

    return "\n".join(lines)