import json
import boto3
import logging
import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

lex_client = boto3.client('lexv2-runtime', region_name='us-east-1')

BOT_ID = 'AZ45NIQPQK'
BOT_ALIAS_ID = 'TSTALIASID'
LOCALE_ID = 'en_US'

def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event))

    # Parse the API request
    body_data = {}
    if "body" in event and event["body"]:
        try:
            body_data = json.loads(event["body"])
        except Exception as e:
            logger.error("Error parsing event body: %s", str(e))
            return {
                "statusCode": 400,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
                    "Access-Control-Allow-Methods": "OPTIONS,POST"
                },
                "body": json.dumps(
                    {
                        "code": 400,
                        "message": "Invalid JSON in request body"
                    }
                )
            }
    else:
        body_data = event

    # Extract the text message from the API request
    messages_array = body_data.get("messages", [])
    if messages_array and len(messages_array) > 0:
        first_message = messages_array[0]
        unstructured = first_message.get("unstructured", {})
        user_id = unstructured.get("id", "defaultUser")
        input_text = unstructured.get("text", "")
    else:
        input_text = ""

    if not input_text:
        return {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            "body": json.dumps(
                {
                    "code": 400,
                    "message": "Missing text in 'messages[0].unstructured.text'"
                }
            )
        }

    # Call Lex V2 with input_text
    try:
        logger.info("Calling Lex V2 with input_text: %s", input_text)
        lex_response = lex_client.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId=LOCALE_ID,
            sessionId=user_id,
            text=input_text
        )
        logger.info("Lex response: %s", json.dumps(lex_response))
    except Exception as e:
        logger.error("Error calling Lex: %s", str(e))
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            "body": json.dumps(
                {
                    "code": 400,
                    "message": f"Error communicating with Lex: {str(e)}"
                }
            )
        }

    # Extract Lex's reply from lex_response
    lex_messages = lex_response.get("messages", [])
    if lex_messages:
        response_text = lex_messages[0].get("content", "No response from Lex.")
    else:
        response_text = "No response from Lex."

    # Send back the response from Lex as the API response
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods": "OPTIONS,POST"
        },
        "body": json.dumps({
            "messages": [
                {
                    "type": "unstructured",
                    "unstructured": {
                        "id": user_id,
                        "text": response_text,
                        "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    }
                }
            ]
        })
    }