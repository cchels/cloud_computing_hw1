import json
import boto3
import logging
import datetime

from utils import elicit_slot, close, delegate, confirm_intent, validate_dining_suggestions, get_slot_value

sqs = boto3.client('sqs', region_name='us-east-1')
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/202533503880/Q1"
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
USERSTATE_TABLE = "UserState"
userstate_table = dynamodb.Table(USERSTATE_TABLE)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Main Intent Handler ---
def handle_greeting_intent(event):
    session_attributes = event.get('sessionState', {}).get('sessionAttributes', {})
    intent_name = event['sessionState']['intent']['name']

    user_id = event.get("sessionId", "defaultUser")
    logger.info(f"GreetingIntent triggered by user_id = {user_id}")

    # Check if the user already have a session. 
    response = userstate_table.get_item(Key={"UserID": user_id})
    item = response.get("Item")
    if item:        # User already have a session. 
        last_cuisine = item.get("lastCuisine", "some cuisine")
        last_location = item.get("lastLocation", "some location")
        last_date = item.get("lastDiningDate", "some date")
        last_time = item.get("lastDiningTime", "some time")
        message = (f"Welcome back! Last time, you searched for {last_cuisine} restaurants in {last_location}. "
                   f"Would you like a recommendation with the same keyword?")
        return confirm_intent(session_attributes, intent_name, message)
    else:           # User doesn't have a session.
        message = "Hi there, how can I help you today?"
        return close(session_attributes, intent_name, 'Fulfilled', message)

def handle_confirmation_yes(event):
    session_attributes = event.get('sessionState', {}).get('sessionAttributes', {})
    intent_name = event['sessionState']['intent']['name']

    user_id = event.get("sessionId", "defaultUser")
    response = userstate_table.get_item(Key={"UserID": user_id})
    item = response.get("Item")

    location = item.get("lastLocation")
    cuisine = item.get("lastCuisine")
    last_date = item.get("lastDiningDate", "")
    last_time = item.get("lastDiningTime", "")
    email = item.get("lastEmail", "")
    num_people = item.get("lastNumberOfPeople", "")

    # Push data from the last session to SQS
    payload = {
        "location": location,
        "cuisine": cuisine,
        "dining_date": last_date,
        "dining_time": last_time,
        "number_of_people": num_people,
        "email": email
    }
    try:
        sqs.send_message(QueueUrl=SQS_QUEUE_URL, MessageBody=json.dumps(payload))
        logger.info(f"Re-queued last search for user {user_id}: {cuisine} in {location} on {last_date} at {last_time}")
    except Exception as e:
        logger.error("Error re-queueing message: %s", e)

    # Fulfillment message
    message = (f"Sure! Another email for {cuisine} in {location} on {last_date} at {last_time} "
               f"will be sent to {email}.")
    return close(session_attributes, intent_name, 'Fulfilled', message)

def handle_confirmation_no(event):
    session_attributes = event.get('sessionState', {}).get('sessionAttributes', {})
    intent_name = event['sessionState']['intent']['name']
    message = "Alright, let me know if you'd like to start a new search."
    return close(session_attributes, intent_name, 'Fulfilled', message)

def handle_dining_suggestions_intent(event):
    slots = event['sessionState']['intent']['slots']
    session_attributes = event.get('sessionState', {}).get('sessionAttributes', {})
    intent_name = event['sessionState']['intent']['name']
    user_id = event.get("sessionId", "defaultUser")

    # Extract slot values
    location = get_slot_value(slots, "Location")
    cuisine = get_slot_value(slots, "Cuisine")
    dining_date = get_slot_value(slots, "DiningDate")
    dining_time = get_slot_value(slots, "DiningTime")
    number_of_people = get_slot_value(slots, "NumberOfPeople")
    email = get_slot_value(slots, "Email")

    # Validate the slots using the helper function from utils.py
    validation_result = validate_dining_suggestions(location, cuisine, dining_date, dining_time, number_of_people, email)
    if not validation_result['isValid']:
        return elicit_slot(session_attributes, 
                           intent_name, 
                           slots, 
                           validation_result['violatedSlot'], 
                           validation_result['message']['content'])

    # If the function is called in DialogCodeHook, delegate control back to Lex
    if event['invocationSource'] == 'DialogCodeHook':
        return delegate(session_attributes, intent_name, slots)

    # Push data to SQS after validation and confirmation
    payload = {
        "location": location,
        "cuisine": cuisine,
        "dining_date": dining_date,
        "dining_time": dining_time,
        "number_of_people": number_of_people,
        "email": email
    }
    try:
        sqs_response = sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(payload)
        )
        print("SQS send_message response:", sqs_response)
    except Exception as e:
        print("Error sending message to SQS:", e)
    
    # Store current user session state to DynamoDB
    try:
        userstate_table.put_item(Item={
            "UserID": user_id,
            "lastLocation": location,
            "lastCuisine": cuisine,
            "lastDiningDate": dining_date,
            "lastDiningTime": dining_time,
            "lastNumberOfPeople": number_of_people,
            "lastEmail": email,
            "timestamp": datetime.datetime.utcnow().isoformat()
        })
        logger.info(f"Stored last search for user {user_id}: {cuisine} in {location} on {dining_date} at {dining_time}, email={email}")
    except Exception as e:
        logger.error("Error storing user state: %s", e)

    # Fulfillment message
    message = (f"Your dining suggestion request for {number_of_people} people at a {cuisine} restaurant in {location} "
               f"on {dining_date} at {dining_time} has been received. We'll send recommendations to {email} shortly.")

    return close(session_attributes, intent_name, 'Fulfilled', message)

def handle_thank_you_intent(event):
    session_attributes = event.get('sessionState', {}).get('sessionAttributes', {})
    intent_name = event['sessionState']['intent']['name']
    message = "You're welcome!"
    return close(session_attributes, intent_name, 'Fulfilled', message)

def lambda_handler(event, context):
    """Main handler for incoming requests."""
    intent_name = event['sessionState']['intent']['name']

    # Route to the appropriate intent handler.
    if intent_name == "GreetingIntent":
        confirmation_state = event['sessionState']['intent']['confirmationState']
        if confirmation_state == "Confirmed":
            return handle_confirmation_yes(event)
        elif confirmation_state == "Denied":
            return handle_confirmation_no(event)
        else:
            return handle_greeting_intent(event)
    elif intent_name == "ThankYouIntent":
        return handle_thank_you_intent(event)
    elif intent_name == "DiningSuggestionsIntent":
        return handle_dining_suggestions_intent(event)
    else:
        raise Exception(f"Intent with name {intent_name} not supported")