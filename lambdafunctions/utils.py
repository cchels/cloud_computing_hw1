# --- Helper Functions for Lex Responses ---
import datetime
import re

# --- Lex Response Helper Functions ---
def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    """Elicit the next slot value."""
    return {
        "sessionState": {
            "dialogAction": {
                "type": "ElicitSlot",
                "slotToElicit": slot_to_elicit
            },
            "intent": {
                "name": intent_name,
                "slots": slots,
                "state": "InProgress"
            },
            "sessionAttributes": session_attributes
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": message
            }
        ]
    }

def close(session_attributes, intent_name, fulfillment_state, message):
    """Close the conversation with a completion message."""
    return {
        "sessionState": {
            "dialogAction": {
                "type": "Close"
            },
            "intent": {
                "name": intent_name,
                "state": fulfillment_state
            },
            "sessionAttributes": session_attributes
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": message
            }
        ]
    }

def delegate(session_attributes, intent_name, slots):
    """Delegate control to Lex for the next step in the conversation."""
    return {
        "sessionState": {
            "dialogAction": {
                "type": "Delegate"
            },
            "intent": {
                "name": intent_name,
                "slots": slots,
                "state": "InProgress"
            },
            "sessionAttributes": session_attributes
        }
    }

def confirm_intent(session_attributes, intent_name, message):
    """Ask the user to confirm the intent."""
    return {
        "sessionState": {
            "dialogAction": {
                "type": "ConfirmIntent"
            },
            "intent": {
                "name": intent_name,
                "state": "InProgress"
            },
            "sessionAttributes": session_attributes
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": message
            }
        ]
    }

def build_validation_result(is_valid, violated_slot, message_content):
    """Build the result structure for validation."""
    if not is_valid:
        return {
            'isValid': False,
            'violatedSlot': violated_slot,
            'message': {'contentType': 'PlainText', 'content': message_content}
        }
    return {'isValid': True}


# --- Slot Validation Functions ---
def is_valid_date(date):
    """Check if the date is a valid future date."""
    try:
        input_date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
        if input_date < datetime.date.today():
            return False, "Please provide a future date. "
        return True, None
    except ValueError:
        return False, "The date format is invalid. Please use YYYY-MM-DD format."

def is_valid_location(location):
    """Check if the location is supported."""
    try:
        supported_locations = ["manhattan"]
        if location.lower() not in supported_locations:
            return False, "Sorry, We only support Manhattan at this time. Please say Manhattan. "
        return True, None
    except ValueError:
        return False, "The location format is invalid. Please use Manhattan. "

def is_valid_cuisine(cuisine):
    """Check if the cuisine is supported."""
    try:
        supported_cuisines = ["chinese", "japanese", "thai"]
        if cuisine.lower() not in supported_cuisines:
            return False, "Sorry, We only support Chinese, Japanese, and Thai cuisines at this time. Please choose one of these cuisines. "
        return True, None
    except ValueError:
        return False, "The cuisine format is invalid. Please choose one of these cuisines: Chinese, Japanese, Thai. "

def is_valid_number_of_people(number_of_people):
    """Check if the number of people is within the valid range."""
    try:
        number_of_people = int(number_of_people)
        if number_of_people < 1 or number_of_people > 20:
            return False, "Please provide a number between 1 and 20."
        return True, None
    except (ValueError, TypeError):
        return False, "The number of people format is invalid. Please provide a number between 1 and 20."

def is_valid_email(email):
    """Check if the email is valid."""
    try:
        pattern = (
            r"^(?!.*\.\.)"                      # Asserts no consecutive dots
            r"[A-Za-z0-9][A-Za-z0-9._%+-]*"     # Start with a letter or number, followed by zero or more valid characters
            r"@"                                # Requires @
            r"[A-Za-z0-9][A-Za-z0-9.-]*"        # Domain name part before the dot
            r"\.[a-zA-Z]{2,10}$"                # Domain name part after the dot
        )
        if re.match(pattern, email):
            return True, None
        else:
            return False, "Please provide a valid email address."
    except ValueError:
        return False, "The email format is invalid. Please provide a valid email address."


# --- Main Validation Function ---
def validate_dining_suggestions(location, cuisine, dining_date, dining_time, number_of_people, email):
    """Perform validation for all slots with advanced checks."""
    if location:
        location_valid, location_message = is_valid_location(location)
        if not location_valid:
            return build_validation_result(False, 'Location', location_message)
    if cuisine:
        cuisine_valid, cuisine_message = is_valid_cuisine(cuisine)
        if not cuisine_valid:
            return build_validation_result(False, 'Cuisine', cuisine_message)
    if dining_date:
        date_valid, date_message = is_valid_date(dining_date)
        if not date_valid:
            return build_validation_result(False, 'DiningDate', date_message)
    if number_of_people:
        number_of_people_valid, number_of_people_message = is_valid_number_of_people(number_of_people)
        if not number_of_people_valid:
            return build_validation_result(False, 'NumberOfPeople', number_of_people_message)
    if email:
        email_valid, email_message = is_valid_email(email)
        if not email_valid:
            return build_validation_result(False, 'Email', email_message)
    return {'isValid': True, 'violatedSlot': None, 'message': None}


# --- Other Util Function ---
def get_slot_value(slots, slot_name):
    """Extract the interpretedValue from Lex V2 slot structure."""
    slot = slots.get(slot_name)
    if slot is not None:
        slot_value = slot.get("value", {}).get("interpretedValue")
        return slot_value
    return None