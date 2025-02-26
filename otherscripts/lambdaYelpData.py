import json
import boto3
import urllib.request
import urllib.parse
from datetime import datetime
from decimal import Decimal
import base64

API_KEY = "5m8eluY_fu3TyDu81ZGhsht58-Er24p8fGuW2HIfMS2kzy-EZh_vdInr-27B75vpUMWITdV3sW6sMSTH4E56RGvFiyuPwYlaQdV2Jy52x7m0C-v3WjiX98zDF2-7Z3Yx"  
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

DYNAMODB_TABLE = "yelp-restaurants"
OS_ENDPOINT = "https://search-restaurants-opensearch-7xzru44w4rv2q4pr4rsw5kkkb4.us-east-1.es.amazonaws.com"
REGION = "us-east-1"

OS_USERNAME = "******"
OS_PASSWORD = "******"

dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(DYNAMODB_TABLE)

def get_os_headers():
    """Returns HTTP headers with OpenSearch Basic Authentication"""
    credentials = f"{OS_USERNAME}:{OS_PASSWORD}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    return {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/json"
    }

def get_restaurants(cuisine, location="Manhattan, NY", limit=50, offset=0):
    """Fetch restaurants from Yelp API using urllib with proper encoding."""
    encoded_cuisine = urllib.parse.quote(cuisine)
    encoded_location = urllib.parse.quote(location)

    url = f"https://api.yelp.com/v3/businesses/search?term={encoded_cuisine}&location={encoded_location}&limit={limit}&offset={offset}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {API_KEY}"})
    
    try:
        with urllib.request.urlopen(req) as response:
            data = response.read().decode()
            return json.loads(data)
    except urllib.error.HTTPError as e:
        print(f"HTTPError: {e.code}, {e.reason}")
    except urllib.error.URLError as e:
        print(f"URLError: {e.reason}")
    
    return {}

def store_in_dynamodb(restaurant):
    """Store restaurant data in DynamoDB, ensuring no duplicate entries."""
    response = table.get_item(Key={"business_id": restaurant["id"]})

    if "Item" in response:
        print(f"Restaurant {restaurant['name']} already exists in DynamoDB.")
        return False

    if "Item" not in response:
        item = {
            "business_id": restaurant["id"],
            "name": restaurant["name"],
            "address": ", ".join(restaurant["location"]["display_address"]),
            "coordinates": str(restaurant["coordinates"]),
            "num_reviews": int(restaurant["review_count"]),
            "rating": Decimal(str(restaurant["rating"])),
            "zip_code": restaurant["location"].get("zip_code", "Unknown"),
            "insertedAtTimestamp": datetime.utcnow().isoformat()
        }
        try:
            table.put_item(Item=item)
            print(f"Successfully stored: {restaurant['name']} ({restaurant['id']})")
            return True
        except Exception as e:
            print(f"Error storing {restaurant['name']}: {e}")
            return False

def store_in_opensearch(restaurant, cuisine):
    """Store restaurant data in OpenSearch"""
    index_name = "restaurants"
    doc_id = restaurant["id"]

    document = {
        "RestaurantID": restaurant["id"],
        "Cuisine": cuisine
    }

    url = f"{OS_ENDPOINT}/{index_name}/_doc/{doc_id}"
    headers = get_os_headers()
    data = json.dumps(document).encode("utf-8")

    req = urllib.request.Request(url, data=data, headers=headers, method="PUT")

    try:
        with urllib.request.urlopen(req) as response:
            print(f"Indexed in OpenSearch: {restaurant['name']} ({restaurant['id']})")
    except urllib.error.HTTPError as e:
        print(f"OpenSearch HTTPError: {e.code}, {e.reason}")
    except urllib.error.URLError as e:
        print(f"OpenSearch URLError: {e.reason}")

def lambda_handler(event, context):
    """Lambda function entry point"""
    cuisines = ["Chinese", "Japanese", "Thai"]
    total_unique_restaurants = 0

    for cuisine in cuisines:
        collected = 0
        offset = 0
        print(f"Fetching data for {cuisine}...")
        while collected < 50:
            data = get_restaurants(cuisine, offset=offset)
            businesses = data.get("businesses", [])
            print(f"Fetched {len(businesses)} restaurants for {cuisine}")

            if not businesses:
                break  # Stop fetching if no more results
            
            for restaurant in businesses:
                if store_in_dynamodb(restaurant):
                    total_unique_restaurants += 1
                    store_in_opensearch(restaurant, cuisine)
            
            collected += len(businesses)
            offset += 50

    return {
        "statusCode": 200,
        "body": json.dumps(f"Stored {total_unique_restaurants} restaurants successfully!")
    }
