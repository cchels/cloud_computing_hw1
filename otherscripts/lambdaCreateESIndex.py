import json
import urllib.request
import base64

OS_ENDPOINT = "https://search-restaurants-opensearch-7xzru44w4rv2q4pr4rsw5kkkb4.us-east-1.es.amazonaws.com"
INDEX_NAME = "restaurants"

OPENSEARCH_USER = "******"
OPENSEARCH_PASS = "******"

credentials = f"{OPENSEARCH_USER}:{OPENSEARCH_PASS}"
encoded_credentials = base64.b64encode(credentials.encode()).decode("utf-8")

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Basic {encoded_credentials}"
}

def lambda_handler(event, context):
    """Lambda function to create an OpenSearch index with FGAC enabled."""
    url = f"{OS_ENDPOINT}/{INDEX_NAME}"
    
    index_config = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1
        },
        "mappings": {
            "properties": {
                "RestaurantID": {"type": "keyword"},
                "Cuisine": {"type": "text"}
            }
        }
    }

    req = urllib.request.Request(url, data=json.dumps(index_config).encode("utf-8"), headers=HEADERS, method="PUT")

    try:
        with urllib.request.urlopen(req) as response:
            result = response.read().decode()
            print("Index Created:", result)
            return {"statusCode": 200, "body": "Index created successfully!"}
    except urllib.error.HTTPError as e:
        print(f"OpenSearch HTTPError: {e.code}, {e.reason}")
        return {"statusCode": e.code, "body": f"HTTPError: {e.reason}"}
    except urllib.error.URLError as e:
        print(f"OpenSearch URLError: {e.reason}")
        return {"statusCode": 500, "body": f"URLError: {e.reason}"}