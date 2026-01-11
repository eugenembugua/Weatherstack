#Importing the dependencies
import requests
import json


#Read the secrets file
with open("/opt/airflow/api-request/secrets.json", "r") as f:
    secrets = json.load(f)
    
#Grab the 'openaq-api-key' from secrets file
access_key = secrets.get("api_key")

#API url
api_url = "https://api.weatherstack.com/current"
query="Nairobi"

#Function to fetch data from API
def fetch_data():
    print("Fetching data from API")
    url = f"{api_url}?access_key={access_key}&query={query}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        print("API response is successfull!")
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"An Error occurred: {e}")
        raise
    
if __name__ == "__main__":
    try:
        weather_data = fetch_data()
        print("\n--Full API Response--")
        print(json.dumps(weather_data, indent=4))
            
    except Exception as e:
        print(f"Failed to retrieve data: {e}")