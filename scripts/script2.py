# -*- coding: utf-8 -*-
"""
"""
import sys
import requests
import json
from datetime import datetime, timedelta
import sys
import time
import csv 
import pandas as pd
import os
from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import ProtocolError

# client key and secret key
client_key = "enter key"
client_secret = "enter secret key"

def append_to_existing_or_create_new(df, combined_df_path):
    # Load the existing combined DataFrame
    if os.path.exists(combined_df_path):
        combined_df = pd.read_csv(combined_df_path)
    else:
        combined_df = pd.DataFrame()

    # Iterate through each date in the new DataFrame
    for date in df['utc_date_string'].unique():
        # Filter the new DataFrame for the current date
        df_date = df[df['utc_date_string'] == date]

        # Define the file path for the current date
        date_file_path = f"elections<date>.csv"

        if os.path.exists(date_file_path):
            # Load the existing DataFrame for that date
            existing_df = pd.read_csv(date_file_path)

            # Append the new data to the existing DataFrame
            updated_df = pd.concat([existing_df, df_date], ignore_index=True)

            # Drop duplicates based on the 'id' column
            updated_df = updated_df.drop_duplicates(subset=['id'], keep='first')

            # Save the updated DataFrame back to the same CSV file
            updated_df.to_csv(date_file_path, index=False)
        else:
            # Create a new CSV file for the new date
            df_date.to_csv(date_file_path, index=False)

        # Append the new data to the combined DataFrame
        combined_df = pd.concat([combined_df, df_date], ignore_index=True)

    # Drop duplicates in the combined DataFrame and save it
    combined_df = combined_df.drop_duplicates(subset=['id'], keep='first')
    print("length of final/combined/elections csv file is:",len(combined_df))
    combined_df.to_csv(combined_df_path, index=False)

def save_to_json_file(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)


def createURL(username,videoid):
    """
    Create the URL to TikTok post: the username and video's is required

    Args: 
        username(str): Username of the person that posted the TikTok video
        videoid(int): 'id'' of the video

    Returns: 
        str: the URL to the TikTok post
    """
    return f"https://www.tiktok.com/@{username}/video/{videoid}"


def convert_epoch_to_datetime(input_time):
    """
    Convert Epoch/Unix time to UTC time
    
    Args:
        input(str): The time the video was created in Epoch/UNIX time 

    Returns:
        pd.Series: The extract the time that include the year, month, day, hour, minute, second, complete date (YYY-MM-DD), and the complete time stamp (HH:MM:SS)
    """
    utc_time_stamp = datetime.utcfromtimestamp(input_time)

    # Extract date and time components
    year = utc_time_stamp.year
    month = utc_time_stamp.month
    day = utc_time_stamp.day
    hour = utc_time_stamp.hour
    minute = utc_time_stamp.minute
    second = utc_time_stamp.second
    date_string = utc_time_stamp.strftime("%Y-%m-%d")
    time_string = utc_time_stamp.strftime("%H:%M:%S")

    return pd.Series([year,month,day,hour,minute,second,date_string,time_string])


def get_access_token(client_key, client_secret):
    """
    Fetches the Research API access token.

    Args: 
        client_key (str): Key is copied from TikTok's developer portal 
        client_secret (str): Secret is copied from TikTok's developer portal

    Returns: 
        access_token_dict (dict): Dictionary the contains the following attributes/keys 'access_token','expires_in','token_type'
    """

    # Endpoint URL
    endpoint_url = "https://open.tiktokapis.com/v2/oauth/token/"

    # Request headers
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    # Request body parameters
    data = {
        'client_key': client_key,
        'client_secret': client_secret,
        'grant_type': 'client_credentials',
    }
    # Make the POST request
    response = requests.post(endpoint_url, headers=headers, data=data)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse and print the response JSON
        response_json = response.json()
        keys = ['access_token', 'expires_in', 'token_type']
        access_token_dict = {key: response_json[key] for key in keys if key in response_json}
        return access_token_dict
    else:
        # If the request was not successful, print the error response JSON
        print("Error:", response.json())
        return response.json()
    
def fetch_tiktok_data(start_date,end_date,keywordsList,hashtagsList,token_info):
    """
    Fetch the metadata of the tiktok post/video

    Args:
        start_date (str, required): Date should be YYYYMMDD format. 
        end_date (str, required): Date should be YYYYMMDD format. 
        keywordsList (list, required): List of strings, where each string is the keyword you want to apply to the query
        hashtagsList (list, required): List of strings, where each string is a hashtag you want to apply to the query
        token_info (dict, required): Dictionary that contains the access token, when the token will expire, and the type
    
    Returns:
        JSON,int: The metadata for each video in JSON format, and the number of videos collected
    
    Raises: 
        ChunkedEncodingError: Problem reading or process 
        ProtocolError: Could not complete the request 
        Other: Unexpected error
    """
    full_json_response = {}
    full_json_response['data']={}
    full_json_response['data']['videos'] = [] 
    
    access_token = token_info["access_token"]
    headers = {
        'authorization': f"Bearer {access_token}",
    }

    url = 'https://open.tiktokapis.com/v2/research/video/query/?fields=id,like_count,create_time,region_code,share_count,view_count,comment_count,music_id,hashtag_names,username,effect_ids,playlist_id,video_description,voice_to_text'
    
    data = {
            "query": {
                "and": [
                    { "operation": "IN", "field_name": "region_code", "field_values": ["US"] },
                ],
                "or":[
                    { "operation": "IN", "field_name": "keyword", "field_values": keywordsList },
                    { "operation": "IN", "field_name": "hashtag_name", "field_values": hashtagsList },
                ]
            }, 
            "start_date":start_date,
            "end_date":end_date,
            "max_count": 100 #Max value of videos that can be returned, this is count that is posted on the TikTok Research API 
    }
    print(data)
    max_retries = 300  # Set the maximum number of retries
    retries = 0

    total_count = 0
    while True:
        if time.time() > token_info["expires_at"]:
            token_info = get_access_token(client_key, client_secret)
            token_info['expires_at'] = time.time() + token_info['expires_in']
            access_token = token_info["access_token"]
            headers["authorization"] =f"Bearer {access_token}"
            print("token expired so new token generated")
        try:
            response = requests.post(url, headers=headers, json=data)
            if response.status_code == 200:
                if response.json()["data"]["has_more"] != True:
                    full_json_response['data']['videos'].extend(response.json()['data']['videos'])
                    time.sleep(40)
                    print("cur length:",len(response.json()['data']['videos']))
                    return full_json_response,total_count
                elif response.json()["data"]["has_more"] == True:
                    next_cursor = response.json()['data']['cursor']
                    next_search_id = response.json()['data']['search_id']
                    data['cursor'] = next_cursor
                    data['search_id'] = next_search_id
                    print("cur length in cursor :",len(response.json()['data']['videos']))
                    full_json_response['data']['videos'].extend(response.json()['data']['videos'])
                    time.sleep(40)
                    total_count += len(response.json()['data']['videos'])
                    print("Total Count: ",total_count)
                    if total_count>=5000:
                        return full_json_response,total_count
            elif response.status_code == 400: 
                #400 error
                print("400 error wait for some time")
                time.sleep(50)
            elif response.status_code == 401: 
                #401 error: Token expired
                print("Error:",response.status_code,response.text)
                print("Wll generate a new token in next iteration: ")
            elif response.status_code == 429: 
                #429 error: Limit exceeded
                print("Error:",response.status_code,response.text)
                return full_json_response,total_count
            elif response.status_code == 500: 
                #500 error: Internal server error
                print("Error:",response.status_code,response.text)
                retries += 1
                if retries >= max_retries:
                    return full_json_response, total_count
                time.sleep(80)
            elif response.status_code == 503: 
                #503 error
                print("Error:",response.status_code,response.text)
                time.sleep(100)
            elif response.status_code == 504:
                #504 error: Request timed out error
                print("Error:",response.status_code,response.text)
                time.sleep(50)
            else:
                print("Error:", response.status_code, response.text)
                return full_json_response,total_count
        except (ChunkedEncodingError, ProtocolError) as e:
            retries += 1
            if retries >= max_retries:
                print(f"Max retries reached. Last error: {e}")
                return full_json_response, total_count
            print(f"Encountered an error: {e}. Retrying ({retries}/{max_retries})...")
            time.sleep(160)  # Exponential backoff
        except Exception as e:
            print(f"Unexpected error: {e}")
            return full_json_response, total_count
    

dict1 = dict()
start_date="YYYYMMDD"  

# Convert the string to a datetime object
start_date_obj = datetime.strptime(start_date, "%Y%m%d")

# Open the file in read mode
with open("<FILE PATH TO TEXTFILE CONTAINING KEYWORDS AND HASTAGS>", 'r') as file:
    # Read each line (hashtag/keywords and store it in a list) 
    lines = [line.strip() for line in file if line.strip()]

keywordsList= lines
hashtagsList = lines

#Obtain the access token
token_info = get_access_token(client_key, client_secret)
#Obtain token's expiration date
token_info['expires_at'] = time.time() + token_info['expires_in']
while start_date != "YYYYMMDD":  #insert the date you want the loop to start
    # Add one day to the date
    end_date_obj = start_date_obj + timedelta(days=1)
    # Convert the new date back to string format
    end_date_str = end_date_obj.strftime("%Y%m%d")
    print("start str:",start_date)
    print("end str:",end_date_str)
    data,total = fetch_tiktok_data(start_date,end_date_str,keywordsList=keywordsList,hashtagsList=hashtagsList,token_info=token_info)
    dict1[start_date] =total
    print("total videos:",total)

    if total==0:
        print("Nothing Returned")
        sys.exit()
    #Save the data into a JSON file 
    if data:
        save_to_json_file(data, f'{start_date}_{end_date_str}_elections.json')

    #Put data into csv file
    if data:
        df = pd.DataFrame(data['data']['videos'])
        #Apply function that creates the URL to the TikTok
        df['tiktokurl'] = df.apply(lambda row: createURL(row['username'], row['id']), axis=1)
        #Extract the year,month, day, hour,minute, seconds in utc time, the create_at is in Epoch time/Unix time
        df[['utc_year','utc_month','utc_day','utc_hour','utc_minute','utc_second','utc_date_string','utc_time_string']] = df['create_time'].apply(convert_epoch_to_datetime)
        #Append data to the "main" file, where all data for each video is stored
        append_to_existing_or_create_new(df, "FILE PATH TO your main file, e.g. elections.csv")

    #Increate the start date by 1 day
    start_date_obj = datetime.strptime(start_date, "%Y%m%d")
    start_date_obj = start_date_obj + timedelta(days=1)
    start_date = start_date_obj.strftime("%Y%m%d")
