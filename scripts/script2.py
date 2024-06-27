# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 12:28:40 2024

@author: Tanishq Salkar
"""
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  4 16:46:15 2024

@author: Tanishq Salkar
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
client_key = "aw3fsjy1ztapoj5x"
client_secret = "7dmdXdbTG505h50mcvl1ilnFGTSXcG9d"

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
        date_file_path = f"C:/Users/91810/Desktop/ISI/US_Elections_Video_Data_Collection/data_2/elections{date}.csv"

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
    return f"https://www.tiktok.com/@{username}/video/{videoid}"


def convert_epoch_to_datetime(input_time):
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
        #print(access_token_dict)
        return access_token_dict
    else:
        # If the request was not successful, print the error response JSON
        print("Error:", response.json())
        return response.json()
    
def fetch_tiktok_data(start_date,end_date,keywordsList,hashtagsList,token_info):
    count = 0
    full_json_response = {}
    full_json_response['data']={}
    full_json_response['data']['videos'] = [] 
    
    access_token = token_info["access_token"]
    headers = {
        'authorization': f"Bearer {access_token}",
    }

    #https://open.tiktokapis.com/v2/research/video/query/


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
            "max_count": 100 #max value (see documentation) = 100
    }
    print(data)
    max_retries = 300  # Set the maximum number of retries
    retries = 0

    loops = 0
    total_count = 0
    while True:
        # if loops == 4:
        #     sys.exit()
        # print(headers)
        # print(url)
        # print(data)
        if time.time() > token_info["expires_at"]:
            token_info = get_access_token(client_key, client_secret)
            token_info['expires_at'] = time.time() + token_info['expires_in']
            access_token = token_info["access_token"]
            headers["authorization"] =f"Bearer {access_token}"
            print("token expired so new token generated")
        try:
            response = requests.post(url, headers=headers, json=data)
           # print(response)
            if response.status_code == 200:
                if response.json()["data"]["has_more"] != True:
                    full_json_response['data']['videos'].extend(response.json()['data']['videos'])
                    time.sleep(40)
                    print("cur length:",len(response.json()['data']['videos']))
                    return full_json_response,total_count
                elif response.json()["data"]["has_more"] == True:

                    next_cursor = response.json()['data']['cursor']
                    #print(response.json())
                    next_search_id = response.json()['data']['search_id']
                    data['cursor'] = next_cursor
                    data['search_id'] = next_search_id
                    print("cur length in cursor :",len(response.json()['data']['videos']))
                    # print("cur length:",len(response.json()['data']['videos']))
                    full_json_response['data']['videos'].extend(response.json()['data']['videos'])
                    time.sleep(40)
                    total_count += len(response.json()['data']['videos'])
                    print("Total Count: ",total_count)
                    if total_count>=5000:
                        return full_json_response,total_count
            elif response.status_code == 400:
                print("400 error wait for some time")
                time.sleep(50)
            elif response.status_code == 401: #token expired
                print("Error:",response.status_code,response.text)
                # access_token =  get_access_token(client_key, client_secret)
                print("will generate a new token in next iteration: ")
                #return full_json_response,total_count
            elif response.status_code == 429: #limit passed
                # print("status code 429")
                print("Error:",response.status_code,response.text)
                return full_json_response,total_count
            elif response.status_code == 500: #internal server error
                print("Error:",response.status_code,response.text)
                retries += 1
                if retries >= max_retries:
                    return full_json_response, total_count
                # time.sleep(160 * retries)
                time.sleep(80)
            elif response.status_code == 503: 
                print("Error:",response.status_code,response.text)
                # sys.exit()
                time.sleep(100)
            elif response.status_code == 504: #Request timed out error
                print("Error:",response.status_code,response.text)
                # sys.exit()
                time.sleep(50)
            else:
                print("Error:", response.status_code, response.text)
                return full_json_response,total_count
            # loops+=1
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
    
#start date (phase 1): 11/01/2023        
#end date (phase 1): 01/15/2024
        
dict1 = dict()
#phase 4: March 6th - remove dean phillips and nikki haley
start_date="20240518"  
# end_date=""

# Convert the string to a datetime object
start_date_obj = datetime.strptime(start_date, "%Y%m%d")

# Open the file in read mode
with open('C:/Users/91810/Desktop/ISI/US_Elections_Video_Data_Collection/supplementary_files/keywords_hashtags_phase5.txt', 'r') as file:
    # Read each line and store it in a list
    lines = [line.strip() for line in file if line.strip()]
# Print the list of phrases
# print(lines)
keywordsList= lines
hashtagsList = lines
token_info = get_access_token(client_key, client_secret)
token_info['expires_at'] = time.time() + token_info['expires_in']
while start_date != "20240606":
    # Add one day to the date
    end_date_obj = start_date_obj + timedelta(days=1)
    # Convert the new date back to string format
    end_date_str = end_date_obj.strftime("%Y%m%d")
    print("start str:",start_date)
    print("end str:",end_date_str)
    
    # token_info = get_access_token(client_key, client_secret)
    # token_info['expires_at'] = time.time() + token_info['expires_in']
    data,total = fetch_tiktok_data(start_date,end_date_str,keywordsList=keywordsList,hashtagsList=hashtagsList,token_info=token_info)
    dict1[start_date] =total
    print("total videos:",total)
    print("==========================")

    if total==0:
        print("nothing returned")
        sys.exit()

    # put the data into a json file 
    if data:
        save_to_json_file(data, f'{start_date}_{end_date_str}_elections.json')

    #put data into csv file
    if data:
        df = pd.DataFrame(data['data']['videos'])


        #Add the tiktok urls
        df['tiktokurl'] = df.apply(lambda row: createURL(row['username'], row['id']), axis=1)
        df[['utc_year','utc_month','utc_day','utc_hour','utc_minute','utc_second','utc_date_string','utc_time_string']] = df['create_time'].apply(convert_epoch_to_datetime)
        
        append_to_existing_or_create_new(df, "C:/Users/91810/Desktop/ISI/US_Elections_Video_Data_Collection/New_folder/elections.csv")

    #new start date
    start_date_obj = datetime.strptime(start_date, "%Y%m%d")
    start_date_obj = start_date_obj + timedelta(days=1)
    start_date = start_date_obj.strftime("%Y%m%d")