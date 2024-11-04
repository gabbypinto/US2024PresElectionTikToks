# step 1
# install TikTokApi
# pip install playwright==1.37.0 
# run playwright install
#step 2
#add the below code in : ../.venv/lib/python3.9/site-packages/TikTokApi/api/user.py
# if "userInfo" in keys and not data["userInfo"]:
#     # If userInfo is empty, save the username to a CSV file and exit
#     with open('empty_user_info.csv', mode='a', newline='', encoding='utf-8') as file:
#         writer = csv.writer(file)
#         writer.writerow([self.username])
#     return
# Step 3
# To increase the timeout, you’ll need to modify the TikTokApi package directly, 
# specifically where the wait_for_function is called within the generate_x_bogus method in the tiktok.py file.
# modify await session.page.wait_for_function("window.byted_acrawler !== undefined") to ->  await session.page.wait_for_function("window.byted_acrawler !== undefined", timeout=60000)  # 60 seconds timeout


from TikTokApi import TikTokApi
import asyncio
import os
import csv
import pandas as pd
import time

# Retrieve the ms_token from environment variables
ms_token = os.environ.get("ms_token", None)

# Function to fetch user information and save it to a CSV file
async def fetch_user_info(usernames, output_file, error_file):
    print("function called")
    async with TikTokApi() as api:
        await api.create_sessions(ms_tokens=[ms_token], num_sessions=1, sleep_after=3, headless=False)

        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['display_name', 'followers', 'following', 'Like_count', 'Video_count', 'nickname', 'verified'])  # Write header
            print("file opened successfully")

            # Open error file to log usernames that cause errors
            with open(error_file, mode='w', newline='', encoding='utf-8') as error_file:
                error_writer = csv.writer(error_file)
                error_writer.writerow(['username', 'error'])  # Header for error file

                for username in usernames:
                    try:
                        user = api.user(username)
                        user_data = await user.info()

                        # Check if userInfo is empty
                        if not user_data.get("userInfo"):
                            continue

                        # Extract relevant information
                        display_name = username
                        followers = user_data['userInfo']['stats']['followerCount']
                        following = user_data['userInfo']['stats']['followingCount']
                        like = user_data['userInfo']['stats']['heart']
                        video = user_data['userInfo']['stats']['videoCount']
                        nickname = user_data['userInfo']['user']['nickname']
                        verified = user_data['userInfo']['user']['verified']
                        print("user fetched successfully", display_name)

                        # Write user data to CSV
                        writer.writerow([display_name, followers, following, like, video, nickname, verified])

                    except Exception as e:
                        # Log the username and error message to the error file
                        error_writer.writerow([username, str(e)])
                        print(f"Error fetching data for {username}: {e}")
                    time.sleep(2)

# Function to process each input CSV file and fetch user information
async def process_csv_files(csv_files):
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        usernames = df['username'].unique()  # Extract unique usernames
        print(f"Total unique users are:",usernames.shape[0])
        output_file = f"user_info_{os.path.basename(csv_file)}"
        error_file = f"errors_{os.path.basename(csv_file)}"  # Separate file for errors
        await fetch_user_info(usernames, output_file, error_file)

if __name__ == "__main__":
    csv_files = ["../month_wise_data/July2024.csv"]  # List of CSV files
    asyncio.run(process_csv_files(csv_files))