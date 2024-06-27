'''
This script downloads the videos
'''
import pandas as pd 
import pyktok as pyk
import time
import requests
from requests.exceptions import ReadTimeout, ConnectTimeout
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import time
import logging
import sys


def setup_logging(file_path):
    """
    Create the log file for those videos (one day)
    """
    log_dir = "../download_scripts/log_files"
    print("In Setup logging")
    # Set up logging with a separate log file for each CSV file
    path_parts = file_path.parts
    date_part = '_'.join(path_parts[-2].split('_')[-2:])  # Get the date part from the directory name
    file_part = path_parts[-1].split('.')[0]   # Get the file name without the extension

    # Combine the parts to create the log file name
    log_file_name = f"{date_part}_{file_part}_process_log.log"

    # Specify the directory where you want to save the log files
    log_file_path = os.path.join(log_dir, log_file_name)

    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    print("setup logging completed")



def isPrivate(row):
    """
        Check if the video is still publicly available on TikTok

        Parameters:
        pandas dataframe row: the row of the pandas dataframe
        
        Returns:
        boolean: True if the video is public, False if it's private
    """

    username = row['username']
    id = row['id']
    max_attempts = 4
    print("in is private function")
    for attempt in range(max_attempts):
        try:
            tt_json = pyk.alt_get_tiktok_json(f"https://www.tiktok.com/@{username}/video/{id}?is_copy_url=1&is_from_webapp=v1")
            print("tt_json",tt_json)
            obj = tt_json["__DEFAULT_SCOPE__"]['webapp.video-detail']['itemInfo']['itemStruct']['privateItem']
            if obj == True: #video is private / privateItem == True
                return False
                print("private 0 false")
            else: #video is public / privateItem == False
                print("private 1 true")
                return True 
        except Exception as e:
            # print(f"Attempt {attempt + 1} failed with error: {e}")
            if isinstance(e, ReadTimeout):
                if attempt < max_attempts - 1:
                    # print("Retrying...")
                    time.sleep(100)  
                else:
                    # print("Max attempts reached. Moving to next URL.")
                    # print("="*100)
                    print("private 1 false")
                    return False  # Indicate failure to retrieve data
            elif "webapp.video-detail" in str(e) or "itemInfo" in str(e):
                print("private 2 false")
                return False
            else:
                # print("Unhandled error, moving to next URL.")
                # print(f"Attempt {attempt + 1} failed with error: {e}")
                if attempt < max_attempts - 1:
                    # print("Retrying...")
                    # print("="*100)
                    time.sleep(100)  
                else:
                    # print("Max attempts reached. Moving to next URL.")
                    print("private 3 false")
                    # print("="*100)
                    return False 
                

def is_mp4_file(file_path):
    """
        Check if the video is a valid mp4

        Parameters:
        str: file path to the mp4 file
        
        Returns:
        boolean: True if the mp4 is valid, False if its not
    """
    print("in is mp4 file function")
    try:
        with open(file_path, 'rb') as file:
            header = file.read(12)
            # Check for MP4 signature (ftyp box)
            return header[4:8] == b'ftyp'
    except IOError:
        return False


def format_url(url):
    """
        Format the url needed for pyktok

        Parameters:
        str: url to the tiktok video
        Returns:
        str: formatted url required for pyktok
    """
    print("IN format url function")
    return url+'?is_copy_url=1&is_from_webapp=v1'
                

def save_video(url):
    """
        Downloads the video given the url

        Parameters:
        str: string that is in the url formatted require for pyk: 

        Returns:
        None: The video will download in the current directory
    """
    print("In save video function")
    pyk.save_tiktok(url,save_video=True,browser_name="chrome")



def download(row,videoFolderPath):
    """
        Retrieve the data from the TikTok API

        Parameters:
        str: url, the construct url outputted in the create_url function
        
        Returns:
        None
    """
    print("In download function")
    start_time = time.time()
    url = row['tiktokurl']

    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            time.sleep(10)
            formatted_url = format_url(url)
            print("save video function started")
            save_video(formatted_url) #download videos here

            #if the videos downloaded..this doesn't guaranteed that it downloaded properly 
            #(i.e. it might download an empty mp4 file)
            video_file_path = videoFolderPath+f"/@{row['username']}_video_{row['id']}.mp4"
            print("video_file_path",video_file_path)
            mp4_valid = is_mp4_file(video_file_path)
            end_time = time.time()
            logging.info(f"Downloaded video {url} in {end_time - start_time:.2f} seconds")
            return mp4_valid #it does exist
        except Exception as e:
            print("EXCEPTION occured in download function")
            #if the webpage didn't load properly then we will call pyktok again
            # print(f"Attempt {attempt + 1} failed with error: {e}")
            if isinstance(e, ReadTimeout):
                if attempt < max_attempts - 1:
                    # print("Retrying...")
                    time.sleep(100)  
                else:
                    # print("Max attempts reached. Moving to next URL.")
                    end_time = time.time()
                    logging.info(f"FAILED Downloaded video {url} in {end_time - start_time:.2f} seconds")
                    # print("="*100)
                    return # Indicate failure to retrieve data
            elif "webapp.video-detail" in str(e) or "itemInfo" in str(e):
                return 
            else:
                # print("Unhandled error, moving to next URL.")
                # print(f"Attempt {attempt + 1} failed with error: {e}")
                if attempt < max_attempts - 1:
                    # print("Retrying...")
                    # print("="*100)
                    time.sleep(100)  # Consider adjusting sleep time as needed
                else:
                    # print("Max attempts reached. Moving to next URL.")
                    end_time = time.time()
                    logging.info(f"FAILED Downloaded video {url} in {end_time - start_time:.2f} seconds")
                    # print("="*100)
                    return  


################################################

def process_csv_file(file_path):
    # Read the CSV file into a DataFrame
    start_time = time.time()  # Record the start time
    df = pd.read_csv(file_path)
    print("file path",file_path)
    print("df head",df.head())
    print("in process csv func dataframe shape is",df.shape)
    # # Grab the date (month_day) -- it will be the new subdirectory for the output csv file
    # # Split the file path into parts
    # path_parts = file_path.split(os.sep)
    # # Find the part that starts with "chunks"
    # chunks_part = [part for part in path_parts if part.startswith("chunks")][0]
    # # Extract the "11_01"/date part after "chunks_"
    # date_part = chunks_part.split("_", 1)[1]
    path_parts = file_path.split("elections")[1]
    date_part = path_parts[5:-4]
    print("date_part",date_part)
    

    #make directory for output 
    os.makedirs(f"../metadata_csv_day_chunks_values/{date_part}", exist_ok=True)

    df['isPublic'] = df.apply(isPrivate,axis=1)
    # Create a new directory based on the CSV file name and change into it
    file_path = Path(file_path) if not isinstance(file_path, Path) else file_path


    directory_name = file_path.stem #the chunk
    fullDirPath = f"../videos_by_date/{date_part}/"+directory_name
    if not os.path.exists(directory_name):
        print(fullDirPath)
        os.makedirs(fullDirPath, exist_ok=True)
    os.chdir(fullDirPath)
    print("fullDirPath",fullDirPath)
    print("process csv directory setup done")
    videoFolderPath = fullDirPath
    df['mp4_isValid']=df.apply(download,axis=1,args=(videoFolderPath,))
    print("mp4_isValid ","download function applied on the dataframe")
    # Save the DataFrame back to CSV
    df.to_csv(f"../metadata_csv_day_chunks_values/{date_part}/"+file_path.stem+".csv", index=False)
    # print(f"Processed {file_path}")
    end_time = time.time()  # Record the end time
    execution_time = end_time - start_time  # Calculate the execution time
    logging.info(f"Processed {file_path} in {execution_time:.2f} seconds")

if __name__ == "__main__":
    pyk.specify_browser('Chrome') #required for pyktok

    # if len(sys.argv) != 2:
    #     print("Usage: python download_videos.py <path_to_csv_file>")
    #     sys.exit(1)

    # csv_file_path = sys.argv[1] 
    # print("argv[1]:",csv_file_path)

    csv_file_path = "elections2024-01-02.csv"
    print("argv[1]:",csv_file_path)


    csvDir = "../data/"
    full_path = csvDir+csv_file_path
    setup_logging(Path(full_path)) #setup the log file
    print("Setup logging function completed")
    process_csv_file(full_path) #download and etc.
    print("process_csv_file func completed")
