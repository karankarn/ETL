# -*- coding: utf-8 -*-
"""
Created on Mon Jan 27 14:47:50 2025

@author: User
"""


"""Download the zip file containing the required data in multiple formats and extract contents.
https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip
"""



import glob
import pandas as pd
import xml.etree.ElementTree as ET  # to parse data in XML format
from datetime import datetime 




"""You also require two file paths that will be available globally in the code for all functions. 
These are transformed_data.csv, to store the final output data that you can load to a database, and 
log_file.txt, that stores all the logs."""

log_file = "log_file.txt" 
target_file = "transformed_data.csv" 

""" EXTRACT"""
 
""" Function to extract .csv files"""

def extract_csv(filename):
    df = pd.read_csv(filename)
    return df

""" Function to extract .csv files"""

def extract_json(filename):
    df = pd.read_json(filename, lines = True) # lines = True (enable funtion to read json file line by line)
    return df

"""Funtion to extract XML files"""
def extract_xml(filename): 
    df = pd.DataFrame(columns=["name", "height", "weight"]) 
    tree = ET.parse(filename) 
    root = tree.getroot() 
    for person in root: 
        name = person.find("name").text 
        height = float(person.find("height").text) 
        weight = float(person.find("weight").text) 
        df = pd.concat([df, pd.DataFrame([{"name":name, "height":height, "weight":weight}])], ignore_index=True) 
    return df

"""Now you need a function to identify which function to call on basis of the filetype of the
 data file. To call the relevant function, write a function extract, which uses the glob library to
  identify the filetype. This can be done as follows:"""

def extract(): 
    extracted_data = pd.DataFrame(columns=['name','height','weight']) # create an empty data frame to hold extracted data 
     
    # process all csv files 
    for csvfile in glob.glob("*.csv"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_csv(csvfile))], ignore_index=True) 
         
    # process all json files 
    for jsonfile in glob.glob("*.json"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_json(jsonfile))], ignore_index=True) 
     
    # process all xml files 
    for xmlfile in glob.glob("*.xml"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_xml(xmlfile))], ignore_index=True) 
         
    return extracted_data 



""" TRANSFORM """

"""The height in the extracted data is in inches, and 
the weight is in pounds. However, for your application, 
the height is required to be in meters, and the weight 
is required to be in kilograms, rounded to two decimal 
places. Therefore, you need to write the function to 
perform the unit conversion for the two parameters."""

def transform(data): 
    '''Convert inches to meters and round off to two decimals 
    1 inch is 0.0254 meters '''
    data['height'] = round(data.height * 0.0254,2) 
 
    '''Convert pounds to kilograms and round off to two decimals 
    1 pound is 0.45359237 kilograms '''
    data['weight'] = round(data.weight * 0.45359237,2) 
    
    return data 

""" LOAD """

"""To load the data, you need a function load_data() that accepts
 the transformed data as a dataframe and the target_file path. You need to use the to_csv 
 attribute of the dataframe in the function as follows:"""
def load_data(target_file, transformed_data): 
    transformed_data.to_csv(target_file) 

""" implement a logging funtion that accepts log message
as arguement. the funtion writes current data and time
using the datetime function along with the log message 
in a string format"""

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 


# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file,transformed_data) 
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 


    
