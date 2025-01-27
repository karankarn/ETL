# ETL
Building an basic ETL pipeline for learning purposes from Python Project for Data Engineering offered by IBM on Cousera.

## Overview  
This ETL pipeline is designed to:  

Extract data from CSV, JSON, and XML files.  
Transform the data by converting:  
  Height from inches to meters (rounded to two decimals).  
  Weight from pounds to kilograms (rounded to two decimals).  
Load the transformed data into a single CSV file named transformed_data.csv.  
Log the steps of the process in a log_file.txt.  

## Pre Requisites  
Python 3.7+  
Pandas (pip install pandas)  
xml.etree.ElementTree (Standard library for Python)  
datetime (Standard library for Python)  
Ensure you have these installed and accessible in your environment.  

## Data Preparation  
Download the data  
Download the source.zip file which contains sample CSV, JSON, and XML data.  

Extract the ZIP file  
Extract the contents of source.zip into your project folder. You should see multiple files of different formats (.csv, .json, .xml).  

## Project Structure  
A suggested directory structure is as follows:  

├── etl_script.py              -- This is the main script containing all ETL functions  
├── source.zip                 -- The zipped data (downloaded)  
├── *.csv                      -- CSV files (after unzipping)  
├── *.json                     -- JSON files (after unzipping)  
├── *.xml                      -- XML files (after unzipping)  
├── transformed_data.csv       -- Output CSV after running the pipeline  
├── log_file.txt               -- Log file generated after running the pipeline  
└── README.md                  -- This README file  

## Usage  

Clone or Download this repository.  

Place the extracted data files (CSV, JSON, XML) in the same directory as the etl_script.py (or adjust paths in the script if needed).  

Run below ETL Script in bash  
python etl_script.py

The script will:  

Extract data from all .csv, .json, and .xml files found in the directory.  
Transform the data (convert inches to meters and pounds to kilograms).  
Load the final data to transformed_data.csv.  
Record logs in log_file.txt.  
Inspect the output file transformed_data.csv for the consolidated and transformed data.  


### Happy ETL-ing! If you have any questions, suggestions, or concerns, feel free to open an issue or contribute via pull request.






