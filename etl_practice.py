import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime


log_file1 = "log_file1.txt"
target_file1 = "transformed_data1.csv"

def extract_from_csv(file_to_process):
    df = pd.read_csv(file_to_process)
    return df

def extract_from_json(file_to_process):
    df = pd.read_json(file_to_process, lines=True)
    return df

def extract_from_xml(file_to_process):
    df = pd.DataFrame(columns = ["car_model","year_of_manufacture","price","fuel"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for car in root:
        car_model = car.find("car_model").text
        year_of_manufacture = float(car.find("year_of_manufacture").text)
        price = float(car.find("price").text)
        fuel = car.find("fuel").text
        df = pd.concat([df, pd.DataFrame([{"car_model":car_model,"year_of_manufacture":year_of_manufacture,"price":price, "fuel":fuel}])], ignore_index=True)
    return df

def extract():
    extracted_data = pd.DataFrame(columns=["car_model","year_of_manufacture","price","fuel"])

    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)

    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True)
    
    return extracted_data

def transform(data):
    data["price"] = round(data.price, 2)
    return data

def load_data(target_file1, transformed_data1):
    transformed_data1.to_csv(target_file1)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file1, "a") as f:
        f.write(timestamp + ',' + message + '\n')

log_progress("ETL Job Started")

log_progress("Extract phase Started")
extracted_data = extract()
log_progress("Extract phase Ended")

log_progress("Transform phase Started")
transformed_data1 = transform(extracted_data)
print("Transformed Data")
print(transformed_data1)
log_progress("Tranform phase Ended")

log_progress("Load phase Started")
load_data(target_file1, transformed_data1)
log_progress("Load phase Ended")

log_progress("ETL job ended")
