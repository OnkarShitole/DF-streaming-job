import csv
import time
from google.cloud import pubsub_v1
import json
from datetime import datetime


project_id= "centered-sol-469812-v8"
topic_id=""

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def publish_message(Id, Name, Salary):
    timestamp =datetime.utcnow().isoformat() + "Z"
    message = {
        "Id": Id,
        "Name": Name,
        "Salary": Salary,
        "Timestamp": timestamp,
    }
        
    message_json = json.dumps(message).encode("utf-8")
        
    publish_status = publisher.publish(topic_path,message_json)
    print(f"Published message with ID: {publish_status.result()} - {message}")
        
csv_file_path = "emp.csv"
    
with open(csv_file_path, mode="r") as file:
    reader = csv.DictReader(file)
    for row in reader:
        Id = row["Id"]
        Name = row["Name"]
        Salary = ["Salary"]
            
        publish_message(Id,Name,Salary)
            
        time.sleep(10)