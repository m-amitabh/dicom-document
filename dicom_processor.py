import os
import pydicom
from pymongo import MongoClient
from mongodb_connection import MongoDBConnection
import uuid
from datetime import datetime
import json

testing = False
debug = False


def dicom_dataset_to_dict(ds):
    # Convert DICOM dataset to a dictionary
    dicom_dict = {}
    for element in ds:
        if (
            element.VR != "SQ" and element.keyword != "PixelData"
        ):  # If not a sequence and not PixelData, add to dictionary
            dicom_dict[element.keyword] = {
                "Value": str(element.value),
                "Tag": str(element.tag),
                "name": str(element.name),
            }
    return dicom_dict


def generate_uri(filepath):
    # Generate URI based on the path of the DICOM file
    return f"file://{os.path.abspath(filepath)}"
    # return f"s3://your-bucket-name/your-key-name/{os.path.basename(filepath)}"


# Function to parse DICOM file and return dicom_instance.json
def parse_dicom_file(file_path, uploadId):
    dicom_data = pydicom.dcmread(file_path)
    metadata = dicom_dataset_to_dict(dicom_data)
    dicom_instance = {
        "header": {
            "SOPInstanceUID": dicom_data.SOPInstanceUID,
            "Metadata": metadata,
        },
        "uploadId": uploadId,
        "filePath": generate_uri(file_path),
    }
    return dicom_instance


# Function to create/update dicom_pesi.json
def update_dicom_pesi(client, dicom_pesi):
    db = client.playground
    collection = db.dicom_pesi
    collection.insert_one(dicom_pesi)


# Function to create uploads.json
def create_uploads_json(root_folder):
    files = []
    uploadId = str(uuid.uuid4())
    for dirpath, dirnames, filenames in os.walk(root_folder):
        for filename in filenames:
            if filename.endswith(".dcm"):
                file_path = os.path.join(dirpath, filename)
                files.append(
                    {
                        "uri": generate_uri(file_path),
                        "checksum_md5": "d41d8cd98f00b204e9800998ecf8427e",
                        "sizeInBytes": os.path.getsize(file_path),
                    }
                )
    uploads_json = {
        "uploadName": "My First Upload",
        "uploadId": uploadId,
        "userId": "userxxxx",
        "uploadDate": datetime.now(),
        "totalNoFiles": len(files),
        "dataType": "DICOM",
        "tags": ["Hospital1", "Dicom data from source xyz"],
        "files": files,
    }
    return uploads_json, uploadId


# Function to insert records to MongoDB
def insert_records(client, records, collection_name):
    db = client.playground
    collection = db[collection_name]
    collection.insert_many(records)


def aggregate_dicom_pesi(dicom_pesi, dicom_instance, file_path):
    if debug:
        print("***" * 30)
        print(f" In aggregate dicom_pesi, dicom instance is : {dicom_instance}")
        print("^^^" * 30)
        print("dicom_pesi received is : ", dicom_pesi)
        print("^^^" * 30)

    instanceDict = {
        "SOPInstanceUID": dicom_instance["header"]["Metadata"]["SOPInstanceUID"][
            "Value"
        ],
        "URI": generate_uri(file_path),
    }
    seriesUID = dicom_instance["header"]["Metadata"]["SeriesInstanceUID"]["Value"]
    studyUID = dicom_instance["header"]["Metadata"]["StudyInstanceUID"]["Value"]
    instance_number = 0
    try:
        instance_number = int(
            dicom_instance["header"]["Metadata"]["InstanceNumber"]["Value"]
        )
    except:
        pass
    if debug:
        print("########", 1)
    t_series = {
        "SeriesInstanceUID": dicom_instance["header"]["Metadata"]["SeriesInstanceUID"][
            "Value"
        ],
        "SeriesDescription": dicom_instance.get("header")
        .get("Metadata")
        .get("SeriesDescription", {})
        .get("Value", "NA"),
        "SeriesNumber": dicom_instance["header"]["Metadata"]["SeriesNumber"]["Value"],
        "MaxInstanceNumber": instance_number,
        "TotalNumberOfInstancesReceived": 1,
        "instances": [instanceDict],
    }
    if debug:
        print("########", 2)
    t_study = {
        "StudyInstanceUID": dicom_instance["header"]["Metadata"]["StudyInstanceUID"][
            "Value"
        ],
        "StudyDate": dicom_instance["header"]["Metadata"]["StudyDate"]["Value"],
        "StudyTime": dicom_instance["header"]["Metadata"]["StudyTime"]["Value"],
        "StudyDescription": dicom_instance.get("header")
        .get("Metadata")
        .get("StudyDescription", {})
        .get("Value", "NA"),
        "TotalNumberofSeriesReceived": 1,
        "series": [t_series],
    }
    if debug:
        print("########", 3, t_study)

    # Very first instance is added.
    if len(dicom_pesi["studies"]) == 0:
        dicom_pesi["studies"].append(t_study)
        if debug:
            print("########", 4)
    else:
        # At lest 1 instance exists.
        if debug:
            print("########", 5)
        for study in dicom_pesi["studies"]:
            studyFound = False
            # If the study matches
            if study["StudyInstanceUID"] == studyUID:
                if debug:
                    print("########", 6)
                studyFound = True
                # Find if the series in given instance exists in the study.
                seriesFound = False
                for ser in study["series"]:
                    if ser["SeriesInstanceUID"] == seriesUID:
                        if debug:
                            print("########", 7)
                        # Existing Series
                        # add instance data and update details
                        ser["instances"].append(instanceDict)
                        ser["TotalNumberOfInstancesReceived"] += 1
                        ser["MaxInstanceNumber"] = max(
                            ser["MaxInstanceNumber"], int(instance_number)
                        )
                        seriesFound = True
                if not seriesFound:
                    if debug:
                        print("########", 8)
                    # Series was not found, so add it
                    t_study["series"].append(t_series)
                    t_study["TotalNumberofSeriesReceived"] += 1
        if not studyFound:
            if debug:
                print("########", 9)
            # Append new study
            dicom_pesi["studies"].append(t_study)
            if debug:
                print("^^^^^^^^ 10 : ", dicom_pesi, "\n", "!!!" * 30)


# Main function
def main(root_folder, client):
    dicom_instances = []
    dicom_pesi = {}
    dicom_pesi["studies"] = []
    uploads_json, uploadId = create_uploads_json(root_folder)
    dicom_pesi["uploadId"] = uploadId
    if not testing:
        insert_records(client, [uploads_json], "uploads")

    for dirpath, dirnames, filenames in os.walk(root_folder):
        for filename in filenames:
            if debug:
                print("---" * 30)
            if filename.endswith(".dcm"):
                file_path = os.path.join(dirpath, filename)
                if debug:
                    print(f"file_path : {file_path}")
                dicom_instance = parse_dicom_file(file_path, uploadId)
                dicom_instances.append(dicom_instance)
                if debug:
                    print(f"dicom_instance: {dicom_instance}")
                    print(len(dicom_instances))
                aggregate_dicom_pesi(dicom_pesi, dicom_instance, file_path)

    if debug:
        print(f" dicom_pesi: {dicom_pesi}")
        file_path = "./out.json"
        with open(file_path, "w") as json_file:
            json.dump(dicom_pesi, json_file)

    if not testing:
        insert_records(client, dicom_instances, "dicom_instance")
        update_dicom_pesi(client, dicom_pesi)


# Call the main function with the root folder path
client = None
if not testing:
    connection = MongoDBConnection()
    client = connection.connect()

dir_path = ""
dir_path = input("Enter dir path: ")

main(dir_path, client)
if not testing:
    connection.disconnect()
