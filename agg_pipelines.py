from pymongo import MongoClient
from mongodb_connection import MongoDBConnection


def call_pipeline(client, query):
    pass


if __name__ == "__main__":
    client = None
    connection = MongoDBConnection()
    client = connection.connect()
    studyUID="2.25.77168008002489147186120709736842771958"
    uploadID="b0cdf329-4248-4769-a9e6-03fde557c0ff"
#   the below aggregator pipeline does not work for given schema as per dersired result.    
#     result = client["playground"]["dicom_pesi"].aggregate(
#         [
#             {
#                 "$match": {
#                     "studies.StudyInstanceUID": studyUID
#                 }
#             },
#             {"$unwind": "$studies"},
#             {
#                 "$match": {
#                     "studies.StudyInstanceUID": studyUID
#                 }
#             },
#             {
#     "$project":
#       {
#         "_id": 0,
#       },
#   },
#         ]
#     )

    filter={
        'studies.StudyInstanceUID': '2.25.111992828440641426467630674457300418038', 
        'uploadId': 'b0cdf329-4248-4769-a9e6-03fde557c0ff'
    }
    project={
        '_id': 0
    }
    result = client['playground']['dicom_pesi'].find(
    filter=filter,
    projection=project
    )

    data = list(result)

    from pprint import pprint
    # pprint(data)

    for item in data[0].values():
        if type(item) == list:
            for studies in item:
                if studies["StudyInstanceUID"] == studyUID:
                    pprint(studies)
    connection.disconnect()
