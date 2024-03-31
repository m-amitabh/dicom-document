from pymongo import MongoClient
from mongodb_connection import MongoDBConnection


def call_pipeline(client, query):
    pass


if __name__ == "__main__":
    client = None
    connection = MongoDBConnection()
    client = connection.connect()
    studyUID="1.3.6.1.4.1.14519.5.2.1.2135.6389.110097821188395872519993504556"
    result = client["playground"]["dicom_pesi"].aggregate(
        [
            {
                "$match": {
                    "studies.StudyInstanceUID": studyUID
                }
            },
            {"$unwind": "$studies"},
            #{"$unwind": "$studies.StudyInstanceUID"},
            {
                "$match": {
                    "studies.StudyInstanceUID": studyUID
                }
            },
            {
    "$project":
      {
        "_id": 0,
      },
  },
        ]
    )
    output = list(result)
    print(type(output))
    import json
    with open('./out1.json', 'w') as agg_out:
        json.dump(output, agg_out)

    connection.disconnect()
