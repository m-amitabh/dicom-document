from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()


def connect_this():
    uri = os.getenv('MONGO_URI')

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi("1"))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command("ping")
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    return client


class MongoDBConnection:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.client = None
        return cls._instance

    def connect(self):
        if self.client is None:
            self.client = connect_this()
        return self.client

    def disconnect(self):
        if self.client is not None:
            self.client.close()
            self.client = None


if __name__ == "__main__":
    # Example usage:
    connection = MongoDBConnection()
    client = connection.connect()  # Connect to MongoDB
    print("Connected to MongoDB")

    # Use the client for database operations...
    for db_name in client.list_database_names():
        print(db_name)
    db = client.playground
    for doc in db.uploads.find():
        print(doc)

    # Disconnect from MongoDB
    connection.disconnect()
    print("Disconnected from MongoDB")
