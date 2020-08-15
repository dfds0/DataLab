import pymongo
import pprint
import urllib.parse
import uuid
import pandas as pd

username = urllib.parse.quote_plus('USERNAME')
password = urllib.parse.quote_plus('PASSWORD')
mondo_uri = "mongodb+srv://%s:%s@cluster0-oqncw.mongodb.net/test?retryWrites=true&w=majority&ssl=true"%(username, password)

# Create the connection
client = pymongo.MongoClient(mondo_uri)

# Database in the client
databases = client.list_database_names()
print("\n - Databases:", databases)

# Load the database
database = client['sample_analytics']

# Load a collection
customers = database['customers']

# Load a instance of Customer
instance = customers.find_one()

# Key in the instance of Customer
keys = instance.keys()
print("\n - Key of Customers:", keys)

# Add new instance of Customer
new_instance_draft = { "username":uuid.uuid4() , "name":"New User", "email":"new_user@user.com" }
new_instance_id = customers.insert_one(new_instance_draft).inserted_id

# Interlude
print("\n - New instance:", new_instance_id)

# Load the instance created
new_instance_full = customers.find_one({"_id": new_instance_id})
print("\n - Username of instance:", new_instance_full['username'])

# Delete the instance created
customers.delete_one({"_id": new_instance_id})
new_instance_full = customers.find_one({"_id": new_instance_id})
print("\n - Instance:", new_instance_full)

# Load 100 customers
samples = customers.find().sort("_id",pymongo.DESCENDING)[:100]
dataframe = pd.DataFrame(samples)
#dataframe.head()

dataframe.to_csv('Customes.csv',index=False)

print("")

print("CIAO!")
