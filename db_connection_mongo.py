#-------------------------------------------------------------------------
# AUTHOR: Nathaniel Battad
# FILENAME: db_connection_mongo.py
# SPECIFICATION: creates, updates, and queries MongoDB database tables
# FOR: CS 4250- Assignment #3
# TIME SPENT: about 2-3 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here

from pymongo import MongoClient
from string import punctuation

def connectDataBase():
    # Create a database connection object using pymongo
    DB_NAME = "assignment3"
    DB_HOST = "localhost"
    DB_PORT = 27017

    try:
        client = MongoClient(host = DB_HOST, port = DB_PORT)
        db = client[DB_NAME]
        return db
    except:
        print("Database not connected successfully")

def createDocument(col, docId, docText, docTitle, docDate, docCat):

# create a dictionary indexed by term to count how many times each term appears in the document.
    term_appearances = {}
# Use space " " as the delimiter character for terms and remember to lowercase them.
    clearPunctuation = ''.join(char for char in docText if char not in punctuation)
    terms = clearPunctuation.lower().split(" ")
    for term in terms:
        term_appearances[term] = term_appearances.get(term, 0) + 1

# create a list of objects to include full term objects. [{"term", count, num_char}]
    term_objects = [{"term": term, "count": count, "num_char": len(term)} for term, count in term_appearances.items()]

# produce a final document as a dictionary including all the required document fields
    document = {
        "_id": docId,
        "title": docTitle,
        "text": docText,
        "num_chars": sum(not char.isspace() for char in clearPunctuation),
        "date": docDate,
        "category": docCat,
        "terms": term_objects
    }

# insert the document
    col.insert_one(document)

def deleteDocument(col, docId):
# Delete the document from the database
    col.delete_one({"_id": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):
# Delete the document
    deleteDocument(col, docId)

# Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):
# Query the database to return the documents where each term occurs with their corresponding count. Output example:
# {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
# ...
    index = {}
    cursor = col.find({})
    for doc in cursor:
        for termObject in doc["terms"]:
            term = termObject["term"]
            count = termObject["count"]
            if term in index:
                index[term].append(f"{doc['title']}:{count}")
            else:
                index[term] = [f"{doc['title']}:{count}"]
    return index