from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client.poc
collection = db.snapshots

snapshots = collection.find(filter={'ticker': 'JAMES'})

records_to_delete = []

for s in snapshots:
    print('found ticker = ' + s['ticker'] + '; id = ' + s['id'] + '...will delete')
    records_to_delete.append(s['id'])

for r in records_to_delete:
    collection.delete_one({'id': r})
    print('record ' + r + ' deleted.')




