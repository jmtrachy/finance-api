from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client.poc
collection = db.snapshots

snapshots = collection.find(filter={'ticker': 'JAMES'})

snapshot_records_to_delete = []

# Retrieve all the snapshots to clear out
for s in snapshots:
    print('found snapshot with ticker = ' + s['ticker'] + '; id = ' + s['id'] + '...will delete')
    snapshot_records_to_delete.append(s['id'])

# Delete the snapshots that have been marked for deletion
for r in snapshot_records_to_delete:
    collection.delete_one({'id': r})
    print('record ' + r + ' deleted.')

# Retrieve all the aggregates to clear out
collection_a = db.aggregates
aggregates = collection_a.find(filter={'ticker': 'JAMES'})
aggregate_records_to_delete = []

for a in aggregates:
    print('found aggregate with ticker = ' + a['ticker'] + '; id = ' + a['id'] + '...will delete')
    aggregate_records_to_delete.append(a['id'])

# Delete the snapshots that have been marked for deletion
for r in aggregate_records_to_delete:
    collection_a.delete_one({'id': r})
    print('record ' + r + ' deleted.')


#Retrieve all the equities to clear out
collection_e = db.equities
equities = collection_e.find(filter={'ticker': 'JAMES'})
equity_records_to_delete = []

for e in equities:
    print('found equity with ticker = ' + e['ticker'] + '; id = ' + e['id'] + '...will delete')
    equity_records_to_delete.append(e['id'])

# Delete the equities that have been marked for deletion
for r in equity_records_to_delete:
    collection_e.delete_one({'id': r})
    print('record ' + r + ' deleted.')

client.close()




