import argparse
import http.client
import json

class Equity():
    def __init__(self, ticker, name, exchange, industry, dow):
        self.ticker = ticker
        self.name = name
        self.exchange = exchange
        self.industry = industry
        self.dow = dow


def get_existing_equities():
    equities = []

    f = open('equities.csv', 'r')
    for row in f:
        tokens = row.split(',')
        ticker = tokens[0].strip('\n')
        name = tokens[1].strip('\n')
        exchange = tokens[2].strip('\n')
        industry = tokens[3].strip('\n')
        if industry == 'NULL':
            industry = None
        dow_company = tokens[4].strip('\n')
        if dow_company == '1':
            dow_company = True
        else:
            dow_company = False

        equity = Equity(ticker, name, exchange, industry, dow_company)
        equities.append(equity)

    return equities


def post_equity(equity):
    conn = http.client.HTTPConnection('localhost', 5000)

    request_body = {
        'ticker': equity.ticker,
        'name': equity.name,
        'exchange': equity.exchange,
        'dow': equity.dow
    }
    if equity.industry is not None:
        request_body['industry'] = equity.industry
    request_body_str = json.dumps(request_body)

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Content-Length': len(request_body_str)
    }

    conn.request('POST', '/v1/equities', request_body_str, headers)

    raw_response = conn.getresponse()
    # Return the status code as well as the content
    response_status_code = raw_response.status
    print('response status code = {}'.format(response_status_code))
    response_data = json.loads(raw_response.read().decode('utf-8'))
    conn.close()

    print('{} - created id = {}'.format(response_status_code, response_data.get('id')))

def get_all_snapshots_for_equity(ticker):
    conn = http.client.HTTPConnection('localhost', 5000)
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    conn.request('GET', '/v1/equities/{}/snapshots?limit=100000'.format(ticker), None, headers)

    raw_response = conn.getresponse()
    print('received a {} from the server'.format(raw_response.status))

    snapshots = json.loads(raw_response.read().decode('utf-8'))
    conn.close()
    return snapshots

def get_all_aggregates_for_equity(ticker):
    conn = http.client.HTTPConnection('localhost', 5000)
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    conn.request('GET', '/v1/equities/{}/aggregates?limit=100000'.format(ticker), None, headers)

    raw_response = conn.getresponse()
    print('received a {} from the server'.format(raw_response.status))

    aggregates = json.loads(raw_response.read().decode('utf-8'))
    conn.close()
    return aggregates

def get_all_equities():
    conn = http.client.HTTPConnection('localhost', 5000)
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    conn.request('GET', '/v1/equities', None, headers)

    raw_response = conn.getresponse()
    print('received a {} from the server'.format(raw_response.status))

    equities = json.loads(raw_response.read().decode('utf-8'))
    conn.close()
    return equities

def delete_equity(equity):
    conn = http.client.HTTPConnection('localhost', 5000)
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    conn.request('DELETE', '/v1/equities/{}'.format(equity.get('id')), None, headers)

    raw_response = conn.getresponse()
    conn.close()
    print('deleted {} - {} from the database'.format(equity.get('ticker'), equity.get('id')))

def delete_snapshot(snapshot):
    conn = http.client.HTTPConnection('localhost', 5000)
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    conn.request('DELETE', '/v1/equities/{}/snapshots/{}'.format(snapshot.get('ticker'), snapshot.get('id')), None, headers)

    raw_response = conn.getresponse()
    conn.close()
    print('received a {} response from the database deleting {} - aggregate {}'.format(raw_response.status, snapshot.get('ticker'), snapshot.get('id')))

def delete_aggregate(aggregate):
    conn = http.client.HTTPConnection('localhost', 5000)
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    conn.request('DELETE', '/v1/equities/{}/aggregates/{}'.format(aggregate.get('ticker'), aggregate.get('id')), None,
                 headers)

    raw_response = conn.getresponse()
    conn.close()
    print('received a {} response from the database deleting {} - snapshot {}'.format(raw_response.status,
                                                                                      aggregate.get('ticker'),
                                                                                      aggregate.get('id')))

def delete_equities_from_database():
    equities = get_all_equities()
    for equity in equities:
        delete_equity(equity)
        #print('Equity {} has an id of {}'.format(equity.get('ticker'), equity.get('id')))

def load_equities():
    equities = get_existing_equities()
    for equity in equities:
        post_equity(equity)

def delete_snapshots_for_equity(ticker):
    snapshots = get_all_snapshots_for_equity(ticker)
    for snapshot in snapshots:
        delete_snapshot(snapshot)

def delete_aggregates_for_equity(ticker):
    aggregates = get_all_aggregates_for_equity(ticker)
    for aggregate in aggregates:
        delete_aggregate(aggregate)

def delete_everything():
    equities = get_all_equities()
    for equity in equities:
        ticker = equity.get('ticker')
        delete_snapshots_for_equity(ticker)
        delete_aggregates_for_equity(ticker)
        delete_equity(equity)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Gathering arguments')
    parser.add_argument('-l', required=False, dest='load', action='store', help='Load all the equities from equities.csv')
    parser.add_argument('-d', required=False, dest='delete', action='store', help='Deletes ALL equities from the database')
    parser.add_argument('-x', required=False, dest='delete_all', action='store', help='Deletes all EVERYTHING from the database')
    args = parser.parse_args()

    if args.load == 'true':
        load_equities()
    elif args.delete == 'true':
        user_input = input('This will delete ALL equities from the database. Are you sure you\'d like to continue? (Y/N) [N]: ')
        if user_input == 'Y':
            delete_equities_from_database()
        else:
            print('Phew - that was a close one')
    elif args.delete_all == 'true':
        user_input = input('This will delete EVERYTHING from the database. Are you sure you\'d like to continue? (Y/N) [N]: ')
        if user_input == 'Y':
            print('about to delete stuff')
            delete_everything()