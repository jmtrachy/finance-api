import argparse
import http.client
import json
import concurrent.futures


class Equity():
    def __init__(self, ticker, name, exchange, industry, dow):
        self.ticker = ticker
        self.name = name
        self.exchange = exchange
        self.industry = industry
        self.dow = dow


class Snapshot():
    def __init__(self, ticker, created_date, date, price, price_change, price_change_percent, dividend, dividend_yield, pe):
        self.ticker = ticker
        self.created_date = created_date
        self.date = date
        self.price = price
        self.price_change = price_change
        self.price_change_percent = price_change_percent
        self.dividend = dividend
        self.dividend_yield = dividend_yield
        self.pe = pe


class Aggregate():
    def __init__(self, ticker, created_date, date, fifty_day_moving_avg, fifty_day_volatility_avg,
                 per_off_recent_high, per_off_recent_low):
        self.ticker = ticker
        self.created_date = created_date
        self.date = date
        self.fifty_day_moving_avg = fifty_day_moving_avg
        self.fifty_day_volatility_avg = fifty_day_volatility_avg
        self.per_off_recent_high = per_off_recent_high
        self.per_off_recent_low = per_off_recent_low


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
    f.close()

    return equities

def get_existing_snapshots():
    snapshots = []

    f = open('snapshots.csv', 'r')
    for row in f:
        tokens = row.split(',')
        ticker = tokens[0].strip('\n')
        created_date = tokens[1].strip('\n')
        date = tokens[2].strip('\n')
        price = tokens[3].strip('\n')
        price_change = tokens[4].strip('\n')
        price_change_percent = tokens[5].strip('\n')
        dividend = tokens[6].strip('\n')
        if dividend == 'NULL':
            dividend = None
        dividend_yield = tokens[7].strip('\n')
        if dividend_yield == 'NULL':
            dividend_yield = None
        pe = tokens[8].strip('\n')
        if pe == 'NULL':
            pe = None

        snapshot = Snapshot(ticker, created_date, date, price, price_change, price_change_percent, dividend, dividend_yield, pe)
        snapshots.append(snapshot)

    f.close()
    return snapshots

def get_existing_aggregates():
    aggregates = []

    f = open('all_aggregates.csv', 'r')
    for row in f:
        tokens = row.split(',')
        ticker = tokens[0].strip('\n')
        created_date = tokens[1].strip('\n')
        date = tokens[2].strip('\n')
        fifty_day_moving_avg = tokens[3].strip('\n')
        fifty_day_volatility_avg = tokens[4].strip('\n')
        per_off_recent_high = tokens[5].strip('\n')
        per_off_recent_low = tokens[6].strip('\n')

        aggregate = Aggregate(ticker, created_date, date, fifty_day_moving_avg, fifty_day_volatility_avg,
                              per_off_recent_high, per_off_recent_low)
        aggregates.append(aggregate)

    f.close()
    return aggregates


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

def post_snapshot(snapshot):
    conn = http.client.HTTPConnection('localhost', 5000)

    request_body = {
        'createdDate': snapshot.created_date,
        'date': snapshot.date,
        'price': snapshot.price,
        'priceChange': snapshot.price_change,
        'priceChangePercent': snapshot.price_change_percent
    }

    if snapshot.dividend is not None:
        request_body['dividend'] = snapshot.dividend
    if snapshot.dividend_yield is not None:
        request_body['dividendYield'] = snapshot.dividend_yield
    if snapshot.pe is not None:
        request_body['pe'] = snapshot.pe

    request_body_str = json.dumps(request_body)

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Content-Length': len(request_body_str)
    }

    conn.request('POST', '/v1/equities/{}/snapshots'.format(snapshot.ticker), request_body_str, headers)
    #print('about to post to {} with {}'.format(snapshot.ticker, request_body_str))

    raw_response = conn.getresponse()
    # Return the status code as well as the content
    response_status_code = raw_response.status
    print('received {} when loading snapshot for ticker {}'.format(response_status_code, snapshot.ticker))
    response_data = json.loads(raw_response.read().decode('utf-8'))
    conn.close()

def post_aggregate(aggregate):
    conn = http.client.HTTPConnection('localhost', 5000)

    request_body = {
        'createdDate': aggregate.created_date,
        'date': aggregate.date,
        'fiftyDayMovingAverage': aggregate.fifty_day_moving_avg,
        'fiftyDayVolatilityAverage': aggregate.fifty_day_volatility_avg,
        'perOffRecentHigh': aggregate.per_off_recent_high,
        'perOffRecentLow': aggregate.per_off_recent_low
    }

    request_body_str = json.dumps(request_body)

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Content-Length': len(request_body_str)
    }

    conn.request('POST', '/v1/equities/{}/aggregates'.format(aggregate.ticker), request_body_str, headers)
    # print('about to post to {} with {}'.format(snapshot.ticker, request_body_str))

    raw_response = conn.getresponse()
    # Return the status code as well as the content
    response_status_code = raw_response.status
    print('received {} when loading aggregate for ticker {}'.format(response_status_code, aggregate.ticker))
    response_data = json.loads(raw_response.read().decode('utf-8'))
    conn.close()

def get_all_snapshots_for_equity(ticker):
    conn = http.client.HTTPConnection('localhost', 5000)
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    conn.request('GET', '/v1/equities/{}/snapshots?limit=100000'.format(ticker), None, headers)

    raw_response = conn.getresponse()
    #print('received a {} when retrieving snapshots for {}'.format(raw_response.status, ticker))

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
    print('received a {} when retrieving aggregates for {}'.format(raw_response.status, ticker))

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
    #print('received a {} from the server'.format(raw_response.status))

    equities = json.loads(raw_response.read().decode('utf-8'))
    conn.close()
    return equities


def delete_equity(equity, num):
    conn = http.client.HTTPConnection('localhost', 5000)
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    conn.request('DELETE', '/v1/equities/{}'.format(equity.get('id')), None, headers)

    raw_response = conn.getresponse()
    conn.close()
    print('{}) deleted {} - {} from the database'.format(num, equity.get('ticker'), equity.get('id')))


def delete_snapshot(snapshot):
    conn = http.client.HTTPConnection('localhost', 5000)
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    conn.request('DELETE', '/v1/equities/{}/snapshots/{}'.format(snapshot.get('ticker'), snapshot.get('id')), None, headers)

    raw_response = conn.getresponse()
    conn.close()
    #print('received a {} response from the database deleting {} - snapshot {}'.format(raw_response.status, snapshot.get('ticker'), snapshot.get('id')))


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
    print('received a {} response from the database deleting {} - aggregate {}'.format(raw_response.status,
                                                                                      aggregate.get('ticker'),
                                                                                      aggregate.get('id')))


def delete_equities_from_database():
    equities = get_all_equities()
    the_count = 1
    for equity in equities:
        delete_equity(equity, the_count)
        the_count += 1
        #print('Equity {} has an id of {}'.format(equity.get('ticker'), equity.get('id')))


def load_all_data():
    load_equities()
    load_snapshots()
    load_aggregates()


def load_equities():
    equities = get_existing_equities()
    for equity in equities:
        post_equity(equity)


def load_snapshots():
    snapshots = get_existing_snapshots()

    # Multi-thread the event across 20 threads
    executor = concurrent.futures.ProcessPoolExecutor(2)
    futures = [executor.submit(post_snapshot, snapshot) for snapshot in snapshots]
    concurrent.futures.wait(futures)

    # Free up some memory
    snapshots = None


def load_aggregates():
    # Get existing aggregates and load them
    aggregates = get_existing_aggregates()
    for aggregate in aggregates:
        post_aggregate(aggregate)

    # Free up some memory
    aggregates = None


def delete_snapshots_for_equity(ticker):
    snapshots = get_all_snapshots_for_equity(ticker)
    print('About to delete {} snapshots from {}'.format(len(snapshots), ticker))

    # Multi-thread the event across 20 threads
    executor = concurrent.futures.ProcessPoolExecutor(2)
    futures = [executor.submit(delete_snapshot, snapshot) for snapshot in snapshots]
    concurrent.futures.wait(futures)

    #for snapshot in snapshots:
    #    delete_snapshot(snapshot)


def delete_aggregates_for_equity(ticker):
    aggregates = get_all_aggregates_for_equity(ticker)
    for aggregate in aggregates:
        delete_aggregate(aggregate)


def delete_everything():
    equities = get_all_equities()
    the_count = 1
    for equity in equities:
        ticker = equity.get('ticker')
        delete_snapshots_for_equity(ticker)
        delete_aggregates_for_equity(ticker)
        delete_equity(equity, the_count)
        the_count += 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Gathering arguments')
    parser.add_argument('-l', required=False, dest='load', action='store', help='Load all the equities from equities.csv')
    parser.add_argument('-d', required=False, dest='delete', action='store', help='Deletes ALL equities from the database')
    parser.add_argument('-x', required=False, dest='delete_all', action='store', help='Deletes all EVERYTHING from the database')
    args = parser.parse_args()

    if args.load == 'true':
        load_all_data()
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