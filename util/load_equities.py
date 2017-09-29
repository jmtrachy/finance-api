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
    response_data = json.loads(raw_response.read().decode("utf-8"))

    print('{} - created id = {}'.format(response_status_code, response_data.get('id')))


def load_equities():
    equities = get_existing_equities()
    for equity in equities:
        post_equity(equity)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Gathering arguments')
    parser.add_argument("-l", required=False, dest="load", action="store", help="Load all the equities from equities.csv")
    args = parser.parse_args()

    if args.load == 'true':
        load_equities()