curl -v -X POST -H "Content-Type: application/json" -d '{"createdDate":"10-13-2016","date": "02-26-2017", "ticker":"JAMES", "fiftyDayMovingAverage": 50.01, "fiftyDayVolatilityAverage": 0.43, "perOffRecentHigh": 8.0, "perOffRecentLow": 3.4}' "http://localhost:5000/v1/aggregates"

curl -v -X POST -H "Content-Type: application/json" -d '{"createdDate":"02-24-2017","date": "02-25-2017", "ticker":"JAMES", "fiftyDayMovingAverage": 50.00, "fiftyDayVolatilityAverage": 0.42, "perOffRecentHigh": 7.9, "perOffRecentLow": 3.3}' "http://localhost:5000/v1/aggregates"

curl -v -X POST -H "Content-Type: application/json" -d '{"createdDate":"02-24-2017","date": "02-24-2017", "ticker":"JAMES", "fiftyDayMovingAverage": 49.99, "fiftyDayVolatilityAverage": 0.41, "perOffRecentHigh": 7.8, "perOffRecentLow": 3.2}' "http://localhost:5000/v1/aggregates"
