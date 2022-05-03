import json
import boto3
import yfinance as yf
import datetime


# Your goal is to get per-hour stock price data for a time range for the ten stocks specified in the doc. 
# Further, you should call the static info api for the stocks to get their current 52WeekHigh and 52WeekLow values.
# You should craft individual data records with information about the stockid, price, price timestamp, 52WeekHigh and 52WeekLow values and push them individually on the Kinesis stream

kinesis = boto3.client('kinesis', region_name = "us-east-1") #Modify this line of code according to your requirement.

today = datetime.date.today()
yesterday = datetime.date.today() - datetime.timedelta(1)

## Add code to pull the data for the stocks specified in the doc
tickers = "MSFT,MVIS,GOOG,SPOT,INO,OCGN,ABML,RLLCF,JNJ,PSFE"
data = yf.download(tickers, start= yesterday, end= today, interval = '1h', group_by='Ticker')

## Add additional code to call 'info' API to get 52WeekHigh and 52WeekLow refering this this link - https://pypi.org/project/yfinance/
tickers_list = tickers.split(',')
counter = data[tickers_list[0]]["Close"].count()
ticker_count = 0
stock_info = {}
input_stream = {}
input_stream['stock_stats'] = {}
while ticker_count < len(tickers_list):
    stockid = tickers_list[ticker_count]
    stock_info[stockid]={}
    stock = yf.Ticker(stockid).info
    stock_info[stockid]['fiftyTwoWeekLow'] = stock['fiftyTwoWeekLow']
    stock_info[stockid]['fiftyTwoWeekHigh'] = stock['fiftyTwoWeekHigh']
    ticker_count = ticker_count + 1
print(f'Stock stats with  Fifty two weeks low and high:\n {stock_info}')
print('----------------------------------------------------------------------------------------------------')


## Add your code here to push data records to Kinesis stream.
## Loop through the stock list and construct stock data for acquired per hour and stream it to Kinesis
stock_counter = 0
per_hour_counter = 0
timeIndexMax = 6
while stock_counter < len(tickers_list):
    while per_hour_counter <= 6:
        stream = {}
        stock_id = tickers_list[stock_counter]
        stream['stockid'] = stock_id
        stream['price_timestamp'] = (data[tickers_list[stock_counter]]['Close'].index[per_hour_counter]).strftime('%Y-%m-%d %H:%M:%S')
        stream['close_price'] = data[stock_id]['Close'][per_hour_counter]
        stream['fiftyTwoWeekLow'] = stock_info[tickers_list[stock_counter]]['fiftyTwoWeekLow']
        stream['fiftyTwoWeekHigh'] = stock_info[tickers_list[stock_counter]]['fiftyTwoWeekHigh']
        print(f'Sending Data to Kinesis Stream : {stream}')
        kinesis.put_record(StreamName='stock_ticker_stats',
                    Data=json.dumps(stream),
                                PartitionKey=stock_id)
        per_hour_counter = per_hour_counter + 1
    # Reset the per hour counter for iterating through other stock tickers.
    per_hour_counter = 0
    stock_counter = stock_counter + 1