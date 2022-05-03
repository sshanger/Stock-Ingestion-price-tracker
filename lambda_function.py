import json
import boto3
import datetime

def lambda_handler(event, context):
	# clients to connect to Kinesis dynamodb and sns notification
	kinesis = boto3.client('kinesis', region_name = "us-east-1")
	dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
	table = dynamodb.Table('POI_Alerts')
	sns = boto3.client('sns', region_name='us-east-1')
	topic_arn = "arn:aws:sns:us-east-1:653173704254:PointOfInterestAlert"
	
	response = kinesis.describe_stream(StreamName='stock_ticker_stats')
	shard_id = response['StreamDescription']['Shards'][0]['ShardId']
	# Get the Shard number
	shard_iterator = kinesis.get_shard_iterator(StreamName="stock_ticker_stats",ShardId=shard_id, ShardIteratorType="LATEST")['ShardIterator']
	record_response = kinesis.get_records(ShardIterator = shard_iterator, Limit = 10)
	poi_alert_dict= {}
	alert_triggered = {}
	# Read 10 records from the shard number
	while 'NextShardIterator' in record_response:
		record_response = kinesis.get_records(ShardIterator=record_response['NextShardIterator'], Limit=10)
		if(record_response['Records']):
			stock_stats = record_response['Records'][0]['Data'].decode('utf8')
			stock_stats = json.loads(stock_stats)
			if round(stock_stats['close_price'],2) >= round(0.8 * stock_stats['fiftyTwoWeekHigh'], 2):
				date = datetime.datetime.strptime(stock_stats['price_timestamp'], "%Y-%m-%d %H:%M:%S").date()
				# check if alert was already triggered for the stock ticker for the day from the journal dictionary.
				if date in alert_triggered.keys() and stock_stats['stockid'] in alert_triggered[date].keys():
					pass
				else:
					add_alerts_to_dict(alert_triggered, date, stock_stats)
					message = f"{stock_stats['stockid']} close_price :{round(stock_stats['close_price'], 2)} is greater than 80% of fiftyTwoWeekHigh :{round(stock_stats['fiftyTwoWeekHigh'], 2)}"
					add_items_into_poi_alert_dict(message, poi_alert_dict, stock_stats)
					# publish item to dynamodb.
					table.put_item(Item=poi_alert_dict)
					# push notification to the SNS.
					result = publish_to_sns(poi_alert_dict, sns, topic_arn)
					
			elif round(stock_stats['close_price'], 2) <= round((1.2 * stock_stats['fiftyTwoWeekLow']), 2):
				date = datetime.datetime.strptime(stock_stats['price_timestamp'], "%Y-%m-%d %H:%M:%S").date()
				if date in alert_triggered.keys() and stock_stats['stockid'] in alert_triggered[date].keys():
					pass
				else:
					add_alerts_to_dict(alert_triggered, date, stock_stats)
					message = f"{stock_stats['stockid']} close_price :{round(stock_stats['close_price'],2)} is lesser than 120% of fiftyTwoWeekLow :{round(stock_stats['fiftyTwoWeekLow'], 2)}"
					add_items_into_poi_alert_dict(message, poi_alert_dict, stock_stats)
					# publish item to dynamodb.
					table.put_item(Item=poi_alert_dict)
					# push notification to the SNS.
					result = publish_to_sns(poi_alert_dict, sns, topic_arn)
					return result

# Method to publish alerts to SNS.
def publish_to_sns(poi_alert_dict, sns, topic_arn):
	try:
		sns.publish(TopicArn=topic_arn, Message=f"{poi_alert_dict['message']}",
					Subject=f"Point of interest Alert triggered : {poi_alert_dict['stockid']}")
		result = 1
	except Exception:
		result = 0
	return result

# method to add item into POI alerts dictionary.
def add_items_into_poi_alert_dict(message, poi_alert_dict, stock_stats):
	poi_alert_dict['stockid'] = stock_stats['stockid']
	poi_alert_dict['price_timestamp'] = stock_stats['price_timestamp']
	poi_alert_dict['message'] = message

# method to add item into alerts journal dictionary.
def add_alerts_to_dict(alert_triggered, date, stock_stats):
	alert_triggered[date] = {}
	alert_triggered[date][stock_stats['stockid']] = {}
	alert_triggered[date][stock_stats['stockid']]['price_timestamp'] = stock_stats['price_timestamp']