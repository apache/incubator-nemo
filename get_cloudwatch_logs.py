
import boto3
from datetime import datetime

client = boto3.client('logs')

log_group = "/aws/lambda/nemo-dev-tg-erverless-worker"


import sys


if len(sys.argv) - 1 < 3:
    print("Enter parameter: ...", sys.argv)
    print("1: start time")
    print("2: end time")
    print("3: delete after retrieving log (true/false)")
    sys.exit(0)


start_time = int(sys.argv[1]) * 1000
end_time = int(sys.argv[2]) * 1000
delete = True if sys.argv[3] == "true" else False

def get_log_streams(log_streams):
    def get_log_stream(log_stream):
        print("[------------------- Printing log stream ", log_stream["logStreamName"], "-----------------]")

        prev_token = None
        while True:

            stream_name = log_stream['logStreamName']
            if prev_token == None:
                if start_time == 0:
                    response = client.get_log_events(
                            logGroupName = log_group,
                            logStreamName = stream_name,
                            startFromHead = True,
                            )
                else: 
                    response = client.get_log_events(
                            logGroupName = log_group,
                            logStreamName = stream_name,
                            startFromHead = True,
                            startTime = start_time,
                            endTime = end_time,
                            )
            else:
                if start_time == 0:
                    response = client.get_log_events(
                            logGroupName = log_group,
                            logStreamName = stream_name,
                            startFromHead = True,
                            nextToken = prev_token
                            )
                else:
                    response = client.get_log_events(
                            logGroupName = log_group,
                            logStreamName = stream_name,
                            startFromHead = True,
                            startTime = start_time,
                            endTime = end_time,
                            nextToken = prev_token
                            )

            events = response["events"]
            next_token = response.get("nextForwardToken")
        
            for event in events:
                timestamp = event["timestamp"] // 1000
                message = event["message"]
                dt_object = datetime.fromtimestamp(timestamp)
                t = dt_object.strftime("%y/%m/%d %H:%M:%S")
                line = (t + " " + message).strip()
                print(line)

            if not next_token or prev_token == next_token:
                break
            else:
                prev_token = next_token

    for log_stream in log_streams:
        get_log_stream(log_stream)
        if delete:
            client.delete_log_stream(logGroupName = log_group, 
                    logStreamName = log_stream["logStreamName"])


prev_token = None 
next_token = None
while True:

    if prev_token == None:
        result = client.describe_log_streams(
            logGroupName=log_group,
            )
    else:
        result = client.describe_log_streams(
            logGroupName=log_group,
            nextToken = prev_token
            )

    log_streams = result["logStreams"]
    next_token = result.get("nextToken")

    #print("next token: ", next_token)
    
    get_log_streams(log_streams)

    if not next_token or prev_token == next_token:
        break
    else:
        prev_token = next_token



