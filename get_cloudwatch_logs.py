
import boto3
from datetime import datetime


import sys


if len(sys.argv) - 1 < 5:
    print("Enter parameter: ...", sys.argv)
    print("1: profile (default/taegeonum)")
    print("2: start time")
    print("3: end time")
    print("4: number of executor")
    print("5: delete after retrieving log (true/false)")
    sys.exit(0)


profile = sys.argv[1]

num_lambda = int(sys.argv[4])

session = boto3.Session(profile_name = profile, region_name = "ap-northeast-1")
client = session.client('logs')

log_group = "/aws/lambda/lambda-dev-lambda-executor"


start_time = int(sys.argv[2]) * 1000
end_time = int(sys.argv[3]) * 1000
delete = True if sys.argv[5] == "true" else False

def get_log_streams(log_streams, group_name):
    def get_log_stream(log_stream):
        print("[------------------- Printing log stream ", log_stream["logStreamName"], "-----------------]")

        prev_token = None
        while True:

            stream_name = log_stream['logStreamName']
            if prev_token == None:
                if start_time == 0:
                    response = client.get_log_events(
                            logGroupName = group_name,
                            logStreamName = stream_name,
                            startFromHead = True,
                            )
                else: 
                    response = client.get_log_events(
                            logGroupName = group_name,
                            logStreamName = stream_name,
                            startFromHead = True,
                            startTime = start_time,
                            endTime = end_time,
                            )
            else:
                if start_time == 0:
                    response = client.get_log_events(
                            logGroupName = group_name,
                            logStreamName = stream_name,
                            startFromHead = True,
                            nextToken = prev_token
                            )
                else:
                    response = client.get_log_events(
                            logGroupName = group_name,
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
            client.delete_log_stream(logGroupName = group_name, 
                    logStreamName = log_stream['logStreamName'])


prev_token = None 
next_token = None
for i in range(0, num_lambda):
    print("---------- Print Lambda-" + str(i) + "-------")
    while True:

        if prev_token == None:
            result = client.describe_log_streams(
                logGroupName=log_group + str(i+1),
             )
        else:
            result = client.describe_log_streams(
                logGroupName=log_group + str(i+1),
                nextToken = prev_token
            )

        log_streams = result["logStreams"]
        next_token = result.get("nextToken")

        #print("next token: ", next_token)
    
        get_log_streams(log_streams, log_group + str(i+1))

        if not next_token or prev_token == next_token:
            break
        else:
            prev_token = next_token



