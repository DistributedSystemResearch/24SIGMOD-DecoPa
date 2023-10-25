import argparse
import logging
import string
from datetime import datetime, timedelta
import pathlib
import socket
import time
import uuid

def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

    input_file = args.input_file or  \
                 str(pathlib.Path(__file__).parent.resolve()) + f"/trace_{args.nodeId}.csv"

    client_ip  = args.host
    client_port = 5500 + args.nodeId

    # read event stream input txt
    with open(input_file) as f:
        event_stream = f.readlines()

    print("read:", input_file)
    read_and_send_event_stream(event_stream, client_ip, client_port)


#send data with this function
def send(socket, eventtype, timestamp, event_id, creation_timestamp, attribute_values):
    timestamp_string = str(creation_timestamp)
    timestamp_string = timestamp_string.replace(".", ":")
    print(timestamp_string)
    message = "simple | %s | %s | %s\n" % (event_id, timestamp_string, eventtype)
    message = message.replace('\r', '').replace('\n', '')

    for attr in attribute_values:
        message += "|" + attr

    message += " \n"
    print(message)
    
    try:
        socket.send(message.encode(encoding="UTF-8"))
        print("Message sent!")
    except Exception as error:
        print("Error - message not sent!",error)

def send_end_of_the_stream_message(socket):
    try:
        socket.send("end-of-the-stream\n".encode(encoding="UTF-8"))
        print("end-of-the-stream!!")
    except Exception as error:
        print("Error - message not sent!",error)

def read_and_send_event_stream(event_stream, client_ip, client_port):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((str(client_ip), client_port))

    event_type_universe = string.ascii_uppercase[0:25]

    repeated_time_counters = dict()
    prev_event_time = None
    
    for event in event_stream:
        event = event.strip()
        attributes = event.split(",")
        timestamp = attributes[0]
        event_type = attributes[1]

        if event_type not in event_type_universe:
            continue
        
        
        try:
        	hours, minutes, seconds, us = map(int, timestamp.split(':'))
        except Exception:
            hours, minutes, seconds = map(int, timestamp.split(':'))
            us = 0	

        event_time = timedelta(days=hours//24, hours=(hours-(hours//24)*24), minutes=minutes, seconds=seconds, microseconds=us)
        
        if prev_event_time is not None:
            delay = (event_time - prev_event_time).total_seconds()
            time.sleep(delay)
        
        prev_event_time = event_time

        #event_id = str(uuid.uuid4())[0:8]  # random event id
	
        creation_timestamp = datetime.now() # + event_time
        event_id = attributes[2]
        
        attribute_values = attributes[3:]

        send(client_socket, event_type, None, event_id, creation_timestamp.strftime("%H:%M:%S.%f"), attribute_values)

    # signal the end of the stream by sending a "end-of-the-stream" message
    send_end_of_the_stream_message(client_socket)

def parse_args():
    parser = argparse.ArgumentParser(description='Send event data to the given tcp address.')
    parser.add_argument("nodeId", type=int, default=0, help="Node id to use. Will use config_id, trace_id, port 5500+id and localhost, unless overridden by long options")
    parser.add_argument("-c", "--config-file", help="Path to config file", type=str, required=False)
    parser.add_argument("-f", "--input-file", help="Path to the input data file (override). ", type=str, required=False)
    parser.add_argument("-p", "--port", type=int, help="Client port", required=False, default=None)
    parser.add_argument("-H", "--host", type=str, default="localhost", required=False, help="The host")
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()

