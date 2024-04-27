import pandas as pd
import datetime
import time
from confluent_kafka import Producer
import socket
import argparse
import json

## Utility Classes

class MaxIterationsException ( Exception) :
    '''Exception thrwon when reaches max iterations limit.'''
    def __init__(self, max_iterations) -> None:
        self.max_iterations = max_iterations

    def __str__(self) -> str:
        return f"Reached max iterations ({self.max_iterations})"

class KafkaProducerFileStreamer:
    '''Wrapper class to turn a file into a stream for Kafka Producer.'''
    def __init__(self, process_handlers, step_frequency:int, accelerator:int = 1) -> None:
        self._process_handlers = process_handlers
        self._step_frequency = step_frequency
        self._running = True
        self._accelerator = accelerator

    def load_file ( self, file_path, time_col = "start_time", value_col = "value"):
        '''Load a file for stream production.'''
        self._df = pd.read_csv(file_path)
        self._time_col = time_col
        self._df [ self._time_col ] = pd.to_datetime( self._df [ self._time_col] )
        self._value_col = value_col

    def stop_stream(self):
        '''Stop the stream.'''
        self._running = False

    def start_stream(self, polling_start_dt:datetime, curr_iteration = 0, max_iterations = None):
        '''Starts Kafka Producer stream.
            params:
            - polling_start_dt - start for polling
            - value_col - name of column for value for Kafka production
            - curr_iteration - offset iteration to start from.
            - max_iterations - number of time iterations to execute.
        '''
        if self._time_col == None:
            raise Exception("load_file needs to be called before starting stream.")

        df = self._df

        while (self._running):
            # Guard Statement:
            if max_iterations and curr_iteration >= max_iterations:
                raise MaxIterationsException(max_iterations)
            
            # Time bound filtering:
            start_time = polling_start_dt + datetime.timedelta(minutes = curr_iteration * self._step_frequency * self._accelerator)
            end_time = start_time + datetime.timedelta( minutes = self._step_frequency * self._accelerator)
            
            res_df = df [ 
                (df[self._time_col] >= start_time) & 
                (df[self._time_col] <  end_time) 
            ]
            
            print( f"{curr_iteration} -- start: {start_time} to end: {end_time}")
            print (f"Found {len(res_df)} records.")

            # Process Values:
            for indx, row in res_df.iterrows():
                for p in self._process_handlers:
                    json_payload = {
                        "ts" : row[self._time_col].strftime("%Y-%m-%d %H:%M:%S"),
                        "value" : row[self._value_col]
                    }
                    p( json.dumps(json_payload) )
                
            curr_iteration = curr_iteration + 1
            time.sleep( self._step_frequency )

## Handler Functions
def print_handler( value ):
    print (f"Processing value: {value}.")

def kafka_producer_handler( producer, topic ):
    
    def handler ( json_payload ):
        producer.produce(topic, value=json_payload)
        producer.flush()
    return handler

## Main
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser("Kafka Stream Producer")
    parser.add_argument("--bootstrap_server", help="hostname:port to Kafka broker.", type=str, )
    parser.add_argument("--file_path", help="path to file as base for stream", type=str, )
    parser.add_argument("--step_frequency", help="how often to poll for new stream data.", type=int, default=5)
    parser.add_argument("--accelerator", help="factor to speed up time for demo.", type=int, default=1)
    args = parser.parse_args()
    
    kafka_config = {
        'bootstrap.servers': args.bootstrap_server,
        'client.id': socket.gethostname()
    }
    
    handlers = [
        print_handler,
        kafka_producer_handler(Producer(kafka_config), "usage")
    ]

    stream_producer = KafkaProducerFileStreamer( handlers, args.step_frequency, args.accelerator )
    stream_producer.load_file( 
                args.file_path,
                time_col = "usage_datetime_start_eastern_time",
                value_col = "usage_kw" )
    
    # Start stream execution:
    stream_producer.start_stream( datetime.datetime(2023,1,13) )