import pandas as pd
from datetime import datetime
from collections import deque
from dataclasses import dataclass
from confluent_kafka import Consumer
from confluent_kafka import Producer
import json
from sklearn.ensemble import IsolationForest


@dataclass
class Event:
    start: datetime
    usage: float

    def toDict(self):
        return {
            "start" : self.start,
            "usage" : self.usage,
            "day" : self.start.weekday(),
            "hour" : self.start.hour,
            "minutes" : self.start.minute }

class FixedEventList:
    
    def __init__(self, max_size) -> None:
        self._queue = deque( maxlen= max_size )
    
    def add(self, ev:Event ):
        self._queue.append(ev)
    
    def toDict(self):
        return list (
            map(
                lambda x: x.toDict(),
                self._queue
            )
        )
    
    def toDataFrame(self):
        return pd.DataFrame(self.toDict())
    
    def getComparableSet(self, ev: Event):
        d = ev.start.weekday()
        h = ev.start.hour
        m = ev.start.minute
        df = self.toDataFrame()
        
        #Guard-statement:
        if self.size() == 0:
            return None
        
        #Return:
        return df[
            (df.day == d) &
            (df.hour == h) &
            (df.minutes == m)
        ]
    
    def size(self):
        return len(self._queue)

## Main
## if __name__ == "__main__":

# Holds a 30 day window:
history = FixedEventList( 30*285 )
iso_forest = IsolationForest(contamination="auto", random_state=42)

kafka_config_p = {
    'bootstrap.servers': "b-2.csciemsk.knyb5i.c1.kafka.us-east-1.amazonaws.com:9092",
    'client.id': "ml_producer" }

kafka_config_c = {
    'bootstrap.servers': "b-2.csciemsk.knyb5i.c1.kafka.us-east-1.amazonaws.com:9092",
    'group.id': 'ml_consumer' }

c = Consumer(kafka_config_c)
c.subscribe( ["usage"] )

p = Producer (kafka_config_p)

# Consume:
while (True):
    msg = c.poll(timeout=10.0)

    if msg is None:
        print ("No messages, continuing...")
        continue
    
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    msg_json = json.loads(msg.value().decode('utf-8'))
    new_event = Event ( 
        datetime.strptime( msg_json['ts'], "%Y-%m-%d %H:%M:%S"),
        msg_json['value']
    )
    print (f"New event: {msg_json['ts']} - {msg_json['value']}")

    comps = history.getComparableSet( new_event )
    history.add( new_event )
    print(f"History: {history.size()}")

    # If not enough comps:
    if comps is None:
        print( "No comps found." )
        continue
    
    print( f"{len(comps)} comps found." )

    if len(comps) < 4:
        continue
    
    # Build Isolation model:
    iso_forest.fit( comps[["usage"]] )
    
    # Score new point:
    new_item = pd.DataFrame([new_event.toDict()])
    prediction = iso_forest.predict( new_item[["usage"]] )

    print (f"Anomaly: {prediction[0]} - {msg_json['value']}")
    if prediction[0] == -1:
        print("Publishing anomaly")
        pl = {"ts": msg_json["ts"],"usage" : msg_json["value"]}
        p.produce("anomaly", value=json.dumps(pl))
