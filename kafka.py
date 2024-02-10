import json
import requests 
from kafka import kafkaPoducer
from sleep import sleep

producer = kafkaPoducer(bootstrap_server=['localhost:9092'],
                        value_serializer =(lambda x: json.dump(x).endcode('utf-8')))

for i in range(50):
    res = request.get('http://api.open-notify.org/iss-now.json')
    data=json.loads(res.content.decode('utf-8'))
    print(data)
    producer.send("testtopic",value=data)
    sleep(5)
    producer.flush()
7