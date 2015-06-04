#!/usr/bin/python
import sys
import time
import subprocess
from kafka import SimpleProducer, KafkaClient

brokerList = ["slave5:9092", "slave6:9092"]
kafka = KafkaClient(brokerList)
producer = SimpleProducer(kafka, batch_send=True, batch_send_every_n=100, batch_send_every_t=5)

sflowToolProc = subprocess.Popen("sflowtool -l".split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

for line in sflowToolProc.stdout:
    fields = line.split(",")
    if fields[0] == "FLOW":
        sourceIP = fields[9]
        destinationIP = fields[10]
        protocol = fields[11]
        sourcePort = fields[14]
        destinationPort = fields[15]
        ipSize = fields[17]
        dateTime = int(time.time()*1000000)
        message = "{},{},{},{},{},{},{}".format(sourceIP, destinationIP, protocol, sourcePort, destinationPort, ipSize, dateTime)
        #print message
        producer.send_messages("netdata", message)
