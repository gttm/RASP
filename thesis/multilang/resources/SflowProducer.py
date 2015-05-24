#!/usr/bin/python
import sys
import time
import subprocess
from kafka import SimpleProducer, KafkaClient

def ipToInt(ipString):
    ipOctets = ipString.split(".")
    return int(ipOctets[0])*16777216 + int(ipOctets[1])*65536 + int(ipOctets[2])*256 + int(ipOctets[3])

kafka = KafkaClient("master:9092")
producer = SimpleProducer(kafka, batch_send=True, batch_send_every_n=20, batch_send_every_t=5)

sflowToolProc = subprocess.Popen("sflowtool -l".split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

for line in sflowToolProc.stdout:
    fields = line.split(",")
    if fields[0] == "FLOW":
        sourceIp = fields[9]
        sourceIpInt = ipToInt(sourceIp)
        destinationIp = fields[10]
        destinationIpInt = ipToInt(destinationIp)
        protocol = fields[11]
        sourcePort = fields[14]
        destinationPort = fields[15]
        ipLength = fields[17]
        date = int(time.time())
        message = "{} {} {} {} {} {} {} {} {}".format(sourceIp, sourceIpInt, destinationIp, destinationIpInt, protocol, sourcePort, destinationPort, ipLength, date)
        #print message
        producer.send_messages("netdata", message)
