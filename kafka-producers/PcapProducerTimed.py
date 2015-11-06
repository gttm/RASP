#!/usr/bin/python
import gzip
import time
import calendar
import random
import sys
from kafka import SimpleProducer, KafkaClient

topic = "netdata"
rate = 8000
if len(sys.argv) == 3:
    topic = str(sys.argv[1])
    rate = int(sys.argv[2])

brokerList = ["worker1:9092", "worker2:9092", "worker3:9092", "worker4:9092"]
kafka = KafkaClient(brokerList)
producer = SimpleProducer(kafka, async=True, batch_send_every_n=100, batch_send_every_t=5)

counter = 1
for day in range(1, 2):
    for hour in range(0, 24):
        filename = 'grix/sflow.2014-02-{}_{}.pcap.gz'.format(u'%02d' % day, u'%02d' % hour)
        print filename
        microsec = 0
        for line in gzip.open(filename, 'rb'):
            fields = line.split()
            sourceIp = fields[0]
            destinationIp = fields[2]
            if ":" in sourceIp + destinationIp: continue
            protocol = fields[4]
            sourcePort = fields[5]
            destinationPort = fields[6]
            ipSize = fields[7]
            date = fields[8]
            #dateTime = calendar.timegm(time.strptime(date, "%Y-%m-%d"))*1000000 + hour*3600000000 + microsec
            dateTime = int(time.time()*1000000)
            message = "{},{},{},{},{},{},{}".format(sourceIp, destinationIp, protocol, sourcePort, destinationPort, ipSize, dateTime)
            #print message
            producer.send_messages(topic, message)
            counter += 1
            microsec += 1
            if counter%rate == 0: time.sleep(1)

