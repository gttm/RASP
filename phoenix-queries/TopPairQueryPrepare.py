#!/usr/bin/python
import sys
import os
import time

def usage():
    print "Usage: {} pair_type minutes_since_window_start window_minutes".format(sys.argv[0])

def createFileWithContent(filename, content):
    fo = open(filename, "w+")
    fo.write(content)
    fo.close()

if len(sys.argv) != 4:
    usage()
    sys.exit()

pairType = sys.argv[1].upper()
windowStart= sys.argv[2]
window = sys.argv[3]

if pairType not in ["AS", "DNS"] or not windowStart.isdigit() or not window.isdigit():
    usage()
    sys.exit()

if pairType == "AS":
    columns = ["\"as\".\"asS\"", "\"as\".\"asD\""]
elif pairType == "DNS":
    columns = ["\"dns\".\"dnsS\"", "\"dns\".\"dnsD\""]

currentTime = int(time.time())
startTime = currentTime - int(windowStart)*60
stopTime = startTime + int(window)*60
print "Window: {} - {}".format(time.ctime(startTime), time.ctime(stopTime))

startTime = startTime*1000000
stopTime = stopTime*1000000

statement = ("SELECT {}, {}, COUNT(1) AS \"pairCount\"\n".format(columns[0], columns[1]) + 
            "FROM \"netdata\"\n" +
            "WHERE \"time\" > {} and \"time\" < {}\n".format(startTime, stopTime) +
            "GROUP BY {}, {}\n".format(columns[0], columns[1]) +
            "ORDER BY \"pairCount\" DESC\n" +
            "LIMIT 10;\n")

queryFile = "Top{}Pairs.sql".format(pairType)

createFileWithContent(queryFile, statement)
print "Query file: {}".format(queryFile)

