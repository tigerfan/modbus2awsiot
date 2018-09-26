'''
本程序作为modbus tcp 客户端读取预定义的点表数据，发布到aws iot相应主题
数据变化时转发或者指定一个周期定时转发
引用了：
    https://github.com/owagner/modbus2mqtt
    https://github.com/ljean/modbus-tk/
    https://github.com/aws/aws-iot-device-sdk-python
by tigerfan
'''

import argparse
import logging
import logging.handlers
import time
import socket
import serial
import io
import sys
import csv
import signal
import json

import modbus_tk
import modbus_tk.defines as cst
from modbus_tk import modbus_rtu
from modbus_tk import modbus_tcp

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

version="0.1"
AllowedActions = ['both', 'publish', 'subscribe']
message = {}
loopCount = 0

# Custom MQTT message callback
def customCallback(client, userdata, message):
    print("Received a new message: ")
    print(message.payload)
    print("from topic: ")
    print(message.topic)
    print("--------------\n\n")

# Read in command-line parameters
parser = argparse.ArgumentParser(description='Bridge between ModBus and MQTT')
parser.add_argument("-e", "--endpoint", action="store", default="那啥.iot.cn-north-1.amazonaws.com.cn", dest="host", help="亚马逊云入口")
parser.add_argument("-r", "--rootCA", action="store", default="c:/awscert/root-CA.crt", dest="rootCAPath", help="根证书")
parser.add_argument("-c", "--cert", action="store", default="c:/awscert/cert.pem", dest="certificatePath", help="设备证书")
parser.add_argument("-k", "--key", action="store", default="c:/awscert/private.key", dest="privateKeyPath", help="设备私钥")
parser.add_argument("-p", "--port", action="store", dest="port", type=int, help="Port number override")
parser.add_argument("-w", "--websocket", action="store_true", dest="useWebsocket", default=False, help="Use MQTT over WebSocket")
parser.add_argument("-id", "--clientId", action="store", dest="clientId", default="modbus2awsiot", help="Targeted client id")
parser.add_argument("-t", "--topic", action="store", dest="topic", default="modbus/data/", help="Targeted topic")
parser.add_argument("-m", "--mode", action="store", dest="mode", default="publish", help="Operation modes: %s"%str(AllowedActions))
parser.add_argument("-M", "--message", action="store", dest="message", default="Hello World!", help="Message to publish")

parser.add_argument('--rtu', help='pyserial URL (or port name) for RTU serial port')
parser.add_argument('--rtu-baud', default='19200', type=int, help='Baud rate for serial port. Defaults to 19200')
parser.add_argument('--rtu-parity', default='even', choices=['even','odd','none'], help='Parity for serial port. Defaults to even')
parser.add_argument('--tcp', default="localhost", help='Act as a Modbus TCP master, connecting to host TCP')
parser.add_argument('--tcp-port', default='502', type=int, help='Port for Modbus TCP. Defaults to 502')
parser.add_argument('--registers', default="c:/awscert/register.csv", help='预定义点表')
parser.add_argument('--force', default='600',type=int, help='publish values after "force" seconds since publish regardless of change. 设为10分钟')

args = parser.parse_args()                    

host = args.host
rootCAPath = args.rootCAPath
certificatePath = args.certificatePath
privateKeyPath = args.privateKeyPath
port = args.port
useWebsocket = args.useWebsocket
clientId = args.clientId
topic = args.topic

if args.mode not in AllowedActions:
    parser.error("Unknown --mode option %s. Must be one of %s" % (args.mode, str(AllowedActions)))
    exit(2)

if args.useWebsocket and args.certificatePath and args.privateKeyPath:
    parser.error("X.509 cert authentication and WebSocket are mutual exclusive. Please pick one.")
    exit(2)

if not args.useWebsocket and (not args.certificatePath or not args.privateKeyPath):
    parser.error("Missing credentials for authentication.")
    exit(2)

# Port defaults
if args.useWebsocket and not args.port:  # When no port override for WebSocket, default to 443
    port = 443
if not args.useWebsocket and not args.port:  # When no port override for non-WebSocket, default to 8883
    port = 8883

# Configure logging
logger = logging.getLogger("AWSIoTPythonSDK.core")
logger.setLevel(logging.INFO)
streamHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

class Register:
    def __init__(self,topic,frequency,slaveid,functioncode,register,size,format):
        self.topic=topic
        self.frequency = int(frequency)
        self.slaveid = int(slaveid)
        self.functioncode = int(functioncode)
        self.register = int(register)
        self.size = int(size)
        self.format = format.split(":",2)
        self.next_due = 0
        self.lastval = None
        self.last = None

    def checkpoll(self):
        if self.next_due<time.time():
            self.poll()
            self.next_due=time.time()+self.frequency

    def poll(self):
        try:
            res = master.execute(self.slaveid,self.functioncode,self.register,self.size,data_format=self.format[0])
            r = res[0]
            if self.format[1]:
                r = self.format[1] % r
            if r != self.lastval or (args.force and (time.time() - self.last) > int(args.force)):
                self.lastval = r
                fulltopic = "$" + self.topic
                message['message'] = fulltopic + ":" + self.lastval
                message['sequence'] = loopCount

                if args.mode == 'both' or args.mode == 'publish':
                    messageJson = json.dumps(message)
                    myAWSIoTMQTTClient.publish(topic, messageJson, 1)

                    if args.mode == 'publish':
                        print('Published topic %s: %s\n' % (topic, messageJson))
                self.last = time.time()

        except modbus_tk.modbus.ModbusError as exc:
            logging.error("Error reading "+self.topic+": Slave returned %s - %s", exc, exc.get_exception_code())
        except Exception as exc:
            logging.error("Error reading "+self.topic+": %s", exc)

registers=[]

# Now lets read the register definition
with open(args.registers,"r") as csvfile:
    dialect = csv.Sniffer().sniff(csvfile.read(8192))
    csvfile.seek(0)
    defaultrow = {"Size":1,"Format":">H","Frequency":60,"Slave":1,"FunctionCode":4}
    reader = csv.DictReader(csvfile,fieldnames = ["Topic","Register","Size","Format","Frequency","Slave","FunctionCode"],dialect=dialect)
    for row in reader:
        # Skip header row
        if row["Frequency"] == "Frequency":
            continue
        # Comment?
        if row["Topic"][0] == "#":
            continue
        if row["Topic"] == "DEFAULT":
            temp = dict((k,v) for k,v in row.items() if v is not None and v!="")
            defaultrow.update(temp)
            continue
        freq = row["Frequency"]
        if freq is None or freq == "":
            freq = defaultrow["Frequency"]
        slave = row["Slave"]
        if slave is None or slave == "":
            slave = defaultrow["Slave"]
        fc=row["FunctionCode"]
        if fc is None or fc == "":
            fc=defaultrow["FunctionCode"]
        fmt = row["Format"]
        if fmt is None or fmt == "":
            fmt = defaultrow["Format"]
        size = row["Size"]
        if size is None or size == "":
            size = defaultrow["Size"]
        r = Register(row["Topic"],freq,slave,fc,row["Register"],size,fmt)
        registers.append(r)

logging.info('Read %u valid register definitions from \"%s\"' %(len(registers), args.registers))

# Init AWSIoTMQTTClient
myAWSIoTMQTTClient = None
if useWebsocket:
    myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId, useWebsocket=True)
    myAWSIoTMQTTClient.configureEndpoint(host, port)
    myAWSIoTMQTTClient.configureCredentials(rootCAPath)
else:
    myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId)
    myAWSIoTMQTTClient.configureEndpoint(host, port)
    myAWSIoTMQTTClient.configureCredentials(rootCAPath, privateKeyPath, certificatePath)

# AWSIoTMQTTClient connection configuration
myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
myAWSIoTMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec

# Connect and subscribe to AWS IoT
myAWSIoTMQTTClient.connect()
if args.mode == 'both' or args.mode == 'subscribe':
    myAWSIoTMQTTClient.subscribe(topic, 1, customCallback)
time.sleep(2)

# Publish to the same topic in a loop forever
if args.rtu:
    master = modbus_rtu.RtuMaster(serial.serial_for_url(args.rtu,baudrate = args.rtu_baud,parity = args.rtu_parity[0].upper()))
elif args.tcp:
    master = modbus_tcp.TcpMaster(args.tcp,args.tcp_port)
else:
    logging.error("You must specify a modbus access method, either --rtu or --tcp")
    sys.exit(1)

master.set_verbose(True)
master.set_timeout(5.0)
    
while True:
    for r in registers:
        r.checkpoll()
        loopCount += 1
    time.sleep(10)
