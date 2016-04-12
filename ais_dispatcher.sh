#!/usr/bin/env python
#-*- coding: UTF-8 -*-

# autor: Carlos Rueda
# date: 2015-12-21
# mail: carlos.rueda@deimos-space.com
# version: 1.0

########################################################################
# version 1.0 release notes:
# Initial version
########################################################################

from __future__ import division
import time
import datetime
import os
import sys
import utm
import SocketServer, socket
import logging, logging.handlers
import json
import httplib2
from threading import Thread
import pika
import MySQLdb


########################################################################
# configuracion y variables globales
from configobj import ConfigObj
config = ConfigObj('./ais_dispatcher.properties')

LOG = config['directory_logs'] + "/ais_dispatcher.log"
LOG_FOR_ROTATE = 10

DB_IP = "localhost"
DB_NAME = "sumo"
DB_USER = "root"
DB_PASSWORD = "dat1234"

RABBITMQ_HOST = config['rabbitMQ_HOST']
RABBITMQ_PORT = config['rabbitMQ_PORT']
RABBITMQ_ADMIN_USERNAME = config['rabbitMQ_admin_username']
RABBITMQ_ADMIN_PASSWORD = config['rabbitMQ_admin_password']
QUEUE_NAME = config['queue_name']

KCS_HOST = config['KCS_HOST']
KCS_PORT = config['KCS_PORT']

DEFAULT_SLEEP_TIME = float(config['sleep_time'])

PID = "/var/run/ais_dispatcher"
########################################################################

# Se definen los logs internos que usaremos para comprobar errores
try:
    logger = logging.getLogger('ais_dispatcher')
    loggerHandler = logging.handlers.TimedRotatingFileHandler(LOG, 'midnight', 1, backupCount=10)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    loggerHandler.setFormatter(formatter)
    logger.addHandler(loggerHandler)
    logger.setLevel(logging.DEBUG)
except:
    print '------------------------------------------------------------------'
    print '[ERROR] Error writing log at %s' % LOG
    print '[ERROR] Please verify path folder exits and write permissions'
    print '------------------------------------------------------------------'
    exit()

########################################################################

if os.access(os.path.expanduser(PID), os.F_OK):
        print "Checking if ais_dispatcher process is already running..."
        pidfile = open(os.path.expanduser(PID), "r")
        pidfile.seek(0)
        old_pd = pidfile.readline()
        # process PID
        if os.path.exists("/proc/%s" % old_pd) and old_pd!="":
            print "You already have an instance of the ais_dispatcher process running"
            print "It is running as process %s" % old_pd
            sys.exit(1)
        else:
            print "Trying to start ais_dispatcher process..."
            os.remove(os.path.expanduser(PID))

#This is part of code where we put a PID file in the lock file
pidfile = open(os.path.expanduser(PID), 'a')
print "ais_dispatcher process started with PID: %s" % os.getpid()
pidfile.write(str(os.getpid()))
pidfile.close()

########################################################################

#socketKCS = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#connectedKCS = False
#connectedQUEUE = False
#connectionRetry = 5

def checkBoat(vehicleLicense):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)
	try:
		cursor = dbConnection.cursor()
		cursor.execute(""" SELECT DEVICE_ID from VEHICLE where VEHICLE_LICENSE= '%s' limit 0,1""" % (vehicleLicense,))
		result = cursor.fetchall()
		if len(result)==1 :
			return result[0][0]
		else :
			return '0'
		cursor.close
		dbConnection.close
	except Exception, error:
		logger.error('Error executing query: %s', error)

def addBoat(vehicleLicense):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)
	try:
		query = """INSERT INTO VEHICLE (VEHICLE_LICENSE,BASTIDOR,ALIAS,POWER_SWITCH,ALARM_STATE,SPEAKER,START_STATE,WARNER,PRIVATE_MODE,WORKING_SCHEDULE,ALARM_ACTIVATED,PASSWORD,CELL_ID,ICON_DEVICE, KIND_DEVICE,AIS_TYPE,MAX_SPEED,CONSUMPTION,CLAXON,MODEL_TRANSPORT,PROTOCOL_ID,BUILT,CALLSIGN,MAX_PERSONS,MOB,EXCLUSION_ZONE,FLAG,INITIAL_DATE_PURCHASE) VALUES (xxx,'',xxx,-1,-1,-1,'UNKNOWN',-1,0,0,0,'',0,1000,1,3,500,0.0,-1,'boat',0,0,xxx,-1,-1,0,'',NOW())"""
		QUERY = query.replace('xxx', vehicleLicense)
		cursor = dbConnection.cursor()
		cursor.execute(QUERY)
		dbConnection.commit()
		logger.info('Boat %s added at database', vehicleLicense)
		cursor.close
		cursor = dbConnection.cursor()
		cursor.execute("""SELECT LAST_INSERT_ID()""")
		result = cursor.fetchall()
		cursor.close
		dbConnection.close()
		logger.info('Boat added with DEVICE_ID: %s', result[0][0])
        	return result[0][0]
	except Exception, error:
		logger.error('Error executing query : %s', error)

def addComplementary(vehicleLicense, deviceID):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)
	try:
		DEVICE_ID = str(deviceID)
		query = """INSERT INTO OBT (IMEI, VEHICLE_LICENSE, DEVICE_ID, VERSION_ID, ALARM_RATE,COMS_MODULE,CONFIGURATION_ID,CONNECTED,MAX_INVALID_TRACKING_SPEED,PRIORITY,REPLICATED_SERVER_ID,GSM_OPERATOR_ID,ID_CARTOGRAPHY_LAYER,ID_TIME_ZONE,INIT_CONFIG,STAND_BY_RATE,HOST,LOGGER,TYPE_SPECIAL_OBT) VALUES (xxx,xxx,yyy,'11','','127.0.0.1',0,0,500,0,0,0,0,1,'','','',0,12)"""
		queryOBT = query.replace('xxx', vehicleLicense).replace('yyy', DEVICE_ID)
		query = """INSERT INTO HAS (FLEET_ID,VEHICLE_LICENSE,DEVICE_ID) VALUES (533,xxx,yyy)"""
		queryHAS = query.replace('xxx', vehicleLicense).replace('yyy', DEVICE_ID)
		cursor = dbConnection.cursor()
		cursor.execute(queryOBT)
		dbConnection.commit()
		logger.info('OBT info saved at database for deviceID %s', deviceID)
		cursor.execute(queryHAS)
		dbConnection.commit()
		logger.info('HAS info saved at database for deviceID %s', deviceID)
		cursor.close
		dbConnection.close()
	except Exception, error:
		logger.error('Error executing query: %s', error)
		
		
	

def getvehicleLicense(body):
	try:
		vehicleLicense = body.split(',')[0]
		return vehicleLicense
	except Exception, error:
		logger.error('error parsing message: %s' % error)	


def callback(ch, method, properties, body):
    logger.info("Message read from QUEUE: %s" % body)
    #antes de enviar al KCS comprobamos si existe el barco en BD
    vehicleLicense = getvehicleLicense(body)
    logger.info('Checking if boat %s is at database...', vehicleLicense)
    
    if (checkBoat(vehicleLicense) == '0'):
		logger.info('Boat is not at database.')
		# creamos el dispositivo
		deviceID = addBoat(vehicleLicense)
		logger.info('Boat saved at database with DEVICE_ID %s', deviceID)
		addComplementary(vehicleLicense, deviceID)
		#time.sleep(0.1)
    else:
		logger.info('Boat %s found at database', vehicleLicense)
		
    try:
	socketKCS = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	socketKCS.connect((KCS_HOST, int(KCS_PORT)))
        socketKCS.send(body + '\r\n')
        logger.info ("Sent to KCS: %s " % body)
	ch.basic_ack(delivery_tag = method.delivery_tag)
	socketKCS.close()
        time.sleep(DEFAULT_SLEEP_TIME)
    except Exception, error:
        logger.error('Error sending data: %s', error)
        try:
			connectedKCS = False
			socketKCS.close()
	except:
		pass
	while connectedKCS==False:
		try:
			logger.info('Trying reconnection to KCS...')
			socketKCS = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			socketKCS.connect((KCS_HOST, int(KCS_PORT)))
			socketKCS.send(body + '\r\n')
			logger.info ("Sent to KCS: %s " % body)
			ch.basic_ack(delivery_tag = method.delivery_tag)
			connectedKCS = True
			socketKCS.close()
		except Exception, error:
			logger.info('Reconnection to KCS failed....waiting %d seconds to retry.' , connectionRetry)
			try:
				socketKCS.close()
			except:
				pass
			time.sleep(connectionRetry)
	#time.sleep(DEFAULT_SLEEP_TIME)

	
try:
	rabbitMQconnection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
	channel = rabbitMQconnection.channel()
	channel.basic_consume(callback, queue=QUEUE_NAME)
	connectedQUEUE = True
	logger.info('Connected at QUEUE!!!')
	channel.start_consuming()
except Exception, error:
	connectedQUEUE = False
	logger.error('Problem with RabbitMQ connection: %s' % error)
	while connectedQUEUE == False:
		try:
			rabbitMQconnection.close()
		except:
			pass
		try:
			logger.info('Trying reconnection to QUEUE...')
			rabbitMQconnection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
			channel = rabbitMQconnection.channel()
			channel.basic_consume(callback, queue=QUEUE_NAME)
			connectedQUEUE = True
			logger.info('Connected at QUEUE again!!!')
			channel.start_consuming()
		except Exception, error:
			logger.info('Reconnection to QUEUE failed....waiting %d seconds to retry.' , connectionRetry)
			time.sleep(connectionRetry)
			
