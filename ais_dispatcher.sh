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
from datetime import datetime, timedelta
from pytz import timezone
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
DB_USER = "user_dispatcher"
DB_PASSWORD = "dat1234"

SPATIAL_DB_IP = "192.168.27.5"
SPATIAL_DB_NAME = "sumo"
SPATIAL_DB_USER = "root"
SPATIAL_DB_PASSWORD = "dat1234"

RABBITMQ_HOST = config['rabbitMQ_HOST']
RABBITMQ_PORT = config['rabbitMQ_PORT']
RABBITMQ_ADMIN_USERNAME = config['rabbitMQ_admin_username']
RABBITMQ_ADMIN_PASSWORD = config['rabbitMQ_admin_password']
QUEUE_NAME = config['queue_name']

KCS_HOST = config['KCS_HOST']
KCS_PORT = config['KCS_PORT']

MAX_LAT = config['max_lat']
MIN_LAT = config['min_lat']
MAX_LON = config['max_lon']
MIN_LON = config['min_lon']

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
connectionRetry = 0.5

def checkBoat(vehicleLicense):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
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
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getSpecialField(body):
	try:
		field = body.split(',')[1]
		return field
	except Exception, error:
		logger.error('error parsing message: %s' % error)	

def getLongitude(body):
	try:
		lon = body.split(',')[2]
		return lon
	except Exception, error:
		logger.error('error parsing message: %s' % error)	

def getLatitude(body):
	try:
		lat = body.split(',')[3]
		return lat
	except Exception, error:
		logger.error('error parsing message: %s' % error)	

def setVesselInOut(vehicleLicense, inout):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
		    query = """UPDATE VEHICLE SET VEHICLE.INOUT=xxx WHERE VEHICLE_LICENSE='vvv'"""
		    queryINOUT = query.replace('vvv', str(vehicleLicense)).replace('xxx', str(inout))
		    cursor = dbConnection.cursor()
		    cursor.execute(queryINOUT)
		    dbConnection.commit()
		    logger.info('Inout of boat %s modified to %s', vehicleLicense, inout)
		    cursor.close
		except Exception, error:
		    logger.error('Error executing query : %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def addBoat(vehicleLicense):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
		    query = """INSERT INTO VEHICLE (VEHICLE_LICENSE,BASTIDOR,ALIAS,POWER_SWITCH,ALARM_STATE,SPEAKER,START_STATE,WARNER,PRIVATE_MODE,WORKING_SCHEDULE,ALARM_ACTIVATED,PASSWORD,CELL_ID,ICON_DEVICE, KIND_DEVICE,AIS_TYPE,MAX_SPEED,CONSUMPTION,CLAXON,MODEL_TRANSPORT,PROTOCOL_ID,BUILT,CALLSIGN,MAX_PERSONS,MOB,EXCLUSION_ZONE,FLAG,INITIAL_DATE_PURCHASE) VALUES (xxx,xxx,xxx,-1,-1,-1,'UNKNOWN',-1,0,0,0,'',0,1000,1,3,500,0.0,-1,'boat',0,0,xxx,-1,-1,50,'',NOW())"""
		    QUERY = query.replace('xxx', str(vehicleLicense))
		    cursor = dbConnection.cursor()
		    cursor.execute(QUERY)
		    dbConnection.commit()
		    logger.info('Boat %s added at database', vehicleLicense)
		    cursor.close
		    cursor = dbConnection.cursor()
		    cursor.execute("""SELECT LAST_INSERT_ID()""")
		    result = cursor.fetchall()
		    return result[0][0]
		    cursor.close
		    dbConnection.close()
		    logger.info('Boat added with DEVICE_ID: %s', result[0][0])
		except Exception, error:
		    logger.error('Error executing query : %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def updateBoat(vehicleLicense,aisType,iconVessel,nameVessel,callsign,imo=""):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
		    query = "UPDATE VEHICLE SET AIS_TYPE=" + aisType + ",ICON_DEVICE=" + iconVessel + ",ALIAS='" + nameVessel + "', BASTIDOR='" + nameVessel + "', CALLSIGN='" + callsign + "',IMO='" + imo + "' WHERE VEHICLE_LICENSE='" + vehicleLicense +"'";
		    cursor = dbConnection.cursor()
		    cursor.execute(query)
		    dbConnection.commit()
		    logger.info('Boat %s modified at database', vehicleLicense)
		    cursor.close
		    dbConnection.close()
		except Exception, error:
		    logger.error('Error executing query : %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def addComplementary(vehicleLicense, deviceID):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
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
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def saveSpatialPoint(VEHICLE_LICENSE, LON, LAT):
	try:
		dbConnection = MySQLdb.connect(SPATIAL_DB_IP, SPATIAL_DB_USER, SPATIAL_DB_PASSWORD, SPATIAL_DB_NAME)
		try:
		    query = "INSERT INTO TRACKING_SPATIAL (VEHICLE_LICENSE, PT) values (%s, POINT(%s, %s)) ON DUPLICATE KEY UPDATE PT=POINT(%s,%s)" % (VEHICLE_LICENSE, LON, LAT, LON, LAT)
		    cursor = dbConnection.cursor()
		    cursor.execute(query)
		    dbConnection.commit()
		    logger.info('Saved spatial point for vehicle %s', VEHICLE_LICENSE)
		    cursor.close
		    dbConnection.close()
		except Exception, error:
		    logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', SPATIAL_DB_IP, SPATIAL_DB_USER, SPATIAL_DB_PASSWORD, SPATIAL_DB_NAME, error)

def getBoatCloser(LON,LAT, ratio):
	try:
		dbConnection = MySQLdb.connect(SPATIAL_DB_IP, SPATIAL_DB_USER, SPATIAL_DB_PASSWORD, SPATIAL_DB_NAME)
		try:
		    query = "SELECT VEHICLE_LICENSE, ST_DISTANCE_SPHERE(TRACKING_SPATIAL.PT, POINT(%s, %s)) from TRACKING_SPATIAL where ST_DISTANCE_SPHERE(TRACKING_SPATIAL.PT, POINT(%s, %s)) < %s" % (LON, LAT, LON, LAT, ratio)
		    cursor = dbConnection.cursor()
		    cursor.execute(query)
		    result = cursor.fetchall()
		    if len(result)>1 :
			    return result
		    else :
			    return 0
		    cursor.close
		    dbConnection.close
		except Exception, error:
		    logger.error('Error executing query: %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', SPATIAL_DB_IP, SPATIAL_DB_USER, SPATIAL_DB_PASSWORD, SPATIAL_DB_NAME, error)
		
def getvehicleLicense(body):
	try:
		vehicleLicense = body.split(',')[0]
		return vehicleLicense
	except Exception, error:
		logger.error('error parsing message: %s' % error)	

def recordEvent(vehicle1_device, vehicle1_alias, vehicle2_device, vehicle2_alias):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
		    cursor = dbConnection.cursor()
		    #consultar urls a enviar
		    query = """SELECT URL FROM SUBSCRIBER where ID in (SELECT SUBSCRIBER_ID FROM SUBSCRIPTION where EVENT_TYPE=27)"""
		    cursor.execute(query)
		    cursorEvent = dbConnection.cursor()
		    row = cursor.fetchall()
		    while row is not None:
		    	now_utc = datetime.now(timezone('UTC'))
		    	hora = timedelta(hours=1)
		    	future_utc = now_utc + hora
		    	timestamp = now_utc.strftime("%Y-%m-%dT%H:%M:%S.00Z")
		    	expirationTime = future_utc.strftime("%Y-%m-%dT%H:%M:%S.00Z")
		    	data = '{"eventType":27,"resourceId":"' + str(vehicle1_device) + '","resource2Id":"' + str(vehicle2_device) + '","resourceName":"' + str(vehicle1_alias) + '","resource2Name":"' + str(vehicle2_alias) + '","timestamp":"' + timestamp + '","expirationTime":"' + expirationTime + '","source":"KYROS"}'
		    	url = row[0][0]
		    	# Insertar evento
		    	initDate = long(datetime.utcnow().strftime('%s'))*1000
		    	endDate = long(datetime.utcnow().strftime('%s'))*1000 + 3600000
		    	queryNewEvent = "INSERT INTO SUMO_PENDING_EVENT (EVENT_JSON_DATA, URL, EVENT_DATE, LIMIT_DATE, SENT) VALUES ('" + str(data) + "','" + str(url) + "'," + str(initDate) + "," + str(endDate) + ",0)"
		    	logger.info('New event clash: %s', data)
		    	cursorEvent.execute(queryNewEvent)
		    	row = cursor.fetchone()
		    cursorEvent.close
		    cursor.close
		    dbConnection.commit()		    
		    dbConnection.close()
		except Exception, error:
		    logger.error('Error executing query : %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def newLogAlarm(vehicle1_license, vehicle1_alias, vehicle2_license, vehicle2_alias, distance):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
		    cursor = dbConnection.cursor()
		    #corregir el alias
		    if (vehicle1_alias=='AIS vessel'):
			    vehicle1_alias = vehicle1_license
		    if (vehicle2_alias=='AIS vessel'):
			    vehicle2_alias = vehicle2_license
		    msg = "Posibble clash of vessels: " + vehicle1_alias + " (" + vehicle1_license + ")" + " - " + vehicle2_alias + " (" + vehicle2_license + "). Distance=" + str(round(distance,1)) + " m."
		    msg_oposite = "Posibble clash of vessels: " + vehicle2_alias + " (" + vehicle2_license + ")" + " - " + vehicle1_alias + " (" + vehicle1_license + "). Distance=" + str(round(distance,1)) + " m."
		    query_delete_oposite = "DELETE FROM LOGS where MESSAGE='" + msg_oposite + "'"
		    cursor.execute(query_delete_oposite)
		    query = """SELECT ID FROM LOGS WHERE MESSAGE='mmm'"""
		    queryCheckLog = query.replace('mmm', str(msg))
		    cursor.execute(queryCheckLog)
		    result = cursor.fetchall()
		    queryNewLog = "INSERT INTO LOGS (MESSAGE,FINISHED,LOG_TYPE,LEVEL,LOG_DATE) VALUES('" + msg + "',0,1,2," + str(time.time()*1000) + ")"
		    if len(result)>0 :
			    # Existe el log asi que solo actualizo la fecha
			    queryNewLog = "UPDATE LOGS SET LOG_DATE='" + str(time.time()*1000) + "' WHERE ID=" + str(result[0][0])
		    #print queryNewLog
		    cursor.execute(queryNewLog)
		    cursor.close
		    dbConnection.commit()
		    logger.info('New log alarm: %s', msg)
		    dbConnection.close()
		except Exception, error:
		    logger.error('Error executing query : %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)


def getMaxRadius():
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
		    query = """SELECT MAX(EXCLUSION_ZONE) FROM VEHICLE"""
		    cursor = dbConnection.cursor()
		    cursor.execute(query)
		    result = cursor.fetchall()
		    return result[0][0]
		    cursor.close
		    dbConnection.close()
		except Exception, error:
		    logger.error('Error executing query : %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def getBoatData(vehicleLicense):
	try:
		dbConnection = MySQLdb.connect(DB_IP, DB_USER, DB_PASSWORD, DB_NAME)
		try:
		    query = "SELECT EXCLUSION_ZONE, ALIAS, DEVICE_ID FROM VEHICLE WHERE VEHICLE_LICENSE='"+vehicleLicense+"'"
		    cursor = dbConnection.cursor()
		    cursor.execute(query)
		    result = cursor.fetchall()
		    return result[0]
		    cursor.close
		    dbConnection.close()
		except Exception, error:
		    logger.error('Error executing query : %s', error)
	except Exception, error:
		logger.error('Error connecting to database: IP:%s, USER:%s, PASSWORD:%s, DB:%s: %s', DB_IP, DB_USER, DB_PASSWORD, DB_NAME, error)

def callback(ch, method, properties, body):
    #logger.info("ENVIANDO ACK A LA COLA")
    ch.basic_ack(delivery_tag = method.delivery_tag)
    sendMessage = False
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

    lon = getLongitude(body)
    lat = getLatitude(body)

    if (getSpecialField(body)=='$'):
		#print "TRAMA ESPECIAL"
		vtrama = body.split(',')
		try:
			#print "actualizar vessel"
			updateBoat(vtrama[0],vtrama[2],vtrama[3],vtrama[4],vtrama[5],vtrama[6])
		except e:
			logger.error('Excepcion al llamar a updateBoat con vtrama:' + vtrama)
    else:
    	#insertar en la bbdd espacial
    	saveSpatialPoint(vehicleLicense,lon,lat)

    	#comprobar si esta dentro de la zona de windfarm
    	if (lon<MIN_LON or lon>MAX_LON or lat>MAX_LAT or lat<MIN_LAT):
			setVesselInOut(vehicleLicense, 0)
    	else:
			setVesselInOut(vehicleLicense, 1)
			maxradius = getMaxRadius()
			boatData = getBoatData(vehicleLicense)
			boatRadius = boatData[0]
			boatAlias = boatData[1]
			boatDevice = boatData[2]
			boatNearby = getBoatCloser(lon,lat, maxradius)
			if (boatNearby!=0):
				for boat in boatNearby:
					newVehicleLicense = boat[0]
					distance = boat[1]
					if (int(distance) < boatRadius and vehicleLicense!=newVehicleLicense):
						newBoatData = getBoatData(newVehicleLicense)
						newBoatAlias = newBoatData[1]
						newBoatDevice = newBoatData[2]
						logger.info("Posibble clash of vessels: " + vehicleLicense + " - " + newVehicleLicense)
						#newLogAlarm(vehicleLicense, boatAlias, newVehicleLicense, newBoatAlias, distance)
						recordEvent(boatDevice, boatAlias, newBoatDevice, newBoatAlias)

	try:
		socketKCS = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		socketKCS.connect((KCS_HOST, int(KCS_PORT)))
		connectedKCS = True
		socketKCS.send(body + '\r\n')
		logger.info ("Sent to KCS: %s " % body)
		sendMessage = True
		#ch.basic_ack(delivery_tag = method.delivery_tag)
		socketKCS.close()
		time.sleep(DEFAULT_SLEEP_TIME)
	except socket.error,v:
		#print v[0]
		logger.error('Error sending data: %s', v[0])
		try:
			socketKCS.close()
			logger.info('Trying close connection...')
		except Exception, error:
			logger.info('Error closing connection: %s', error)
			pass
			while sendMessage==False:
				try:
					logger.info('Trying reconnection to KCS...')
					socketKCS = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					socketKCS.connect((KCS_HOST, int(KCS_PORT)))
					connectedKCS = True
					socketKCS.send(body + '\r\n')
					logger.info ("Sent to KCS: %s " % body)
					sendMessage = True
					socketKCS.close()
				except Exception, error:
					logger.info('Reconnection to KCS failed....waiting %d seconds to retry.' , connectionRetry)
					sendMessage=False
					try:
						socketKCS.close()
					except:
						pass
					time.sleep(connectionRetry)

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
			connectedQUEUE = False
			logger.info('Reconnection to QUEUE failed....waiting %d seconds to retry.' , connectionRetry)
			time.sleep(connectionRetry)
			
