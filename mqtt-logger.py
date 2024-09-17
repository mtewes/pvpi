"""
Lightweight module / script to log mqtt topics to an sqlite3 database, and write this to csv once per day.
All datetimes are in UTC.

Idea:
- listen to some mqtt topics
- for each topic, the latest value is kept in a memory-buffer (dict)
- when a particular topic is recieved (log_trigger_topic), we log the entire memory-buffer to an sqlite db (all columns with one single datetime)
- if a topic from the memory buffer is "old" (e.g., older than 30 seconds), it gets written as nan (and a warning is shown)
- every new day, the previous day gets exported as csv, and only the last 7 days are kept in the sqlite db (to be used for plots, for example)


"""

import sqlite3
import csv
from datetime import datetime, timedelta, timezone
import os

import paho.mqtt.client as mqtt

import logging
logger = logging.getLogger(__name__)


# MQTT payloads are just sequences of bytes 
# For convenience, we "decode" these when writing into the db (and the exported files), following these rules:
log_topic_types = {
    "SMAHomeManager/psupply":"float",
    "SMAHomeManager/ppurchase":"float",
    "SMAHomeManager/esupply":"float",
    "SMAHomeManager/epurchase":"float",
    "SMAHomeManager/v1":"float",
    "SMAHomeManager/v2":"float",
    "SMAHomeManager/v3":"float",
    "SMATripower/psupply":"int",
    "SMATripower/ppurchase":"int",
    "SMATripower/pgenerate":"int",
    "SMATripower/esupply":"float",
    "SMATripower/epurchase":"float",
    "SMATripower/pconsume":"int",
    "VitocalOpen3E/DomesticHotWaterSensor/Actual":"float",
    "VitocalOpen3E/OutsideTemperatureSensor/Actual":"float",
    "VitocalOpen3E/FlowTemperatureSensor/Actual":"float",
    "VitocalOpen3E/ReturnTemperatureSensor/Actual":"float",
    "VitocalOpen3E/WaterPressureSensor/Actual":"float",
    "VitocalOpen3E/CentralHeatingPump":"float",
    "VitocalOpen3E/SmartGridReadyConsolidator/OperatingStatus":"float",
    "VitocalOpen3E/EnergyConsumptionCentralHeating/Today":"float",
    "VitocalOpen3E/EnergyConsumptionCentralHeating/CurrentYear":"float",
    "VitocalOpen3E/EnergyConsumptionDomesticHotWater/Today":"float",
    "VitocalOpen3E/EnergyConsumptionDomesticHotWater/CurrentYear":"float",
    "VitocalOpen3E/AllengraSensor/Actual":"float",
    "VitocalOpen3E/AllengraSensor/Temperature":"float",
    "VitocalOpen3E/ThermalPower":"float",
    "VitocalOpen3E/HeatPumpCompressor":"float",
    "VitocalOpen3E/AdditionalElectricHeater":"float",
    "VitocalOpen3E/HeatPumpCompressorStatistical/starts":"int",
    "VitocalOpen3E/HeatPumpCompressorStatistical/hours":"float",
    "VitocalOpen3E/CurrentElectricalPowerConsumptionRefrigerantCircuit":"float",
    "VitocalOpen3E/CurrentElectricalPowerConsumptionElectricHeater":"float",
    "VitocalOpen3E/CurrentElectricalPowerConsumptionSystem":"float",
    "VitocalOpen3E/CurrentThermalCapacitySystem":"float",
    "VitocalOpen3E/FourThreeWayValveValveCurrentPosition":"int"
    }

log_topics = [key for (key, value) in log_topic_types.items()]

log_trigger_topic = "VitocalOpen3E/CurrentElectricalPowerConsumptionSystem"

def translate_topic_mqtt_to_db(topic_name):
    return topic_name.replace("/", "_")

log_topics_db = [translate_topic_mqtt_to_db(topic) for topic in log_topics]

def now():
    return datetime.now(timezone.utc)

def now_ymd():
    tmp = now()
    return (tmp.year, tmp.month, tmp.day)

class LogDB:
    def __init__(self, name="test", path=None, export_workdir=None, cols=None):
        """

        path can be ":memory:" to have the sqlite3 db in memory.

        export_workdir is where the csv files get written once per day.
        
        cols is a list of column names to store the values.
        """
        self.name = name

        self.path = path
        if path is None:
            self.path = "{}.db".format(name)

        self.export_workdir = export_workdir
        if export_workdir is None:
            self.export_workdir = "."

        self.cols = cols
        #if cols is None:
        #    self.cols = ["temp", "hum"]

        self.next_ymd_to_export = now_ymd()

        self.create()
    
    def __str__(self):
        return "Table {} at {} with cols {}".format(self.name, self.path, self.cols)

    def create(self):

        self.con = sqlite3.connect(self.path)  #, check_same_thread=False)
        self.cur = self.con.cursor()
    
        if self.cols is not None:
            cmd = "CREATE TABLE IF NOT EXISTS {}(datetime, {})".format(self.name, ",".join(self.cols))
            self.cur.execute(cmd)
            self.con.commit()
    
        #self.con.close()
        logger.info("Connected to table {}".format(str(self)))


    def log(self, d):
        """
        insert dict d data with datetime and commit.
        Only 
        """
        #con = sqlite3.connect(self.path)
        #cur = con.cursor()
        if self.cols is None:
            raise RuntimeError("Cannot log without specifying cols!")
            
        placeholder = ", ".join(["?" for c in self.cols])
        cmd = "INSERT INTO {} values(datetime('now'), {})".format(self.name, placeholder)
        self.cur.execute(cmd, [d[k] for k in self.cols])
        self.con.commit()
        #con.close()

    def print(self):
        #self.con = sqlite3.connect(self.path)
        #self.cur = con.cursor()
        for row in self.cur.execute("SELECT * FROM {}".format(self.name)):
            print(row)
        #con.close()

    def ping_export(self):
        """
        Fast and often triggered function, checking if the export needs to happen,
        and calling the export if needed.
        """

        yesterday = now() - timedelta(days=1)
        yesterday_ymd = (yesterday.year, yesterday.month, yesterday.day)

        if yesterday_ymd == self.next_ymd_to_export:

            logger.info(f"Triggering export for {yesterday_ymd}")
            self.export_last_day()
            logger.info("Triggering deletion of old data from db...")
            self.delete_old()

            self.next_ymd_to_export = now_ymd()


    def export_last_day(self, testmode=False):
        """
        The idea is that this gets triggered early in the morning of some day, and exports
        the "full" yesterday to a csv file.

        export, close, reopen, clean old inputs ?
        """

        # Date of yesterday for filename:
        yesterday = now() - timedelta(days=1)
        
        dbdirname = self.name
        yeardirname = yesterday.strftime('%Y')
        
        dbdir = os.path.join(self.export_workdir, dbdirname, yeardirname)
        os.makedirs(dbdir, exist_ok=True)
        
        filename = yesterday.strftime('%Y-%m-%d') + ".csv"
        filepath = os.path.join(dbdir, filename)
        logger.info("Exporting {} to {}...".format(self.name, filepath))

        cmd = """SELECT * FROM {} WHERE 
        datetime > DATETIME('NOW', 'start of day', '-1 day') 
        and 
        datetime < DATETIME('NOW', 'start of day') 
        ORDER BY datetime""".format(self.name)

        if testmode:
            cmd = """SELECT * FROM {} WHERE 
            datetime > DATETIME('NOW', 'start of day', '-1 day')  
            ORDER BY datetime""".format(self.name)

        self.cur.execute(cmd)

        with open(filepath, 'w') as csv_file: # We intentionally overwrite, as we might have new data
            csv_writer = csv.writer(csv_file, delimiter="\t")
            csv_writer.writerow([i[0] for i in self.cur.description]) 
            csv_writer.writerows(self.cur)

        # No longer used:
        self.last_export_ymd = (yesterday.year, yesterday.month, yesterday.day)


    def delete_old(self):

        logger.info("Deleting old entries from {} ...".format(self.name))

        cmd = "DELETE FROM {} WHERE datetime < DATETIME('NOW', 'start of day', '-7 day')".format(self.name)
        self.cur.execute(cmd)
        self.con.commit()

    def close(self):
        self.con.close()
        logger.info("Closed connection to {}.".format(self.name))


def log_mqtt_to_db(newdict, db):
    """
    Wrapper function controlling details of the logging
    newdict has structure of key, value where value is again a dict with "date" (a datetime) and "payload" (the value)
    """
    logdict = {}
    # We fill it with nans, in case some topics are not covered by the "newdict"
    for topic in log_topics_db:
        logdict[topic] = float("nan")

    lognow = now()
    for key, value in newdict.items():
        age = (lognow - value["date"]).total_seconds()
        if age < 30: # younger than 30 seconds
            # Then we decode this value
            logvalue_str = value["payload"].decode('UTF-8')
            if log_topic_types[key] == "float":
                logvalue = float(logvalue_str)
            elif log_topic_types[key] == "int":
                logvalue = int(round(float(logvalue_str)))
            else:
                logvalue = str(logvalue_str)
        else:
            logvalue = float('nan')
            logger.warning(f"Value for topic {key} is old, last data: {value['date']}: {value['payload']}")

        logkey = translate_topic_mqtt_to_db(key)
        logdict[logkey] = logvalue
    
    db.log(logdict)
    logger.debug(f"Wrote to log: {logdict}")
    #db.print()

       


def on_connect(client, datadict, flags, reason_code, properties):
    if reason_code.is_failure:
        print(f"Failed to connect: {reason_code}. loop_forever() will retry connection")
    else:
        # we should always subscribe from on_connect callback to be sure
        # our subscribed is persisted across reconnections.
        
        for topic in log_topics: 
            client.subscribe(topic)
        
        #client.subscribe("SMATripower/#")
        #client.subscribe("SMAHomeManager/#")
        #client.subscribe("VitocalOpen3E/CurrentElectricalPowerConsumptionSystem")


def on_message(client, userdata, message):
    # userdata is a (dict, db) with all the latest measurements

    # Update the dict:
    userdata["dict"][message.topic] = {"date":now(), "payload":message.payload}
    logger.debug(f"Message recieved: {message.topic} : {message.payload}")
    
    if message.topic == log_trigger_topic:
        # Then we log :
        log_mqtt_to_db(userdata["dict"], userdata["db"])
        # And ping the export 
        userdata["db"].ping_export()

             
        

def run():

    #db = LogDB(name="pvpi", path=":memory:", export_workdir="/home/mtewes/data", cols=log_topics_db)
    db = LogDB(name="pvpi", path="/home/mtewes/data/pvpi.db", export_workdir="/home/mtewes/data/", cols=log_topics_db)
    ini_userdata = {"dict":{}, "db":db}

    broker = "heizung.local"
    port = 1883
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.user_data_set(ini_userdata) # Start with an empty datadict and db
    mqttc.connect(broker, port)

    try:
        mqttc.loop_forever()

    except KeyboardInterrupt:
        print("Bye!")
    
    finally:
        db.close()
        mqttc.disconnect()
        print("Disconnected")


if __name__ == '__main__':

    #logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(level=logging.INFO)
    run()
    
    
