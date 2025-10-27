"""
Script to control:
- the relay connected to the SG-Ready
- the LCD monitor to display current status of affairs


We listen to MQTT to get the current power data.
If we're feeding in above some threshhold and time, we switch the SG-Ready state of the heatpump.


Ideas:
- we don't want to switch the heatpump on and then quickly off as it uses power.

So if it's off, we wai


(psupply > 2000 and pwp < 500 (i..e, WP is off)) --> then it seems ok to turn it on via SG-Ready if this persists for 30 min.
 *or*  
(psupply > 1000  and pwp > 500 (i.e., WP is on)) --> then it's ok to increase target temp (or mainttain current status).

first line: basically means that pgenerate > 2000
second line: also similar to pgenerate > 2000 ---> but here we detect if some other consumer uses lots of this power.







About the LCD display:
https://gpiozero.readthedocs.io/en/latest/api_output.html#base-classes



"""



import gpiozero
import time
import sys

from datetime import datetime, timedelta, timezone

import logging
logger = logging.getLogger(__name__)

import paho.mqtt.client as mqtt

import secretsettings

from RPLCD.i2c import CharLCD # connected to 3.3V 

####### Demo code

#lcd = CharLCD(i2c_expander='PCF8574', address=0x27, port=1,
#              cols=16, rows=1, dotsize=8,
#              charmap='A02',
#              auto_linebreaks=False,
#              backlight_enabled=False)

#lcd.command(0x80 | 0x00 + 3)
#lcd.write_string("blabla")
#lcd.cursor_pos = (0, 40)
#lcd.command(0x80 | 0x40)
#lcd.command(0x10 | 0x00 +2)
#lcd.write_string('12345678')


#time.sleep(60*60*2)

#chan1 = gpiozero.DigitalOutputDevice(20) # GPIO 20
#chan2 = gpiozero.DigitalOutputDevice(21) # GPIO 21

#chan1.on()
#time.sleep(60*60*2)
#chan1.off()

##### Functions for MQTT

userdata = {} # We use this to keep track of the last post for each topic

topics = ["SMAHomeManager/psupply", "SMATripower/pgenerate", "VitocalOpen3E/CurrentElectricalPowerConsumptionSystem", "VitocalOpen3E/DomesticHotWaterSensor/Actual"]

def now():
    return datetime.now(timezone.utc)

def on_connect(client, datadict, flags, reason_code, properties):
    if reason_code.is_failure:
        print(f"Failed to connect: {reason_code}. loop_forever() will retry connection")
    else:
        # we should always subscribe from on_connect callback to be sure
        # our subscribed is persisted across reconnections.
        for topic in topics:    
            client.subscribe(topic)
        


def on_message(client, userdata, message):
    # userdata is a dict with all the latest measurements

    # Update the dict:
    userdata[message.topic] = {"date":now(), "payload":message.payload}
    logger.debug(f"Message recieved: {message.topic} : {message.payload}")
    



def main():

    #### Setup LCD
    lcd = CharLCD(i2c_expander='PCF8574', address=0x27, port=1,
              cols=16, rows=1, dotsize=8,
              charmap='A02',
              auto_linebreaks=False,
              backlight_enabled=False)
    
    lcd.write_string('Setup...')

    # Setup relay
    chan1 = gpiozero.DigitalOutputDevice(20) # GPIO 20
    chan2 = gpiozero.DigitalOutputDevice(21) # GPIO 21

    # Setup MQTT

    
    broker = secretsettings.mqtt_broker
    port = secretsettings.mqtt_port
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.user_data_set(userdata) # Start with an empty datadict and db
    mqttc.connect(broker, port)

    mqttc.loop_start()



    conditions_first_met = None
    conditions_last_met = None
    elapsed_met = None
    elapsed_not_met = None


    display_modes = ["time", "status", "elapsed", "WPpower", "WWtemp"]
    display_i = -1

    try:
        while True:

            display_i += 1
            if display_i > len(display_modes)-1:
                display_i = 0


            for topic in topics:
                
                try:
                    lastupdate = userdata[topic]["date"]
                    if now() - lastupdate > timedelta(minutes=1): # Data is old
                        logger.info(f"Data of {topic} is old, from {lastupdate.isoformat()}")
                        userdata[topic]["value"] = None
                    else:
                        userdata[topic]["value"] = float(userdata[topic]["payload"])

                except KeyError:
                    userdata[topic] = {"value": None}

            
            power = userdata["SMATripower/pgenerate"]["value"]
            if power is None:
                power = 0.0
        

            if power > 2000: # Conditions are met right now

                if conditions_first_met is None: # We set the time that the conditions were first met
                    conditions_first_met = now()
                    logger.info(f"Conditions first met at {conditions_first_met.isoformat()}")

                conditions_last_met = now()
                   
                elapsed_met = conditions_last_met - conditions_first_met
                logger.debug(f"Conditions have been met for {elapsed_met.total_seconds()} seconds")

                if elapsed_met >= timedelta(minutes=30):
                    # Switch on relay 1
                    if chan1.value == 0:
                        pass
                        #chan1.on()
            
            else: # Conditions are NOT met right now

                if conditions_last_met is not None: # They were met not long before...
                    logger.debug(f"Conditions no longer met right now")
                    elapsed_not_met = now() - conditions_last_met

                    if elapsed_not_met > timedelta(minutes=10):
                        logger.info("Conditions not met for over 10 minutes!")

                        # Resettign everything
                        conditions_first_met = None
                        conditions_last_met = None
                        elapsed_met = None
                        elapsed_not_met = None

                        # Switch off relay
                        if chan1.value == 1:
                            chan1.off()
                    else:
                        logger.debug(f"Waiting if conditions improve since {elapsed_not_met.total_seconds()} seconds")


            display_mode = display_modes[display_i]
            if display_mode == "time":
                displaystr = now().strftime("%H:%M UT")
        
            elif display_mode == "status":
                displaystr = f"P{power/1000.0:0>3.1f} R{chan1.value}{chan2.value}"
            
            elif display_mode == "elapsed":

                elapsed_met_minutes = min(int(elapsed_met.total_seconds()/60), 99) if elapsed_met is not None else -1
                elapsed_not_met_minutes = min(int(elapsed_not_met.total_seconds()/60), 99) if elapsed_not_met is not None else -1

                str_g = f"G{elapsed_met_minutes:0>2d} " if elapsed_met is not None else "G-- "
                str_n = f"N{elapsed_not_met_minutes:0>2d} " if elapsed_not_met is not None else "N-- "
                displaystr = str_g + str_n

            elif display_mode == "WPpower":
                wp_power = userdata["VitocalOpen3E/CurrentElectricalPowerConsumptionSystem"]["value"]
                displaystr = f"WP {wp_power: >4.0f}W" if wp_power is not None else "WP ----W"

            elif display_mode == "WWtemp":
                ww_temp = userdata["VitocalOpen3E/DomesticHotWaterSensor/Actual"]["value"]
                displaystr = f"WW {ww_temp: >4.1f}C" if ww_temp is not None else "WW ----C"
                

            assert len(displaystr) == 8
            logger.debug(f"displaystr: {displaystr}")
            
            lcd.cursor_pos = (0, 0)
            lcd.write_string(displaystr)

            time.sleep(1)
            #mqttc.loop_read() # process messages
            

    except KeyboardInterrupt:
        print("Bye!")
        mqttc.loop_stop()
        mqttc.disconnect()
        

    

if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)
    sys.exit(main())
