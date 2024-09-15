

Running on a Pi Zero 2 W, with Waveshare RS485 CAN HAT:
- Mosquitto MQTT broker
- open3e (https://github.com/open3e/open3e) to monitor a Vitocal 250-A heat pump via CAN, sending to MQTT
- the present Python stuff to
  - monitor a SMA Tripower 8 SE to MQTT (via http request, used only to get live PV power)
  - montior a SMA HomeManager 2 to MQTT (listening to its multicast communication)
  - control the heat pump's SG-Ready via a Shelly relais when excess PV power is available 
  - log all MQTT communication to sqlite, with daily exports
  - checkplots via email
  - overview I2C display at the Pi 


A first attempt via node-red / influxdb / grafana was too heavy for the Pi Zero 2.
This is now very lightweight.


Alternative solution if the homemanager identification causes trouble: https://github.com/AnotherDaniel/smahub

