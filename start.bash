


# the exec sh is to keep the screen alive after the command has ended


sudo ip link set can0 type can bitrate 250000
sudo ifconfig can0 up

screen -S open3e -dm bash -c 'cd /home/mtewes/open3e;
source /home/mtewes/open3e-env/bin/activate;
open3e -c can0 -r 271,274,268,269,318,381,543,548,565,1043,1190,1846,2351,2352,2369,2486,2487,2488,2496,2735  -t 15 --config devices.json -v -m localhost:1883:VitocalOpen3E;
exec sh'

screen -S hm -dm bash -c 'cd /home/mtewes/pvpi;
source /home/mtewes/pvpi-env/bin/activate;
python pvpi-homemanager.py;
exec sh'

screen -S tripower -dm bash -c 'cd /home/mtewes/pvpi;
source /home/mtewes/pvpi-env/bin/activate;
python pvpi-tripower.py;
exec sh'

screen -S log -dm bash -c 'cd /home/mtewes/pvpi;
source /home/mtewes/pvpi-env/bin/activate;
python mqtt-logger.py;
exec sh'


