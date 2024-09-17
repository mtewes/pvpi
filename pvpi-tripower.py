import sys
import requests
import warnings
import urllib3
import json
import time

import paho.mqtt.client as mqtt



def read_tripower():
    """
    """

    tripower_json_url = "https://192.168.0.34/dyn/getDashValues.json"

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=urllib3.exceptions.InsecureRequestWarning)
        response = requests.get(tripower_json_url, verify=False)

    if response.status_code != 200:
        dict = {"tripower_respons_status": response.status_code}
        print(dict)
        return(dict)

    try:
        dashvals = response.json()["result"]["01B8-xxxxx788"] # Get rid of some wrapper
        #print(json.dumps(dashvals, indent="  "))
        #print(json.dumps(dashvals))
    
        parsed = {"psupply":dashvals["6100_40463600"]["9"][0]["val"],
                  "ppurchase":dashvals["6100_40463700"]["9"][0]["val"],
                  "pgenerate":dashvals["6100_0046C200"]["9"][0]["val"],
                  "esupply":dashvals["6400_00462400"]["9"][0]["val"]/1000.0,
                  "epurchase":dashvals["6400_00462500"]["9"][0]["val"]/1000.0,
                  }
        parsed["pconsume"] = max(parsed["pgenerate"] + parsed["ppurchase"] - parsed["psupply"], 0)
        # This does not work well when the weather is rapidly changing, as these measurements are not simultaneous it seems.


        print(parsed)
        return parsed

    except TypeError:
        print("Issue with data:", dashvals)
        return {"tripower_parsing_issue":1}
    #print(parsed)
    
    
def run():

    
    broker = "heizung.local"
    port = 1883
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.connect(broker, port)
    mqttc.loop_start()

    try:
        while True:
            d = read_tripower()
            for (key, value) in d.items():
                msg_info = mqttc.publish(f"SMATripower/{key}", value, qos=0)
                msg_info.wait_for_publish()

            time.sleep(3)

    except KeyboardInterrupt:
        print("Bye!")
        mqttc.disconnect()
        mqttc.loop_stop()


def main():
    #read_tripower()
    run()
    return 0

if __name__ == '__main__':
    sys.exit(main())


# Stuff from the node-red function to identify what is what in the json:

#"func": "// @ts-nocheck\nvar 
# arr_p_supply = msg.payload.result[\"01B8-xxxxx788\"][\"6100_40463600\"]
# \nvar arr_p_perchase = msg.payload.result[\"01B8-xxxxx788\"][\"6100_40463700\"]
# \nvar arr_p_generate = msg.payload.result[\"01B8-xxxxx788\"][\"6100_0046C200\"]
# \nvar arr_p_batcharge = msg.payload.result[\"01B8-xxxxx788\"][\"6100_00496900\"]
# \nvar arr_p_batdischarge = msg.payload.result[\"01B8-xxxxx788\"][\"6100_00496A00\"]
# \nvar arr_state_bat = msg.payload.result[\"01B8-xxxxx788\"][\"6100_00295A00\"]
# \nvar arr_energy_yield = msg.payload.result[\"01B8-xxxxx788\"][\"6400_00462400\"]
# \nvar arr_energy_absorb = msg.payload.result[\"01B8-xxxxx788\"][\"6400_00462500\"]
# \n\nP_supply = arr_p_supply[\"9\"][0].val
# \nP_perchase = arr_p_perchase[\"9\"][0].val
# \nP_generate = arr_p_generate[\"9\"][0].val
# \nP_batcharge = arr_p_batcharge[\"9\"][0].val
# \nP_batdischarge = arr_p_batdischarge[\"9\"][0].val
# \nP_home = 0;
# \nS_bat = arr_state_bat[\"9\"][0].val
# \nE_yield = arr_energy_yield[\"9\"][0].val
# \nE_absorb = arr_energy_absorb[\"9\"][0].val
# \n\nif (!(((P_supply > 0) && (P_perchase > 0)) || ((P_batdischarge > 0) && (P_batcharge > 0))))\n{\n    
# P_home = P_generate + P_perchase  + P_batdischarge - P_supply - P_batcharge;\n}\n\nif (P_home < 0)\n
# {\n   P_home = 0; \n}\n\nP_bez = P_perchase;\nP_ein = P_supply
# \n\nmsg.payload = {P_bez, P_ein, P_perchase, P_supply, P_generate, P_batcharge, P_batdischarge, P_home, S_bat, E_yield, E_absorb}
# \nmsg.topic = \"Messwerte2\";\n\nreturn msg;",
#    "outputs": 1,
    