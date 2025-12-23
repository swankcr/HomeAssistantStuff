import time
import json
import os
import sys
import signal
import paho.mqtt.client as mqtt
from hoymiles_modbus.client import HoymilesModbusTCP
from dotenv import load_dotenv

#########################################################################################
# This is a python script to run in a loop that queries my Hoymiles DTU Gateway for solar 
# production data and send to HomeAssistant via MQTT.  It runs locally and doesn't depend 
# on the Hoymiles cloud.  A fair bit of this was written via Gemini.  And it relies on 
# the HoymilesModbusTCP module.  
# swankcr - Dec 23, 2025 
#########################################################################################

# Okay, so this is really just to allow me to run outside of Docker to test changes to the code
# In docker, the variables are all passed by an .env file.  So I just use that same .env file when 
# running locally.  If the .env file doesn't exist (it won't when running in the container), it 
# will just grab ENV vars.  
load_dotenv()

# --- Pulling in all the configuration variables --- 
MODBUS_IP = os.getenv('MODBUS_IP')
UPDATE_INTERVAL = int(os.getenv('UPDATE_INTERVAL', 60))
ENABLE_MQTT = os.getenv('ENABLE_MQTT', 'True').lower() in ('true', '1', 'yes')
DEBUG_PANELS = os.getenv('DEBUG_PANELS', 'False').lower() in ('true', '1', 'yes')
MQTT_BROKER = os.getenv('MQTT_BROKER')
MQTT_PORT = int(os.getenv('MQTT_PORT'))  
MQTT_USER = os.getenv('MQTT_USER')
MQTT_PASS = os.getenv('MQTT_PASS')
TOPIC_BASE = os.getenv('TOPIC_BASE')
TOPIC_STATE = f"{TOPIC_BASE}/state" if TOPIC_BASE else None

# I noticed some weird behavior with calculated production.  And also, it seemed like there was a 
# weird reset interval where the day's production was reset at GMT midnight.  so randomly my panels 
# would just stop showing data.  In order to do this, I am just tracking the last total production 
# in a variable.  
last_valid_total_wh = 0
client = None


# --- signal handlng and cleanup ---  
def signal_handler(sig, frame):
    """Graceful Exit Handler"""
    print("\n[!] Exiting gracefully...")
    if client:
        print("[!] Stopping MQTT loop...")
        client.loop_stop()
        client.disconnect()
        print("[!] MQTT Disconnected.")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# --- Connecting to MQTT --- 
def on_connect(client, userdata, flags, rc, properties=None):
    """Called when we connect to the broker."""
    if rc == 0:
        print(f"Connected to MQTT Broker (RC: {rc})")
    else:
        print(f"Failed to connect, return code {rc}")

# Using MQTT as persistence for the valid production value.  When we connect, 
# pull it from the queue.  This way the container can be upgraded without storing 
# persistent data.  
def on_message(client, userdata, msg):
    """
    Called when we receive a message. 
    We use this ONCE at startup to load the retained value.
    """
    global last_valid_total_wh
    try:
        payload = json.loads(msg.payload.decode())
        saved_total = int(payload.get('total_production_wh', 0))
        
        if saved_total > last_valid_total_wh:
            print(f"[Persistence] RESTORED last valid total from MQTT: {saved_total} Wh")
            last_valid_total_wh = saved_total
        else:
            print(f"[Persistence] MQTT value ({saved_total}) is lower/equal to current memory. Ignoring.")
            
    except Exception as e:
        print(f"[Persistence] Error parsing retained message: {e}")

# --- Connect and restore state to MQTT ---  
if ENABLE_MQTT:
    if not all([MQTT_BROKER, MQTT_USER, MQTT_PASS, TOPIC_BASE]):
        print("ERROR: MQTT is enabled but missing configuration variables!")
        exit(1)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    
    # Attach callbacks
    client.on_connect = on_connect
    client.on_message = on_message 

    try:
        print(f"Connecting to MQTT Broker at {MQTT_BROKER}...")
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start()

        # --- PERSISTENCE RESTORATION BLOCK ---
        # We subscribe temporarily to fetch the last "Retained" message
        print(f"[Persistence] Checking {TOPIC_STATE} for retained history...")
        client.subscribe(TOPIC_STATE)
        
        # Wait 2 seconds for the broker to send the retained message
        time.sleep(2) 
        
        # We unsubscribe so we don't get confused by our own future messages
        client.unsubscribe(TOPIC_STATE)
        print(f"[Persistence] Restoration complete. Starting value: {last_valid_total_wh}")
        # -------------------------------------

    except Exception as e:
        print(f"Failed to connect to MQTT: {e}")
        exit(1)
else:
    print("WARNING: MQTT is DISABLED. Running in 'Dry Run' console-only mode.")

# --- The main loop body ---  
print(f"Starting Loop. Polling {MODBUS_IP} every {UPDATE_INTERVAL}s.")

while True:
    try:
        plant_data = HoymilesModbusTCP(MODBUS_IP).plant_data
        
        # --- Alarm Logic ---
        active_alarms = []
        for inverter in plant_data.inverters:
            a_code = getattr(inverter, 'alarm_code', 0)
            if a_code > 0:
                alarm_data = {
                    "serial_number": str(inverter.serial_number),
                    "port": inverter.port_number,
                    "alarm_code": a_code,
                    "operating_status": getattr(inverter, 'operating_status', -1)
                }
                active_alarms.append(alarm_data)

        # --- Debug Printing ---
        if DEBUG_PANELS:
            print(f"\n--- DEBUG: RAW PANEL DATA ({len(plant_data.inverters)} panels) ---")
            for inverter in plant_data.inverters:
                a_code = getattr(inverter, 'alarm_code', 'N/A')
                op_status = getattr(inverter, 'operating_status', 'N/A')
                print(f"SN: {inverter.serial_number} | Port: {inverter.port_number} | "
                      f"Power: {inverter.pv_power}W | Total: {inverter.total_production}Wh | "
                      f"Status: {op_status} | Alarm_Code: {inverter.alarm_code} | Alarm_Count: {inverter.alarm_count}")
            print("----------------------------------------------------------\n")

        # --- Logic: Hybrid + Memory ---
        dtu_reported_total = int(plant_data.total_production)
        calculated_total = int(sum(inverter.total_production for inverter in plant_data.inverters))
        
        # Fallback
        if dtu_reported_total > 0:
            current_total_wh = dtu_reported_total
            source = "DTU"
        else:
            current_total_wh = calculated_total
            source = "CALC"

        # Memory Check
        if current_total_wh < last_valid_total_wh:
            print(f"Glitch detected! New: {current_total_wh} < Old: {last_valid_total_wh}. Ignoring.")
            current_total_wh = last_valid_total_wh 
        else:
            last_valid_total_wh = current_total_wh 

        # Power Check
        dtu_power = float(plant_data.pv_power)
        calc_power = float(sum(inverter.pv_power for inverter in plant_data.inverters))
        
        if dtu_power == 0 and calc_power > 0:
            final_power = calc_power
        else:
            final_power = dtu_power

        # --- Payload ---
        payload = {
            "current_power_w": final_power,
            "total_production_wh": current_total_wh,
            "alarm_flag": plant_data.alarm_flag,
            "active_alarms": active_alarms,
            "debug_source": source
        }

        # --- Publish ---
        log_msg = f"[{time.strftime('%H:%M:%S')}] Src:{source} | Pwr:{final_power}W | Total:{current_total_wh}Wh | Alarm_Status: {plant_data.alarm_flag}  | Alarms:{len(active_alarms)}"
        if ENABLE_MQTT and client:
            # CRITICAL: retain=True makes the broker save this message!
            client.publish(TOPIC_STATE, json.dumps(payload), retain=True)
            print(f"{log_msg} -> SENT MQTT (Retained)")
        else:
            print(f"{log_msg} -> SKIPPED MQTT (Dry Run)")

    except Exception as e:
        print(f"Error: {e}")

    # Interruptible sleep
    for _ in range(UPDATE_INTERVAL):
        time.sleep(1)
