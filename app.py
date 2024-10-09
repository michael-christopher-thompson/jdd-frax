import paho.mqtt.client as mqtt
import json
from datetime import datetime
import time

# MQTT broker credentials
broker = "frax.wavedata.cloud"
port = 1883
topic = "AMI/Pump/PF-Asset1/Data/Channels"
username = "ProFracAPI"
password = "#puBLi25D474"

# Generate the current timestamp in the format "2024-07-17T17:34:38.577Z"
def get_current_timestamp():
    return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

# JSON payload with dynamically generated timestamp
def create_payload():
    return {
        "timestamp": get_current_timestamp(),
        "timezone": -6,
        "channels": {
            "Engine: DF Supply Pressure": 159.542,
            "Engine: DF Displacement": 0,
            "Engine: DF Substitution": 0,
            "Engine: DF Type": 2,
            "EGT Left Bank Average": 368.544,
            "EGT Right Bank Average": 253.175,
            "Engine: Percent Load": 8,
            "Engine: Speed": 700.375,
            "Engine: Percent Torque": 2,
            "Discharge Pressure": 149.89,
            "Transmission: Current Gear": 0,
            "Engine: Throttle Percent": 0,
            "Fluid-End: Discharge Rate": 0,
            "Fault SPN 1": 0,
            "Fault SPN 2": 0,
            "Fault SPN 3": 0,
            "Fault SPN 4": 0,
            "Fault SPN 5": 0,
            "Fault FMI 1": 0,
            "Fault FMI 2": 0,
            "Fault FMI 3": 0,
            "Fault FMI 4": 0,
            "Fault FMI 5": 0,
            "Trip Discharge Pressure": 0,
            "Discharge Pressure: 5s High": 165.069,
            "AI Channel 1: Scaled Value": 0,
            "AI Channel 2: Scaled Value": 0,
            "AI Channel 5: Scaled Value": 0,
            "AI Channel 6: Scaled Value": 0,
            "Fire Detection": 0.24,
            "AI Channel 8: Scaled Value": 0,
            "AI Channel 9: Scaled Value": 0,
            "AI Channel 10: Scaled Value": 0,
            "Suction Pressure": 85.454,
            "Power-End Oil Pressure": 105.864,
            "Power-End Oil Temperature": 74.28,
            "AI Channel 15: Scaled Value": 0,
            "AI Channel 16: Scaled Value": 0,
            "Fan Drive": 5,
            "Engine: Idle": 1,
            "Engine: StartSwitch": 0,
            "Engine: Hours": 9400.35,
            "Engine: Battery Voltage": 25.6,
            "Engine: Requested Speed": 650,
            "Engine: Coolant Temperature": 174.2,
            "Engine Coolant Pressure": 1,
            "Engine: Coolant Level": 0,
            "Engine Barometric Pressure": 13.271,
            "Engine: Air Filter Differential Pressure": 0,
            "Engine Fuel Rate": 0.06,
            "Engine: Fuel Temperature": 95,
            "Engine Fuel Pressure": 87.023,
            "Engine: Oil Temperature": 178.25,
            "Engine: Oil Pressure": 60.336,
            "Engine: Oil Remote Reservoir": 0,
            "Engine: Oil Pressure Pre Filter": 0,
            "Engine Oil Filter Diff Pressure": 2.031,
            "Engine Crankcase Pressure": 0.009,
            "Engine: Exhaust Gas Pressure": 0,
            "Engine: Exhaust Gas Recirculation Temperature": 0,
            "Engine: Exhaust Gas Temperature": 0,
            "Engine Intake Temperature": 113,
            "Engine Turbo Charger 1 Speed": 8,
            "Engine Turbo Charger 2 Speed": 8,
            "Engine: Fan Speed Estimated Percent": 0,
            "Engine: Injector Metering Rail 1 Pressure": 0,
            "Engine: DF System State": 0,
            "Transmission: Input Speed": 675,
            "Transmission: Output Speed": 0,
            "Transmission Oil Pressure": 325.617,
            "Transmission Oil Temperature": 176,
            "Transmission: Actual Gear Ratio": 0,
            "Transmission Torque Oil Temperature": 176,
            "Transmission: Driveline Engaged": 0,
            "Transmission: Shift In Progress": 0,
            "Transmission: Neutral": 1,
            "Transmission: Oil Filter Restriction": 0,
            "Grease System: Active": 0,
            "FluidEnd Hours Total": 8853.45,
            "FluidEnd Hours Service": 0,
            "PowerEnd Hours Total": 6714.422,
            "PowerEnd Hours Service": 0,
            "Trans. Hours Total": 7510.001,
            "Trans. Hours Service": 0,
            "Engine Hours Total": 9400.35,
            "Engine Hours Service": 0,
            "Strokes Total": 33896364,
            "Strokes Service": 0,
            "J1939 Amber Alarm": 0,
            "J1939 Red Alarm": 0,
            "Station": 0,
            "Group": 0,
            "Brake Engaged": 0,
            "Engine Running": 1,
            "Engine Type": 3,
            "Trans. Locked Up": 0,
            "Warmup Engaged": 0,
            "Poll Latency": 0,
            "Poll Timeout": 0,
            "Write Latency": 0,
            "Write Timeout": 0,
            "Comm Lost Events Count": 0,
            "Network Lost Events Count": 0
        }
    }

# Callback for connection success
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully to MQTT broker")
    else:
        print(f"Connection failed with code {rc}")

# Create an MQTT client instance
client = mqtt.Client()

# Set username and password
client.username_pw_set(username, password)

# Assign the on_connect callback function
client.on_connect = on_connect

# Connect to the MQTT broker
client.connect(broker, port, 60)

# Start the network loop in a separate thread
client.loop_start()

# Publish the data every second
try:
    while True:
        payload_str = json.dumps(create_payload())  # Create the payload with current timestamp
        client.publish(topic, payload_str)  # Publish the payload
        time.sleep(1)  # Wait for 1 second before publishing again
except KeyboardInterrupt:
    print("Publishing stopped.")

# Stop the network loop and disconnect the client
client.loop_stop()
client.disconnect()
