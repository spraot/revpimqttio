#!/usr/bin/env python3

#MQTT format based on
#https://www.home-assistant.io/docs/mqtt/discovery/

#MQTT lib
#https://pypi.org/project/paho-mqtt/

#md-icon see
#https://cdn.materialdesignicons.com/4.5.95/

import os
import sys
import json
import yaml
import traceback
import revpimodio2
import paho.mqtt.client as mqtt
import time
import threading
import logging
import atexit

class MqttLightControl():
    config_file = 'config.yml'
    topic_prefix = 'pi/io'
    homeassistant_prefix = 'homeassistant'
    mqtt_server_ip = "localhost"
    mqtt_server_port = 1883
    mqtt_server_user = ""
    mqtt_server_password = ""
    switch_mqtt_command_topic_map = {}
    switch_input_id_map = {}

    default_switch = {
        'name': 'Switch',
        'md-icon': 'ceiling-light'
    }

    switches = []

    def __init__(self):
        logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"), format='%(asctime)s;<%(levelname)s>;%(message)s')
        logging.info("Init")

        if len(sys.argv) > 1:
            self.config_file = sys.argv[1]

        self.load_config()

        #Construct map for fast indexing
        for switch in self.switches:
            self.switch_mqtt_command_topic_map[switch["mqtt_command_topic"]] = switch

        #RPI init
        self.rpi = revpimodio2.RevPiModIO(autorefresh=True, configrsc='/config.rsc')
        self.rpi.handlesignalend(self.programend)

        #MQTT init
        self.mqttclient = mqtt.Client()
        self.mqttclient.on_connect = self.mqtt_on_connect
        self.mqttclient.on_message = self.mqtt_on_message

         #Register program end event
        atexit.register(self.programend)

        logging.info("init done")

    def load_config(self):
        logging.info("Reading config from "+self.config_file)

        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f)

        for key in ['topic_prefix', 'homeassistant_prefix', 'mqtt_server_ip', 'mqtt_server_port', 'mqtt_server_user', 'mqtt_server_password', 'switches']:
            try:
                self.__setattr__(key, config[key])
            except KeyError:
                pass

        for switch in self.switches:
            if not 'id' in switch:
                raise SyntaxError('Cannot load configuration: switch does not have ''id''')
            if not 'output_id' in switch:
                raise SyntaxError('Cannot load configuration: switch does not have ''output_id''')

            for k, v in self.default_switch.items():
                if not k in switch:
                    switch[k] = v

            if not 'output_id' in switch:
                switch['unique_id'] = switch["id"]

            switch["mqtt_config_topic"] = "{}/switch/{}/config".format(self.homeassistant_prefix, switch["id"])
            switch["mqtt_command_topic"] = "{}/{}/set".format(self.topic_prefix, switch["id"])
            switch["mqtt_state_topic"] = "{}/{}/state".format(self.topic_prefix, switch["id"])
            switch["mqtt_availability_topic"] = "{}/{}/availability".format(self.topic_prefix, switch["id"])


    def configure_mqtt_for_switch(self, switch):
        switch_configuration = {
            "command_topic": switch["mqtt_command_topic"],
            "state_topic": switch["mqtt_state_topic"],
            "availability_topic": switch["mqtt_availability_topic"],
            "retain": True,
            "device": {"identifiers": switch["id"]}
        }

        try:
            switch_configuration['name'] = switch["name"]
            switch_configuration['device']['name'] = switch["name"]
        except KeyError:
            pass

        try:
            switch_configuration['unique_id'] = switch["unique_id"]
        except KeyError:
            switch_configuration['unique_id'] = switch["id"]

        try:
            switch_configuration['icon'] = "mdi:" + switch["md-icon"]
        except KeyError:
            pass

        json_conf = json.dumps(switch_configuration)
        logging.debug("Broadcasting homeassistant configuration for switch: " + switch["name"] + ":" + json_conf)
        self.mqttclient.publish(switch["mqtt_config_topic"], payload=json_conf, qos=0, retain=True)

    def start(self):
        logging.info("starting")

        #MQTT startup
        logging.info("Starting MQTT client")
        self.mqttclient.username_pw_set(self.mqtt_server_user, password=self.mqtt_server_password)
        self.mqttclient.connect(self.mqtt_server_ip, self.mqtt_server_port, 60)
        self.mqttclient.loop_start()
        logging.info("MQTT client started")

        #RPI startup
        logging.info("Starting RPI client")
        self.rpi.mainloop(blocking=False)
        logging.info("RPI client started")

        #Start status thread
        logging.info("Starting status thread")
        self.status_thread = threading.Thread(target = self.status_informer, args = (self.rpi,))
        self.status_thread.service_running = True
        self.status_thread.start()
        logging.info("Status started")

        logging.info("started")

    def status_informer(self, rpi):
        thread = threading.currentThread()
        a1_state = False
        rpi.core.A1 = revpimodio2.OFF

        while getattr(thread, "service_runing", True):
            if a1_state:
                rpi.core.A1 = revpimodio2.OFF
            else:
                rpi.core.A1 = revpimodio2.GREEN

            a1_state = not a1_state
            time.sleep(1)

    def programend(self):
        logging.info("stopping")
        try:
            self.status_thread.service_running = False
        except AttributeError:
            pass

        for switch in self.switches:
            self.mqtt_broadcast_switch_availability(switch, "offline")

        self.rpi.core.A1 = revpimodio2.OFF
        self.mqttclient.disconnect()
        self.rpi.exit()
        time.sleep(0.5)
        logging.info("stopped")

    def mqtt_on_connect(self, client, userdata, flags, rc):
        logging.info("MQTT client connected with result code "+str(rc))

        #Configure MQTT for switches
        for switch in self.switches:
            self.configure_mqtt_for_switch(switch)

        #Broadcast current switch state to MQTT for switches
        for switch in self.switches:
            self.mqtt_broadcast_switch_availability(switch, "online")

        #Subsribe to MQTT switch updates
        for switch in self.switches:
            self.mqttclient.subscribe(switch["mqtt_command_topic"])

    def mqtt_on_message(self, client, userdata, msg):
        payload_as_string = msg.payload.decode('utf-8')
        logging.info("Received MQTT message on topic: " + msg.topic + ", payload: " + payload_as_string + ", retained: " + str(msg.retain))

        switch = self.switch_mqtt_command_topic_map[str(msg.topic)]
        logging.debug("Found switch matching MQTT message: " + switch["name"])

        if payload_as_string.upper() == "ON":
            self.set_switch_state(switch, True)
        elif payload_as_string.upper() == "OFF":
            logging.debug("Setting state " + payload_as_string)
            self.set_switch_state(switch, False)
        else:
            logging.warn("Setting output state to " + payload_as_string + " not supported")

    def set_switch_state(self, switch, state):
        logging.debug("Setting output " + switch["output_id"] + " to " + str(state))
        self.rpi.io[switch["output_id"]].value = 1 if state else 0

        try:
            self.mqtt_broadcast_state(switch, state)           
        except Exception:
            logging.error("Cannot set switch state" + traceback.format_exc())     


    def mqtt_broadcast_switch_availability(self, switch, value):
       logging.debug("Broadcasting MQTT message on topic: " + switch["mqtt_availability_topic"] + ", value: " + value)
       self.mqttclient.publish(switch["mqtt_availability_topic"], payload=value, qos=0, retain=True)

    def mqtt_broadcast_state(self, switch, state):
        if state:
            mqtt_payload = "ON"
        else:
            mqtt_payload = "OFF"
        logging.debug("Broadcasting MQTT message on topic: " + switch["mqtt_state_topic"] + ", value: " + mqtt_payload)
        self.mqttclient.publish(switch["mqtt_state_topic"], payload=mqtt_payload, qos=0, retain=True)

if __name__ == "__main__":
    mqttLightControl =  MqttLightControl()
    mqttLightControl.start()
