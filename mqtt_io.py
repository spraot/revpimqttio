#!/usr/bin/env python3

#MQTT format based on
#https://www.home-assistant.io/docs/mqtt/discovery/

#MQTT lib
#https://pypi.org/project/paho-mqtt/

#md-icon see
#https://cdn.materialdesignicons.com/4.5.95/

from json.decoder import JSONDecodeError
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
    switch_mqtt_topic_map = {}
    unique_id_suffix = '_mqttio'

    default_switch = {
        'name': 'Switch',
        'type': 'switch',
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
            self.switch_mqtt_topic_map.setdefault(switch['mqtt_command_topic'], []).append(switch)
            self.switch_mqtt_topic_map.setdefault(switch['mqtt_state_topic'], []).append(switch)
            try:
                self.switch_mqtt_topic_map.setdefault(switch['group_command_topic'], []).append(switch)
            except KeyError:
                pass

        #RPI init
        self.rpi = revpimodio2.RevPiModIO(autorefresh=True, direct_output=True, configrsc='/config.rsc')
        self.rpi.handlesignalend(self.programend)

        # TODO: Check whether PWM is enabled if type=pwm (see https://revpimodio.org/en/version-2-5-3-2/)

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

        for key in ['topic_prefix', 'homeassistant_prefix', 'mqtt_server_ip', 'mqtt_server_port', 'mqtt_server_user', 'mqtt_server_password', 'switches', 'unique_id_suffix']:
            try:
                self.__setattr__(key, config[key])
            except KeyError:
                pass

        self.availability_topic = self.topic_prefix + '/bridge/state'

        for switch in self.switches:
            if not 'id' in switch:
                raise SyntaxError('Cannot load configuration: switch does not have ''id''')
            if not 'output_id' in switch:
                raise SyntaxError('Cannot load configuration: switch does not have ''output_id''')

            for k, v in self.default_switch.items():
                if not k in switch:
                    switch[k] = v

            if not 'unique_id' in switch:
                switch['unique_id'] = switch["id"].replace('/', '_')
            switch['unique_id'] += self.unique_id_suffix

            if switch['type'] == 'pwm':
                component = 'sensor'
            else:
                component = switch['type']

            if not 'min_brightness' in switch:
                switch['min_brightness'] = 1
            else:
                try:
                    switch['min_brightness'] = float(switch['min_brightness'])
                except ValueError:
                    raise SyntaxError("Cannot load configuration: min_brightness must be a number".format(payload_brightness))

            switch["mqtt_config_topic"] = "{}/{}/{}/config".format(self.homeassistant_prefix, component, switch["unique_id"])
            switch["mqtt_command_topic"] = "{}/{}/set".format(self.topic_prefix, switch["id"])
            switch["mqtt_state_topic"] = "{}/{}/state".format(self.topic_prefix, switch["id"])
            switch["mqtt_availability_topic"] = "{}/{}/availability".format(self.topic_prefix, switch["id"])


    def configure_mqtt_for_switch(self, switch):
        switch_configuration = {
            "command_topic": switch["mqtt_command_topic"],
            "schema": "template",
            "command_on_template": "on",
            "command_off_template": "off",
            "state_topic": switch["mqtt_state_topic"],
            "state_template": "{{ value }}",
            "availability": [
                {'topic': self.availability_topic, 'value_template': '{{ value_jason.state }}'},
                {'topic': switch["mqtt_availability_topic"], 'value_template': '{{ value_jason.state }}'},
            ],
            "retain": False,
            "device": {
                "identifiers": [switch["unique_id"]],
                "manufacturer": "KUNBUS GmbH",
                "model": "RevPi Digital IO",
                "name": "RevPi "+switch['type'],
                "sw_version": "mqttio"
            },
            "unique_id": switch["unique_id"]
        }

        if switch['type'] == 'pwm':
            switch_configuration['unit_of_measurement'] = '%'

        try:
            switch_configuration['name'] = switch["name"]
        except KeyError:
            switch_configuration['name'] = switch["unique_id"]

        switch_configuration['device']['name'] = switch_configuration["name"]

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

        while getattr(thread, "service_running", True):
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
            self.set_switch_state(switch, 0)
            self.mqtt_broadcast_switch_availability(switch, '{"state": "offline"}')

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

        #Broadcast current switch state to MQTT
        for switch in self.switches:
            self.mqtt_broadcast_switch_availability(switch, '{"state": "online"}')

        #Subsribe to MQTT switch updates
        for topic in self.switch_mqtt_topic_map:
            self.mqttclient.subscribe(topic)

        self.mqttclient.publish(self.availability_topic, payload='{"state": "online"}', qos=0, retain=True)
        self.mqttclient.will_set(self.availability_topic, payload='{"state": "offline"}', qos=0, retain=True)

    def mqtt_on_message(self, client, userdata, msg):
        payload = msg.payload.decode('utf-8').strip()
        logging.info("Received MQTT message on topic: " + msg.topic + ", payload: " + payload + ", retained: " + str(msg.retain))

        try:
            switch_group = self.switch_mqtt_topic_map[str(msg.topic)]
            logging.debug("Found switch(es) matching MQTT message: " + ', '.join(s["name"] for s in switch_group))
        except KeyError:
            logging.error("Could not find switch corresponding to topic " + msg.topic)
            return

        payload_brightness = None
        if payload.startswith('{'):
            try:
                payload_json = json.loads(payload)
            except json.decoder.JSONDecodeError:
                logging.error('Could not decode JSON sent on topic "{}": {}'.format(msg.topic, payload))
                return
            try:
                payload_state = payload_json['state'].lower()
            except KeyError:
                payload_state = ''
            try:
                payload_brightness = payload_json['brightness']
            except KeyError:
                pass
        else:
            payload_state = payload.lower()

        for s in switch_group:
            if msg.topic == s['mqtt_state_topic'] and not msg.retain:
                continue

            broadcast_state = payload_state
            state = None
            if payload_state == "toggle":
                state = not self.rpi.io[s["output_id"]].value
                broadcast_state = 'on' if state else 'off'
            elif payload_state == "on":
                state = True
            elif payload_state == "off":
                state = False
            elif s['type'] == 'pwm':
                try:
                    state = float(payload_state)
                    if state < 0 or state > 100:
                        raise ValueError('pwm command must be percent value between 0 and 100')
                except ValueError:
                    logging.error("Setting output state to " + payload_state + " not supported for pwm type, must be percent: 0 <= x <= 100")
                    continue
            elif payload_brightness is None:
                logging.error("Setting output state to " + payload_state + " not supported for switch type")
                continue
            
            if state != 'off' and payload_brightness is not None:
                try:
                    state = float(payload_brightness) >= s['min_brightness']
                except ValueError:
                    logging.error("Cannot apply brightness {}, brightness must be a number".format(payload_brightness))
                    continue
                broadcast_state = 'on' if state else 'off'

            self.set_switch_state(s, state)
            self.mqtt_broadcast_state(s, broadcast_state)

    def set_switch_state(self, switch, state):
        logging.debug("Setting output " + switch["output_id"] + " to " + str(state))
        if switch['type'] == 'pwm':
            if state == True:
                state = 100
            if state == False:
                state = 0
            self.rpi.io[switch["output_id"]].value = round(state*2.55)
        else:
            self.rpi.io[switch["output_id"]].value = 1 if state else 0  

    def mqtt_broadcast_switch_availability(self, switch, value):
       logging.debug("Broadcasting MQTT message on topic: " + switch["mqtt_availability_topic"] + ", value: " + value)
       self.mqttclient.publish(switch["mqtt_availability_topic"], payload=value, qos=0, retain=False)

    def mqtt_broadcast_state(self, switch, state):
        logging.debug("Broadcasting MQTT message on topic: " + switch["mqtt_state_topic"] + ", value: " + state)
        self.mqttclient.publish(switch["mqtt_state_topic"], payload=state, qos=0, retain=True)

if __name__ == "__main__":
    mqttLightControl =  MqttLightControl()
    mqttLightControl.start()
