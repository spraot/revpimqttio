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
import revpimodio2
import paho.mqtt.client as mqtt
import logging
from pythonjsonlogger import jsonlogger
import atexit

logger = logging.getLogger()
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter('%(message)%(levelname)', timestamp='dt')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(os.environ.get('LOGLEVEL', 'INFO'))

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
        logger.info("Init")

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
        self.rpi = revpimodio2.RevPiModIO(autorefresh=True, shared_procimg=True, configrsc='/config.rsc')
        self.rpi.handlesignalend(self.programend)

        # TODO: Check whether PWM is enabled if type=pwm (see https://revpimodio.org/en/version-2-5-3-2/)

        #MQTT init
        self.mqttclient = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
        self.mqttclient.on_connect = self.mqtt_on_connect
        self.mqttclient.on_message = self.mqtt_on_message

         #Register program end event
        atexit.register(self.programend)

        logger.info("init done")

    def load_config(self):
        logger.info("Reading config from "+self.config_file)

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

            if not 'name' in switch:
                switch['name'] = switch["id"]

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
                    raise SyntaxError("Cannot load configuration: min_brightness must be a number")

            switch["mqtt_config_topic"] = "{}/{}/{}/config".format(self.homeassistant_prefix, component, switch["unique_id"])
            switch["mqtt_command_topic"] = "{}/{}/set".format(self.topic_prefix, switch["id"])
            switch["mqtt_state_topic"] = "{}/{}/state".format(self.topic_prefix, switch["id"])
            switch["mqtt_availability_topic"] = "{}/{}/availability".format(self.topic_prefix, switch["id"])


    def configure_mqtt_for_switch(self, switch):
        switch_configuration = {
            "name": switch["name"],
            "command_topic": switch["mqtt_command_topic"],
            "schema": "template",
            "command_on_template": "on",
            "command_off_template": "off",
            "state_topic": switch["mqtt_state_topic"],
            "state_template": "{{ value_jason.state }}",
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

        json_conf = json.dumps(switch_configuration)
        logger.debug("Broadcasting homeassistant configuration for switch: " + switch["name"] + ":" + json_conf)
        self.mqttclient.publish(switch["mqtt_config_topic"], payload=json_conf, qos=0, retain=True)

    def start(self):
        logger.info("starting")

        #MQTT startup
        logger.info("Starting MQTT client")
        self.mqttclient.username_pw_set(self.mqtt_server_user, password=self.mqtt_server_password)
        self.mqttclient.connect(self.mqtt_server_ip, self.mqtt_server_port, 60)
        self.mqttclient.loop_start()
        logger.info("MQTT client started")

        #RPI startup
        logger.info("Starting RPI client")
        self.rpi.mainloop(blocking=False)
        logger.info("RPI client started")

        logger.info("started")

    def programend(self):
        logger.info("stopping")

        for switch in self.switches:
            self.set_switch_state(switch, 0)
            self.mqtt_broadcast_switch_availability(switch, '')

        self.mqttclient.disconnect()
        self.rpi.exit()
        logger.info("stopped")

    def mqtt_on_connect(self, client, userdata, flags, rc):
        logger.info("MQTT client connected with result code "+str(rc))

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
        logger.debug("Received MQTT message on topic: " + msg.topic + ", payload: " + payload + ", retained: " + str(msg.retain))

        try:
            switch_group = self.switch_mqtt_topic_map[str(msg.topic)]
            logger.debug("Found switch(es) matching MQTT message: " + ', '.join(s["name"] for s in switch_group))
        except KeyError:
            logger.error("Could not find switch corresponding to topic " + msg.topic)
            return

        payload_brightness = None
        if payload.startswith('{'):
            try:
                payload_json = json.loads(payload)
            except json.decoder.JSONDecodeError:
                logger.error('Could not decode JSON sent on topic "{}": {}'.format(msg.topic, payload))
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
                    logger.error("Setting output state to " + payload_state + " not supported for pwm type, must be percent: 0 <= x <= 100")
                    continue
            elif payload_brightness is None:
                logger.error("Setting output state to " + payload_state + " not supported for switch type")
                continue
            
            if state != 'off' and payload_brightness is not None:
                try:
                    state = float(payload_brightness) >= s['min_brightness']
                except ValueError:
                    logger.error("Cannot apply brightness {}, brightness must be a number".format(payload_brightness))
                    continue
                broadcast_state = 'on' if state else 'off'

            self.set_switch_state(s, state)
            self.mqtt_broadcast_state(s, broadcast_state)

    def set_switch_state(self, switch, state):
        if switch['type'] == 'pwm':
            if state == True:
                state = 100
            if state == False:
                state = 0
            new_value = round(state*2.55)
        else:
            new_value = 1 if state else 0

        if self.rpi.io[switch["output_id"]].value != new_value:
            logger.info(f"Setting {switch['name']} ({switch['output_id']}) to {str(state)}")
            self.rpi.io[switch["output_id"]].value = new_value

    def mqtt_broadcast_switch_availability(self, switch, value):
       logger.debug("Broadcasting MQTT message on topic: " + switch["mqtt_availability_topic"] + ", value: " + value)
       self.mqttclient.publish(switch["mqtt_availability_topic"], payload=value, qos=0, retain=True)

    def mqtt_broadcast_state(self, switch, state):
        logger.debug("Broadcasting MQTT message on topic: " + switch["mqtt_state_topic"] + ", value: " + state)
        self.mqttclient.publish(switch["mqtt_state_topic"], payload=state, qos=0, retain=True)

if __name__ == "__main__":
    mqttLightControl =  MqttLightControl()
    mqttLightControl.start()
