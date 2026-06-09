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
formatter = jsonlogger.JsonFormatter(
    '%(message)%(levelname)',
    timestamp='dt',
    rename_fields={"levelname": "level"},
)
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
    # group_command_topic -> per-attribute state topic. Populated for
    # conflict detection during load_config; the runtime mirror reads
    # the per-switch group_state_topic directly.
    group_state_topic_map = {}
    # group_command_topic -> bare-topic JSON state topic. Same shape and
    # purpose as group_state_topic_map.
    group_json_state_topic_map = {}
    # group_command_topic -> derived z2m group availability topic
    group_availability_topic_map = {}
    # availability topic -> bool. Default False (= not-online = we publish).
    # Populated from retained {"state":"online"|"offline"} on connect.
    z2m_group_online = {}
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
            group_command_topic = switch.get('group_command_topic')
            if group_command_topic:
                self.switch_mqtt_topic_map.setdefault(group_command_topic, []).append(switch)

            group_state_topic = switch.get('group_state_topic')
            if group_state_topic:
                if not group_command_topic:
                    raise SyntaxError("Cannot load configuration: group_state_topic requires group_command_topic")
                if group_command_topic in self.group_state_topic_map and self.group_state_topic_map[group_command_topic] != group_state_topic:
                    raise SyntaxError(f"Cannot load configuration: conflicting group_state_topic values for group_command_topic {group_command_topic}")
                self.group_state_topic_map[group_command_topic] = group_state_topic

            group_json_state_topic = switch.get('group_json_state_topic')
            if group_json_state_topic:
                if not group_command_topic:
                    raise SyntaxError("Cannot load configuration: group_json_state_topic requires group_command_topic")
                if group_command_topic in self.group_json_state_topic_map and self.group_json_state_topic_map[group_command_topic] != group_json_state_topic:
                    raise SyntaxError(f"Cannot load configuration: conflicting group_json_state_topic values for group_command_topic {group_command_topic}")
                self.group_json_state_topic_map[group_command_topic] = group_json_state_topic

            if group_state_topic or group_json_state_topic:
                # z2m publishes group availability as retained
                # {"state":"online"|"offline"} at <group_topic>/availability
                # ("offline" for empty groups). Use it to decide whether we
                # need to publish state ourselves. group_topic is guaranteed
                # set here — load_config derives it from group_command_topic
                # if not explicit.
                avail_topic = switch['group_topic'] + '/availability'
                self.group_availability_topic_map[group_command_topic] = avail_topic
                self.z2m_group_online.setdefault(avail_topic, False)

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
        self.homeassistant_status_topic = '{}/status'.format(self.homeassistant_prefix)

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

            # `group_topic` is the canonical base for all per-group topics
            # (e.g. zigbee2mqtt/<area>/lights); the rest derive from it if
            # not explicitly set. For backwards compat with configs that
            # only set `group_command_topic`, reverse-derive `group_topic`
            # by stripping a trailing /set — but don't auto-fill the state
            # topics, since the legacy semantics treat those as explicit
            # opt-in.
            explicit_group_topic = switch.get('group_topic')
            if explicit_group_topic:
                switch.setdefault('group_command_topic', explicit_group_topic + '/set')
                switch.setdefault('group_json_state_topic', explicit_group_topic)
                switch.setdefault('group_state_topic', explicit_group_topic + '/state')
            elif switch.get('group_command_topic'):
                cmd = switch['group_command_topic']
                switch['group_topic'] = cmd[:-len('/set')] if cmd.endswith('/set') else cmd


    def configure_mqtt_for_switch(self, switch):
        switch_configuration = {
            "name": switch["name"],
            "command_topic": switch["mqtt_command_topic"],
            "schema": "template",
            "command_on_template": "on",
            "command_off_template": "off",
            "state_topic": switch["mqtt_state_topic"],
            "state_template": "{{ value_json.state }}",
            "availability": [
                {'topic': self.availability_topic, 'value_template': '{{ value_json.state }}'},
                {'topic': switch["mqtt_availability_topic"], 'value_template': '{{ value_json.state }}'},
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

        #Subscribe to z2m group availability topics so we can defer to z2m
        #when its groups have online members.
        for avail_topic in set(self.group_availability_topic_map.values()):
            self.mqttclient.subscribe(avail_topic)

        #Re-announce discovery when Home Assistant restarts (birth message).
        self.mqttclient.subscribe(self.homeassistant_status_topic)

        self.mqttclient.publish(self.availability_topic, payload='{"state": "online"}', qos=0, retain=True)
        self.mqttclient.will_set(self.availability_topic, payload='{"state": "offline"}', qos=0, retain=True)

    def mqtt_on_message(self, client, userdata, msg):
        payload = msg.payload.decode('utf-8').strip()
        logger.debug("Received MQTT message on topic: " + msg.topic + ", payload: " + payload + ", retained: " + str(msg.retain))

        # Re-announce discovery when Home Assistant restarts (birth message).
        if msg.topic == self.homeassistant_status_topic:
            if payload == 'online':
                logger.info("Home Assistant online — re-announcing discovery configs")
                for switch in self.switches:
                    self.configure_mqtt_for_switch(switch)
            return

        # Availability tracking branch: z2m publishes group availability at
        # <group_root>/availability as retained JSON {"state":"online"|"offline"}.
        # Anything other than positively-online (missing, parse error, "offline")
        # is treated as not-online so we'll publish state ourselves.
        if msg.topic in self.z2m_group_online:
            try:
                self.z2m_group_online[msg.topic] = json.loads(payload).get('state') == 'online'
            except (json.decoder.JSONDecodeError, AttributeError):
                self.z2m_group_online[msg.topic] = False
            logger.debug(f"z2m group availability on {msg.topic}: {'online' if self.z2m_group_online[msg.topic] else 'not-online'}")
            return

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
                payload_state = None
            try:
                payload_brightness = payload_json['brightness']
            except KeyError:
                pass
        else:
            payload_state = payload.lower()

        if payload_brightness is None and payload_state is None:
            logger.error(f'Could not find state or brightness in payload: {payload}')
            return

        broadcast_state = None
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

            # Mirror to the z2m-shaped group topics for any state change,
            # not just commands that arrived on group_command_topic. HA's
            # all-off / per-entity service calls land on mqtt_command_topic
            # (the per-relay set); without this mirror, downstream listeners
            # of group_state_topic see stale state until the next button
            # press goes through group_command_topic.
            group_state_topic = s.get('group_state_topic')
            group_json_state_topic = s.get('group_json_state_topic')
            if not (group_state_topic or group_json_state_topic):
                continue
            avail_topic = self.group_availability_topic_map.get(s.get('group_command_topic'))
            # Defer to z2m only when its group availability is positively "online".
            # Missing/offline/parse-error → publish ourselves.
            if avail_topic and self.z2m_group_online.get(avail_topic, False):
                continue
            if group_state_topic:
                self.mqttclient.publish(
                    group_state_topic,
                    payload=broadcast_state.upper(),
                    qos=0,
                    retain=False,
                )
            if group_json_state_topic:
                self.mqttclient.publish(
                    group_json_state_topic,
                    payload=json.dumps({'state': broadcast_state.upper()}),
                    qos=0,
                    retain=False,
                )

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
