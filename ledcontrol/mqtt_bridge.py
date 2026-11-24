# led-control WS2812B LED Controller Server
# Copyright 2022 jackw01. Released under the MIT License (see LICENSE for details).

# MQTT bridge with Home Assistant MQTT discovery.
#
# Every change made through MQTT goes through AnimationController.update_settings(),
# the same entry point used by the web UI (POST /updatesettings) and the HomeKit
# accessory, so MQTT behaves exactly like the other two.
#
# Topic layout (<prefix> defaults to "ledcontrol/<node_id>"):
#
#   <prefix>/status                        online / offline (retained, last will)
#   <prefix>/settings/set                  raw JSON, same format as POST /updatesettings
#   <prefix>/settings/state                full settings JSON (retained)
#   <prefix>/global/set|state              JSON light: master on/off + global brightness
#   <prefix>/preset/set                    preset name -> applies that preset
#   <prefix>/group/<g>/set|state           JSON light: group brightness, color (hs), pattern (effect)
#   <prefix>/group/<g>/<field>/set|state   plain text; field = speed, scale, start, length, palette

import colorsys
import json
import math
import re
import socket
import threading
import time
import traceback
import zlib

# How often settings are compared with what was last published. This is what keeps
# Home Assistant up to date when settings are changed from the web UI or HomeKit.
SYNC_INTERVAL = 1.0

# Palette ids used to hold colors set over MQTT. Built-in palettes use small ids and
# palettes created in the web UI use millisecond timestamps, so this range is free.
COLOR_PALETTE_BASE = 1000000000
COLOR_PALETTE_SPAN = 1000000000

# Group settings a preset may change (the same fields the web UI saves in a preset)
PRESET_FIELDS = ('brightness', 'color_temp', 'saturation', 'function', 'palette', 'scale', 'speed')

SAFE_ID = re.compile(r'^[A-Za-z0-9_-]+$')

def _safe_id(text):
    return re.sub(r'[^A-Za-z0-9_-]', '_', str(text))

def _clamp(x, lo, hi):
    return max(lo, min(hi, x))

def _number(value, default=None):
    'Parse a finite float, or return default'
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    return v if math.isfinite(v) else default

def _int(value, default=0):
    v = _number(value)
    return default if v is None else int(round(v))

def _fmt(value):
    'Compact number formatting for plain text state topics'
    return ('%.3f' % value).rstrip('0').rstrip('.') or '0'

def _parse_json_command(payload):
    'Parse a Home Assistant JSON light command. Plain ON / OFF is accepted too.'
    text = payload.strip()
    if text.upper() in ('ON', 'OFF'):
        return {'state': text.upper()}
    cmd = json.loads(text)
    if not isinstance(cmd, dict):
        raise ValueError('expected a JSON object')
    return cmd

def _unique_names(pairs):
    'Map ids to display names and back, adding the id to any duplicated name'
    id_to_name = {}
    name_to_id = {}
    for key, name in pairs:
        name = str(name)
        if name in name_to_id:
            name = f'{name} ({key})'
        id_to_name[key] = name
        name_to_id[name] = key
    return id_to_name, name_to_id


class MQTTBridge:
    def __init__(self, controller, functions, presets, led_count, config):
        # Imported here so paho-mqtt is only needed when MQTT is actually enabled
        import paho.mqtt.client as mqtt
        self._mqtt = mqtt

        self._controller = controller
        self._functions = functions  # same dicts that app.py serves to the web UI
        self._presets = presets
        self._led_count = led_count

        self._host = config['host']
        self._port = int(config.get('port') or 1883)
        self._username = config.get('username')
        self._password = config.get('password')
        self._tls = bool(config.get('tls'))
        self._node_id = _safe_id(config.get('node_id') or socket.gethostname())
        self._prefix = (config.get('topic_prefix') or f'ledcontrol/{self._node_id}').strip('/')
        self._discovery_prefix = (config.get('discovery_prefix') or 'homeassistant').strip('/')

        self._device = {
            'identifiers': [f'ledcontrol_{self._node_id}'],
            'name': f'LEDControl {self._node_id}',
            'manufacturer': 'jackw01',
            'model': 'LEDControl',
        }

        self._client = None
        self._thread = None
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._published = {}         # topic -> last payload sent, so only changes are published
        self._remote_configs = set() # discovery topics the broker has for this node
        self._last_brightness = {}   # group -> brightness before it was switched off
        self._last_error = None

    # Topics

    def _topic(self, *parts):
        return '/'.join((self._prefix,) + parts)

    def _config_topic(self, component, object_id):
        return f'{self._discovery_prefix}/{component}/{self._node_id}/{object_id}/config'

    # Connection

    def start(self):
        mqtt = self._mqtt
        client_id = f'ledcontrol-{self._node_id}'
        if hasattr(mqtt, 'CallbackAPIVersion'):  # paho-mqtt 2.x
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        else:  # paho-mqtt 1.x
            client = mqtt.Client(client_id=client_id)
        if self._username:
            client.username_pw_set(self._username, self._password)
        if self._tls:
            client.tls_set()
        client.will_set(self._topic('status'), 'offline', qos=1, retain=True)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        client.reconnect_delay_set(min_delay=1, max_delay=60)
        self._client = client

        # connect_async + loop_start: never blocks startup, and keeps retrying if the
        # broker is unreachable (for example while the network is still coming up)
        client.connect_async(self._host, self._port, keepalive=60)
        client.loop_start()

        self._thread = threading.Thread(target=self._sync_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        client = self._client
        if client is None:
            return
        try:
            if client.is_connected():
                info = client.publish(self._topic('status'), 'offline', qos=1, retain=True)
                try:
                    info.wait_for_publish(1)
                except Exception:
                    time.sleep(0.2)
            client.disconnect()
            client.loop_stop()
        except Exception:
            pass

    # args are version-tolerant: paho 1.x passes (client, userdata, flags, rc),
    # paho 2.x passes (client, userdata, flags, reason_code, properties)
    def _on_connect(self, client, userdata, flags, rc, *args):
        if rc != 0:
            print(f'MQTT connection to {self._host}:{self._port} refused ({rc})')
            return
        print(f'MQTT connected to {self._host}:{self._port}, topic prefix "{self._prefix}"')
        with self._lock:
            self._published.clear()  # a fresh connection must resend everything
        client.subscribe([
            (self._topic('settings', 'set'), 1),
            (self._topic('global', 'set'), 1),
            (self._topic('preset', 'set'), 1),
            (self._topic('group', '+', 'set'), 1),
            (self._topic('group', '+', '+', 'set'), 1),
            (f'{self._discovery_prefix}/status', 1),  # Home Assistant birth message
            (f'{self._discovery_prefix}/+/{self._node_id}/+/config', 1),  # to clean up removed groups
        ])
        client.publish(self._topic('status'), 'online', qos=1, retain=True)
        self._run_sync()

    def _on_disconnect(self, client, userdata, *args):
        if not self._stop_event.is_set():
            print('MQTT disconnected, will keep retrying')

    def _on_message(self, client, userdata, msg):
        try:
            topic = msg.topic
            payload = msg.payload.decode('utf-8', 'replace').strip()
            if topic == f'{self._discovery_prefix}/status':
                if payload == 'online':  # Home Assistant restarted, send everything again
                    with self._lock:
                        self._published.clear()
            elif topic.startswith(self._discovery_prefix + '/'):
                with self._lock:
                    if payload:
                        self._remote_configs.add(topic)
                    else:
                        self._remote_configs.discard(topic)
            elif topic.startswith(self._prefix + '/'):
                self._handle_command(topic[len(self._prefix) + 1:], payload)
        except Exception:
            traceback.print_exc()

    # Commands (Home Assistant / MQTT -> controller)

    def _handle_command(self, rel_topic, payload):
        parts = rel_topic.split('/')
        if parts[-1] != 'set':
            return  # not a command, for example one of our own retained state topics
        with self._lock:
            try:
                if parts == ['settings', 'set']:
                    self._cmd_settings(payload)
                elif parts == ['global', 'set']:
                    self._cmd_global(payload)
                elif parts == ['preset', 'set']:
                    self._cmd_preset(payload)
                elif len(parts) == 3 and parts[0] == 'group' and parts[2] == 'set':
                    self._cmd_group_light(parts[1], payload)
                elif len(parts) == 4 and parts[0] == 'group' and parts[3] == 'set':
                    self._cmd_group_field(parts[1], parts[2], payload)
                else:
                    print(f'MQTT: ignoring message on unknown topic {rel_topic}')
                    return
            except ValueError as e:  # includes JSON decode errors
                print(f'MQTT: bad payload on {rel_topic}: {e}')
                return
            self._run_sync()  # publish the new state right away

    def _cmd_settings(self, payload):
        delta = json.loads(payload)
        if not isinstance(delta, dict):
            raise ValueError('settings payload must be a JSON object')
        # Same as POST /updatesettings: not validated, so keep values well formed
        self._controller.update_settings(delta)

    def _brightness_limit(self, settings):
        limit = _number(settings.get('global_brightness_limit'), 1.0)
        return limit if limit > 0 else 1.0

    def _cmd_global(self, payload):
        cmd = _parse_json_command(payload)
        settings = self._snapshot()
        limit = self._brightness_limit(settings)
        state = str(cmd.get('state', '')).upper()
        delta = {}

        if state == 'OFF':
            # Only the on/off flag changes, so the brightness comes back when switched on again
            delta['on'] = 0
        else:
            if state == 'ON':
                delta['on'] = 1
            b = _number(cmd.get('brightness'))
            if b is not None:
                # Home Assistant's 0-255 maps onto 0 to the configured brightness limit.
                # (update_settings() also switches the LEDs on for any brightness above 0.)
                delta['global_brightness'] = _clamp(b, 0, 255) / 255.0 * limit
            elif state == 'ON' and _number(settings.get('global_brightness'), 0) <= 0:
                delta['global_brightness'] = limit

        if delta:
            self._controller.update_settings(delta)

    def _cmd_preset(self, name):
        preset = self._presets.get(name)
        if not isinstance(preset, dict):
            print(f'MQTT: unknown preset "{name}"')
            return

        # This is Store.loadPreset() from the web UI, done on the server
        groups = self._snapshot()['groups']
        palettes = self._controller.get_palettes()
        delta = {}
        for g, values in list(preset.items()):
            if g not in groups or not isinstance(values, dict):
                continue
            d = {}
            for k in PRESET_FIELDS:
                if k not in values:
                    continue
                if k in ('function', 'palette'):
                    d[k] = _int(values[k], 0)
                else:
                    v = _number(values[k])
                    if v is not None:
                        d[k] = v
            # Presets can refer to patterns or palettes that have since been deleted
            if 'function' in d and d['function'] not in self._functions:
                d['function'] = 0
            if 'palette' in d and d['palette'] not in palettes:
                d['palette'] = 0
            delta[g] = d
        if delta:
            self._controller.update_settings({'groups': delta})

    def _cmd_group_light(self, g, payload):
        group = self._snapshot()['groups'].get(g)
        if group is None:
            print(f'MQTT: unknown group "{g}"')
            return
        cmd = _parse_json_command(payload)
        state = str(cmd.get('state', '')).upper()
        current = _number(group.get('brightness'), 1.0)
        delta = {}

        b = _number(cmd.get('brightness'))
        if b is not None:
            delta['brightness'] = _clamp(b, 0, 255) / 255.0
        if state == 'OFF':
            if current > 0:
                self._last_brightness[g] = current
            delta['brightness'] = 0.0
        elif state == 'ON' and 'brightness' not in delta and current <= 0:
            delta['brightness'] = self._last_brightness.get(g, 1.0)

        effect = cmd.get('effect')
        if effect is not None:
            _, name_to_id = self._function_names()
            if effect in name_to_id:
                delta['function'] = name_to_id[effect]
            else:
                print(f'MQTT: unknown pattern "{effect}"')

        color = cmd.get('color')
        if isinstance(color, dict):
            hs = self._parse_hs(color)
            if hs is not None:
                delta['palette'] = self._set_color_palette(g, group, hs[0], hs[1])
                # A color only shows with a pattern that uses the palette, so unless a
                # pattern was requested too, go back to "Static Color"
                if 'function' not in delta and group.get('function') != 0 and 0 in self._functions:
                    delta['function'] = 0

        if delta:
            self._controller.update_settings({'groups': {g: delta}})

    def _cmd_group_field(self, g, field, payload):
        group = self._snapshot()['groups'].get(g)
        if group is None:
            print(f'MQTT: unknown group "{g}"')
            return

        if field == 'palette':
            _, name_to_id = self._palette_names()
            if payload not in name_to_id:
                print(f'MQTT: unknown palette "{payload}"')
                return
            self._controller.update_settings({'groups': {g: {'palette': name_to_id[payload]}}})
            return

        v = _number(payload)
        if v is None:
            raise ValueError(f'"{payload}" is not a number')

        count = self._led_count
        start = _int(group.get('range_start'), 0)
        end = min(count, _int(group.get('range_end'), count))
        if field == 'speed':
            delta = {'speed': _clamp(v, 0, 2)}
        elif field == 'scale':
            delta = {'scale': _clamp(v, -10, 10)}
        elif field == 'start':
            # Like the web UI: only the start changes (but it must stay before the end)
            delta = {'range_start': int(_clamp(round(v), 0, max(0, end - 1)))}
        elif field == 'length':
            # Length sets the end of the range relative to the current start
            length = int(_clamp(round(v), 1, max(1, count - start)))
            delta = {'range_end': start + length}
        else:
            print(f'MQTT: unknown group setting "{field}"')
            return
        self._controller.update_settings({'groups': {g: delta}})

    def _parse_hs(self, color):
        'Returns (hue, saturation) in the 0-1 range from an {h, s} or {r, g, b} color, or None'
        if 'h' in color and 's' in color:
            h, s = _number(color['h']), _number(color['s'])
            if h is None or s is None:
                return None
            return (h % 360.0) / 360.0, _clamp(s, 0, 100) / 100.0
        if all(k in color for k in ('r', 'g', 'b')):
            rgb = [_number(color[k]) for k in ('r', 'g', 'b')]
            if None in rgb:
                return None
            h, s, _ = colorsys.rgb_to_hsv(*[_clamp(c, 0, 255) / 255.0 for c in rgb])
            return h, s
        return None

    def _set_color_palette(self, group_key, group, h, s):
        '''A group's color comes from its palette, so a color set over MQTT is stored as a
        two-color palette (a palette needs at least two) at an id derived from the group.
        Brightness is left to the group/global brightness settings, so value is always 1.'''
        key = COLOR_PALETTE_BASE + zlib.crc32(group_key.encode('utf-8')) % COLOR_PALETTE_SPAN
        self._controller.set_palette(key, {
            'name': f'MQTT Color ({group.get("name") or group_key})',
            'default': False,
            'colors': [[h, s, 1.0], [h, s, 1.0]],
        })
        self._controller.calculate_palette_table(key)
        return key

    # State (controller -> MQTT)

    def _snapshot(self):
        'Copy of the settings, safe to read while other threads change them'
        for _ in range(5):
            try:
                return json.loads(json.dumps(self._controller.get_settings()))
            except RuntimeError:  # dictionary changed size during iteration
                time.sleep(0.01)
        raise RuntimeError('could not read settings')

    def _function_names(self):
        items = sorted(list(self._functions.items()), key=lambda kv: _int(kv[0], 0))
        return _unique_names((k, v.get('name', k)) for k, v in items)

    def _palette_names(self):
        items = sorted(list(self._controller.get_palettes().items()), key=lambda kv: _int(kv[0], 0))
        return _unique_names((k, v.get('name', k)) for k, v in items)

    def _entity(self, component, object_id, name, config, states):
        cfg = dict(config)
        cfg.update({
            'name': name,
            'unique_id': f'{self._node_id}_{object_id}',
            'availability_topic': self._topic('status'),
            'payload_available': 'online',
            'payload_not_available': 'offline',
            'device': self._device,
        })
        return {'component': component, 'object_id': object_id, 'config': cfg, 'states': states}

    def _build_entities(self, settings):
        entities = []
        effects, _ = self._function_names()
        palette_names, _ = self._palette_names()
        palettes = self._controller.get_palettes()
        count = self._led_count
        limit = self._brightness_limit(settings)

        # Master light: the same controls HomeKit has
        gb = _number(settings.get('global_brightness'), 0.0)
        entities.append(self._entity('light', 'master', 'Master', {
            'schema': 'json',
            'command_topic': self._topic('global', 'set'),
            'state_topic': self._topic('global', 'state'),
            'supported_color_modes': ['brightness'],
        }, {
            self._topic('global', 'state'): json.dumps({
                'state': 'ON' if settings.get('on') else 'OFF',
                'color_mode': 'brightness',
                'brightness': int(round(_clamp(gb / limit, 0, 1) * 255)),
            }, sort_keys=True),
        }))

        entities.append(self._entity('select', 'preset', 'Preset', {
            'command_topic': self._topic('preset', 'set'),
            'options': sorted(str(k) for k in list(self._presets)),
            # no state topic: the controller doesn't track which preset is "current"
        }, {}))

        for g, group in settings.get('groups', {}).items():
            if not SAFE_ID.match(g):
                continue  # can't be used in a topic
            gname = group.get('name') or g
            brightness = _clamp(_number(group.get('brightness'), 1.0), 0, 1)
            start = _int(group.get('range_start'), 0)
            end = min(count, _int(group.get('range_end'), count))

            # Color shown for the group is the first color of its palette: that is what
            # the "Static Color" pattern displays
            h, s = 0.0, 0.0
            palette = palettes.get(_int(group.get('palette'), 0))
            if palette and palette.get('colors'):
                h, s = _number(palette['colors'][0][0], 0.0), _number(palette['colors'][0][1], 0.0)
            light_state = {
                'state': 'ON' if brightness > 0 else 'OFF',
                'color_mode': 'hs',
                'brightness': int(round(brightness * 255)),
                'color': {'h': round((h % 1.0) * 360, 2), 's': round(_clamp(s, 0, 1) * 100, 2)},
            }
            effect = effects.get(_int(group.get('function'), 0))
            if effect is not None:
                light_state['effect'] = effect

            def topic(*parts):
                return self._topic('group', g, *parts)

            entities.append(self._entity('light', g, gname, {
                'schema': 'json',
                'command_topic': topic('set'),
                'state_topic': topic('state'),
                'supported_color_modes': ['hs'],
                'effect': True,
                'effect_list': list(effects.values()),
            }, {topic('state'): json.dumps(light_state, sort_keys=True)}))

            numbers = [
                ('speed', 'speed', {'min': 0, 'max': 2, 'step': 0.01, 'mode': 'slider',
                                    'unit_of_measurement': 'Hz'},
                 _fmt(_number(group.get('speed'), 0.0))),
                ('scale', 'scale', {'min': -10, 'max': 10, 'step': 0.01, 'mode': 'slider'},
                 _fmt(_number(group.get('scale'), 1.0))),
                ('start', 'first LED', {'min': 0, 'max': max(0, count - 1), 'step': 1, 'mode': 'box'},
                 str(start)),
                ('length', 'LED count', {'min': 1, 'max': max(1, count), 'step': 1, 'mode': 'box'},
                 str(max(0, end - start))),
            ]
            for field, label, cfg, value in numbers:
                cfg = dict(cfg)
                cfg['command_topic'] = topic(field, 'set')
                cfg['state_topic'] = topic(field, 'state')
                entities.append(self._entity('number', f'{g}_{field}', f'{gname} {label}', cfg,
                                             {topic(field, 'state'): value}))

            entities.append(self._entity('select', f'{g}_palette', f'{gname} palette', {
                'command_topic': topic('palette', 'set'),
                'state_topic': topic('palette', 'state'),
                'options': list(palette_names.values()),
            }, {topic('palette', 'state'): palette_names.get(_int(group.get('palette'), 0), '')}))

        return entities

    def _publish(self, topic, payload, qos=0, retain=True):
        if self._published.get(topic) == payload:
            return
        self._client.publish(topic, payload, qos=qos, retain=retain)
        self._published[topic] = payload

    def _sync(self):
        'Publish discovery configs and states, but only the ones that changed'
        with self._lock:
            client = self._client
            if client is None or not client.is_connected():
                return
            settings = self._snapshot()
            entities = self._build_entities(settings)

            current = set()
            for e in entities:
                topic = self._config_topic(e['component'], e['object_id'])
                current.add(topic)
                self._publish(topic, json.dumps(e['config'], sort_keys=True), qos=1)
                for state_topic, payload in e['states'].items():
                    self._publish(state_topic, payload)
            self._publish(self._topic('settings', 'state'), json.dumps(settings, sort_keys=True))

            # Remove Home Assistant entities for groups that were deleted
            for topic in list(self._remote_configs - current):
                client.publish(topic, '', qos=1, retain=True)
                self._remote_configs.discard(topic)
                self._published.pop(topic, None)

    def _run_sync(self):
        try:
            self._sync()
            self._last_error = None
        except Exception as e:
            msg = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
            if msg != self._last_error:  # don't repeat the same error every second
                self._last_error = msg
                print(f'MQTT: error while publishing state:\n{msg}')

    def _sync_loop(self):
        while not self._stop_event.wait(SYNC_INTERVAL):
            self._run_sync()
