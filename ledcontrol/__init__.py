# led-control WS2812B LED Controller Server
# Copyright 2022 jackw01. Released under the MIT License (see LICENSE for details).

import argparse
import os
from ledcontrol.app import create_app

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=80,
                        help='Port to use for web interface. Default: 80')
    parser.add_argument('--host', default='0.0.0.0',
                        help='Hostname to use for web interface. Default: 0.0.0.0')
    parser.add_argument('--led_count', type=int, default=0,
                        help='Number of LEDs')
    parser.add_argument('--config_file',
                        help='Location of config file. Default: /etc/ledcontrol.json')
    parser.add_argument('--pixel_mapping_json', type=argparse.FileType('r'),
                        help='JSON file containing pixel mapping (see README)')
    parser.add_argument('--fps', type=int, default=60,
                        help='Refresh rate limit for LEDs, in FPS. Default: 60')
    parser.add_argument('--led_pin', type=int, default=18,
                        help='Pin for LEDs (see https://github.com/jgarff/rpi_ws281x). Default: 18')
    parser.add_argument('--led_data_rate', type=int, default=800000,
                        help='Data rate for LEDs. Default: 800000 Hz')
    parser.add_argument('--led_dma_channel', type=int, default=10,
                        help='DMA channel for LEDs. DO NOT USE CHANNEL 5 ON Pi 3 B. Default: 10')
    parser.add_argument('--led_pixel_order', default='GRB',
                        help='LED color channel order. Any combination of RGB with or without a W at the end. Default: GRB, try GRBW for SK6812')
    parser.add_argument('--led_brightness_limit', type=float, default=1.0,
                        help='LED maximum brightness limit for the web UI. Float from 0.0-1.0. Default: 1.0')
    parser.add_argument('--save_interval', type=int, default=60,
                        help='Interval for automatically saving settings in seconds. Default: 60')
    parser.add_argument('--sacn', action='store_true',
                        help='Enable sACN / E1.31 support. Default: False')
    parser.add_argument('--hap', action='store_true',
                        help='Enable HomeKit Accessory Protocol support. Default: False')
    parser.add_argument('--mqtt_host',
                        help='Enable MQTT / Home Assistant support and connect to the broker at this host')
    parser.add_argument('--mqtt_port', type=int, default=1883,
                        help='MQTT broker port. Default: 1883')
    parser.add_argument('--mqtt_user',
                        help='MQTT username. Can also be set with the LEDCONTROL_MQTT_USER environment variable')
    parser.add_argument('--mqtt_password',
                        help='MQTT password. Prefer the LEDCONTROL_MQTT_PASSWORD environment variable, '
                             'since command line arguments are visible to other users')
    parser.add_argument('--mqtt_tls', action='store_true',
                        help='Use TLS to connect to the MQTT broker. Default: False')
    parser.add_argument('--mqtt_node_id',
                        help='Name used to identify this device in MQTT and Home Assistant. Default: hostname')
    parser.add_argument('--mqtt_topic_prefix',
                        help='MQTT topic prefix. Default: ledcontrol/<node id>')
    parser.add_argument('--mqtt_discovery_prefix', default='homeassistant',
                        help='Home Assistant MQTT discovery prefix. Default: homeassistant')
    parser.add_argument('--no_timer_reset', action='store_true',
                        help='Do not reset the animation timer when patterns are changed. Default: False')
    parser.add_argument('--dev', action='store_true',
                        help='Development flag. Default: False')
    args = parser.parse_args()

    mqtt_config = None
    if args.mqtt_host:
        mqtt_config = {
            'host': args.mqtt_host,
            'port': args.mqtt_port,
            'username': args.mqtt_user or os.environ.get('LEDCONTROL_MQTT_USER'),
            'password': args.mqtt_password or os.environ.get('LEDCONTROL_MQTT_PASSWORD'),
            'tls': args.mqtt_tls,
            'node_id': args.mqtt_node_id,
            'topic_prefix': args.mqtt_topic_prefix,
            'discovery_prefix': args.mqtt_discovery_prefix,
        }

    app = create_app(args.led_count,
                     args.config_file,
                     args.pixel_mapping_json,
                     args.fps,
                     args.led_pin,
                     args.led_data_rate,
                     args.led_dma_channel,
                     args.led_pixel_order.upper(),
                     args.led_brightness_limit,
                     args.save_interval,
                     args.sacn,
                     args.hap,
                     args.no_timer_reset,
                     args.dev,
                     mqtt_config)

    if args.dev:
        app.run(host=args.host, port=args.port)
    else:
        import bjoern
        bjoern.run(app, host=args.host, port=args.port)
