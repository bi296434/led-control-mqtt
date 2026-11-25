# led-control WS2812B LED Controller Server
# Copyright 2022 jackw01. Released under the MIT License (see LICENSE for details).

# Rotary encoder with push switch (for example a KY-040) on the Raspberry Pi GPIO header.
#
#   turn clockwise          raise global brightness
#   turn counterclockwise   lower global brightness
#   press                   load the next preset (in the order the web UI lists them)
#
# Changes go through AnimationController.update_settings(), like the web UI, HomeKit and MQTT.
# GPIO callbacks only queue events: a worker thread applies them, so the GPIO thread never
# waits on the LED controller and several quick clicks become a single update.

import queue
import threading
import time
import traceback

from ledcontrol.presets import apply_preset, preset_names

# Quadrature decoding. The two signals go through four states per cycle (Gray code). Turning
# clockwise, CLK falls first: (DT, CLK) = 11 -> 10 -> 00 -> 01 -> 11. This maps each pin
# state (DT << 1 | CLK) to its position in that cycle. The decoder adds up single steps
# forward and backward and reports a click once enough of them are in one direction, so
# bounce that goes back and forth cancels out, and an impossible jump (a missed edge)
# starts the count again instead of guessing.
_POSITION = {3: 0, 2: 1, 0: 2, 1: 3}

# Another press within this time of the previous falling edge is switch bounce
BUTTON_LOCKOUT = 0.15
# How long queued events are gathered before being applied. Clicks in the same window are added together.
WORKER_INTERVAL = 0.03
POLL_INTERVAL = 0.001


class RotaryEncoder:
    def __init__(self, controller, functions, presets, config):
        self._controller = controller
        self._functions = functions
        self._presets = presets

        self._clk = int(config['clk_pin'])
        self._dt = int(config['dt_pin'])
        self._sw = int(config['sw_pin'])
        self._step = float(config.get('step', 0.02))  # fraction of the brightness limit per click
        self._reverse = bool(config.get('reverse'))
        self._edges = int(config.get('edges', 4))  # pin changes per click: 4 for most, 2 for some
        if len({self._clk, self._dt, self._sw}) != 3:
            raise ValueError('encoder clk, dt and switch must be three different pins')
        if not 0 < self._step <= 1:
            raise ValueError('encoder step must be greater than 0 and at most 1')
        if self._edges not in (1, 2, 4):
            raise ValueError('encoder edges per click must be 1, 2 or 4')

        self._gpio = None
        self._position = 0
        self._accumulated = 0
        self._last_press = 0.0
        self._events = queue.Queue()
        self._stop_event = threading.Event()
        self._last_preset = None

    # Setup

    def start(self):
        import RPi.GPIO as GPIO  # only needed when the encoder is enabled
        self._gpio = GPIO
        if GPIO.getmode() is None:
            GPIO.setmode(GPIO.BCM)
        elif GPIO.getmode() != GPIO.BCM:
            raise RuntimeError('GPIO is already set up with board pin numbering, expected BCM')
        GPIO.setwarnings(False)
        # Internal pull-ups: the encoder common and the switch go to ground
        for pin in (self._clk, self._dt, self._sw):
            GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
        self._position = _POSITION[self._read_pins()]
        self._accumulated = 0

        try:
            GPIO.add_event_detect(self._clk, GPIO.BOTH, callback=self._on_edge)
            GPIO.add_event_detect(self._dt, GPIO.BOTH, callback=self._on_edge)
            GPIO.add_event_detect(self._sw, GPIO.FALLING, callback=self._on_button)
        except RuntimeError as e:
            # Some RPi.GPIO / kernel combinations can't do edge detection. Fall back to polling.
            print(f'Encoder: edge detection unavailable ({e}), polling instead')
            for pin in (self._clk, self._dt, self._sw):
                try:
                    GPIO.remove_event_detect(pin)
                except Exception:
                    pass
            self._spawn(self._poll_loop)

        self._spawn(self._worker_loop)
        print(f'Encoder ready: clk=GPIO{self._clk} dt=GPIO{self._dt} switch=GPIO{self._sw}')

    def stop(self):
        self._stop_event.set()
        GPIO = self._gpio
        if GPIO is None:
            return
        for pin in (self._clk, self._dt, self._sw):
            try:
                GPIO.remove_event_detect(pin)
            except Exception:
                pass
        try:
            GPIO.cleanup([self._clk, self._dt, self._sw])
        except Exception:
            pass

    def _spawn(self, target):
        threading.Thread(target=target, daemon=True).start()

    def _read_pins(self):
        return (self._gpio.input(self._dt) << 1) | self._gpio.input(self._clk)

    # Input (GPIO thread: keep it short)

    def _on_edge(self, channel=None):
        self._feed(self._read_pins())

    def _feed(self, pins):
        'Advance the decoder with a new pin state, and queue a click when enough steps add up'
        position = _POSITION[pins]
        step = (position - self._position) % 4
        self._position = position
        if step == 0:
            return
        if step == 2:  # skipped a state, so the direction is unknown
            self._accumulated = 0
            return
        self._accumulated += 1 if step == 1 else -1
        if abs(self._accumulated) >= self._edges:
            clockwise = self._accumulated > 0
            self._accumulated = 0
            self._events.put(1 if clockwise != self._reverse else -1)

    def _on_button(self, channel=None):
        self._feed_button(time.monotonic())

    def _feed_button(self, now):
        # Updated on every edge, so chatter keeps extending the lockout until it stops
        previous, self._last_press = self._last_press, now
        if now - previous >= BUTTON_LOCKOUT:
            self._events.put('press')

    def _poll_loop(self):
        last_pins = self._read_pins()
        last_sw = self._gpio.input(self._sw)
        while not self._stop_event.wait(POLL_INTERVAL):
            pins = self._read_pins()
            if pins != last_pins:
                last_pins = pins
                self._feed(pins)
            sw = self._gpio.input(self._sw)
            if sw == 0 and last_sw == 1:
                self._feed_button(time.monotonic())
            last_sw = sw

    # Actions (worker thread)

    def _worker_loop(self):
        while not self._stop_event.is_set():
            try:
                batch = [self._events.get(timeout=0.25)]
            except queue.Empty:
                continue
            # Gather whatever arrives in the next moment
            deadline = time.monotonic() + WORKER_INTERVAL
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    batch.append(self._events.get(timeout=remaining))
                except queue.Empty:
                    break
            self.handle_events(batch)

    def handle_events(self, events):
        'Apply events in order. Consecutive clicks are added together.'
        clicks = 0
        for event in events:
            if event == 'press':
                self._apply_clicks(clicks)
                clicks = 0
                self._next_preset()
            else:
                clicks += event
        self._apply_clicks(clicks)

    def _apply_clicks(self, clicks):
        if clicks == 0:
            return
        try:
            settings = self._controller.get_settings()
            limit = float(settings.get('global_brightness_limit') or 1.0)
            current = float(settings.get('global_brightness') or 0.0)
            is_on = bool(settings.get('on'))
            if not is_on and clicks < 0:
                return  # any brightness above 0 switches the LEDs on, so don't turn down while off
            new = round(min(limit, max(0.0, current + clicks * self._step * limit)), 4)
            delta = {'global_brightness': new}
            if not is_on:
                delta['on'] = 1
            if new != current or not is_on:
                self._controller.update_settings(delta)
        except Exception:
            traceback.print_exc()

    def _next_preset(self):
        try:
            names = preset_names(self._presets)
            if not names:
                print('Encoder: no presets saved yet, save one in the web interface first')
                return
            if self._last_preset in names:
                name = names[(names.index(self._last_preset) + 1) % len(names)]
            elif self._last_preset is not None:
                # The last preset was deleted: continue with the first name that sorts after it
                after = [n for n in names if n > self._last_preset]
                name = after[0] if after else names[0]
            else:
                name = names[0]
            self._last_preset = name
            if apply_preset(self._controller, self._functions, self._presets, name):
                print(f'Encoder: preset "{name}"')
        except Exception:
            traceback.print_exc()
