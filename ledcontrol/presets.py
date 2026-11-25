# led-control WS2812B LED Controller Server
# Copyright 2022 jackw01. Released under the MIT License (see LICENSE for details).

# Loading saved presets on the server. The web UI does this in the browser
# (Store.loadPreset() in static/js/Store.js); MQTT and the rotary encoder need the
# same behavior, so it lives here.

import math

# Group settings a preset may change (the same fields the web UI saves in a preset)
PRESET_FIELDS = ('brightness', 'color_temp', 'saturation', 'function', 'palette', 'scale', 'speed')

def _number(value):
    'Parse a finite float, or return None'
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None

def _int(value, default=0):
    v = _number(value)
    return default if v is None else int(round(v))

def preset_names(presets):
    '''Preset names in the order the web UI lists them (the API sorts keys)'''
    return sorted(str(k) for k in list(presets))

def apply_preset(controller, functions, presets, name):
    '''Apply the preset called name. Returns False if there is no such preset.
    Same as Store.loadPreset() in the web UI: patterns or palettes that have since been
    deleted fall back to the first one, and groups that no longer exist are skipped.'''
    preset = presets.get(name)
    if not isinstance(preset, dict):
        return False

    groups = list(controller.get_settings()['groups'])
    palettes = controller.get_palettes()
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
        if 'function' in d and d['function'] not in functions:
            d['function'] = 0
        if 'palette' in d and d['palette'] not in palettes:
            d['palette'] = 0
        delta[g] = d
    if delta:
        controller.update_settings({'groups': delta})
    return True
