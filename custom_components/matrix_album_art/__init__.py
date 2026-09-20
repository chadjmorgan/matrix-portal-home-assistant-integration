"""Companion integration to dynamically track an active media player from MQTT."""
from __future__ import annotations

import io
import logging
import asyncio
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall, Event, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_track_state_change_event
from homeassistant.components import mqtt as ha_mqtt

import aiohttp
from PIL import Image, ImageEnhance  # Added ImageEnhance
import paho.mqtt.publish as publish
import paho.mqtt.client as paho_client

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

TRACKER_DATA_KEY = f"{DOMAIN}_unsub_listener"

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up the integration from a UI config entry."""
    
    mqtt_broker = entry.options.get("mqtt_broker", entry.data.get("mqtt_broker"))
    mqtt_topic = entry.options.get("mqtt_topic", entry.data.get("mqtt_topic", "appletv/matrix/album_art"))
    control_topic = "appletv/matrix/active_player"

    # Grab the ImageEnhance multipliers dynamically from user settings
    val_saturation = entry.options.get("color_saturation", entry.data.get("color_saturation", 1.0))
    val_contrast = entry.options.get("contrast", entry.data.get("contrast", 1.0))
    val_brightness = entry.options.get("brightness", entry.data.get("brightness", 1.0))

    async def async_process_and_send(image_path: str):
        """Processes the extracted image path, resizes it, and sends it via MQTT."""
        if not image_path:
            return

        if "cache=" in image_path:
            full_url = image_path.split("cache=")[-1]
        elif image_path.startswith("http"):
            full_url = image_path
        else:
            base_url = hass.config.internal_url or "http://localhost:8123"
            full_url = f"{base_url.rstrip('/')}/{image_path.lstrip('/')}"
            
        if "{w}" in full_url or "{h}" in full_url:
            full_url = full_url.replace("{w}", "32").replace("{h}", "32").replace("{c}", "").replace("{f}", "png")

        _LOGGER.debug(f"Matrix pushing download request: {full_url}")
        session = async_get_clientsession(hass, verify_ssl=False)
        headers = {"X-HA-Internal-Request": "1"} if "127.0.0.1" in full_url or "localhost" in full_url or "api/media_player_proxy" in full_url else {}

        try:
            async with asyncio.timeout(10):
                async with session.get(full_url, headers=headers) as response:
                    if response.status != 200:
                        return
                    image_bytes = await response.read()

            def process_image() -> bytes:
                img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
                
                # Apply the UI multipliers dynamically
                if val_saturation != 1.0:
                    img = ImageEnhance.Color(img).enhance(val_saturation)
                if val_contrast != 1.0:
                    img = ImageEnhance.Contrast(img).enhance(val_contrast)
                if val_brightness != 1.0:
                    img = ImageEnhance.Brightness(img).enhance(val_brightness)

                # Resize to target output matrix constraints
                img = img.resize((32, 32), Image.Resampling.NEAREST)
                
                rgb565_bytes = bytearray()
                for y in range(32):
                    for x in range(32):
                        r, g, b = img.getpixel((x, y))
                        r_5bit = int((r * 31) / 255)
                        g_6bit = int((g * 63) / 255)
                        b_5bit = int((b * 31) / 255)
                        rgb565 = (r_5bit << 11) | (g_6bit << 5) | b_5bit
                        rgb565_bytes.append((rgb565 >> 8) & 0xFF)
                        rgb565_bytes.append(rgb565 & 0xFF)
                return bytes(rgb565_bytes)

            payload_bytes = await hass.async_add_executor_job(process_image)

            def mqtt_publish_worker():
                publish.single(
                    topic=mqtt_topic, payload=payload_bytes, qos=0, retain=False,
                    hostname=mqtt_broker, port=1883, protocol=paho_client.MQTTv311
                )

            await hass.async_add_executor_job(mqtt_publish_worker)
            _LOGGER.warning(f"Matrix Stream Success! {len(payload_bytes)} bytes sent.")

        except Exception as e:
            _LOGGER.error(f"Matrix engine error: {e}")

    @callback
    def async_bind_player_listener(entity_id: str):
        """Binds a native event state change listener to a single chosen media player."""
        if TRACKER_DATA_KEY in hass.data.get(entry.entry_id, {}):
            hass.data[entry.entry_id][TRACKER_DATA_KEY]()
            
        _LOGGER.warning(f"Matrix Matrix now actively tracking state adjustments for: {entity_id}")

        async def async_state_changed_listener(event: Event):
            """Triggers instantly whenever the chosen player updates."""
            new_state = event.data.get("new_state")
            if not new_state:
                return
            img_path = new_state.attributes.get("entity_picture")
            if img_path:
                await async_process_and_send(img_path)

        unsub_func = async_track_state_change_event(hass, [entity_id], async_state_changed_listener)
        
        if entry.entry_id not in hass.data:
            hass.data[entry.entry_id] = {}
        hass.data[entry.entry_id][TRACKER_DATA_KEY] = unsub_func

        current_state = hass.states.get(entity_id)
        if current_state and current_state.attributes.get("entity_picture"):
            hass.async_create_task(async_process_and_send(current_state.attributes.get("entity_picture")))

    async def async_mqtt_message_handler(msg: ha_mqtt.ReceiveMessage):
        """Fires whenever your matrix control topic receives an input."""
        raw_payload = msg.payload.strip()
        
        if not raw_payload.startswith("media_player."):
            target_player_id = f"media_player.{raw_payload}"
        else:
            target_player_id = raw_payload
            
        if not target_player_id.replace("media_player.", "").isalnum:
            _LOGGER.error(f"Matrix rejection: '{raw_payload}' contains invalid entity characters.")
            return
        
        async_bind_player_listener(target_player_id)

    try:
        await ha_mqtt.async_subscribe(hass, control_topic, async_mqtt_message_handler)
        _LOGGER.warning(f"Matrix engine successfully subscribed to control topic: {control_topic}")
    except Exception as e:
        _LOGGER.error(f"Could not hook MQTT subscription! Error: {e}")

    entry.async_on_unload(entry.add_update_listener(async_update_listener))
    return True

async def async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle options update by reloading the integration instantly."""
    await hass.config_entries.async_reload(entry.entry_id)

async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload integration entry cleanly if deleted or reloaded."""
    if entry.entry_id in hass.data and TRACKER_DATA_KEY in hass.data[entry.entry_id]:
        hass.data[entry.entry_id][TRACKER_DATA_KEY]()
    return True
