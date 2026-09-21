"""Companion integration to handle combined image and metadata extraction via MQTT."""
from __future__ import annotations

import io
import json
import logging
import asyncio
import re
import base64

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, Event, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_track_state_change_event
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.network import get_url
from homeassistant.components import mqtt as ha_mqtt

from PIL import Image, ImageEnhance
from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

TRACKER_DATA_KEY = f"{DOMAIN}_unsub_listener"

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up the integration from a UI config entry."""
    
    if "mqtt" not in hass.config.components:
        _LOGGER.error("Matrix failure: Core Home Assistant MQTT integration is not configured.")
        return False

    val_saturation = entry.options.get("color_saturation", entry.data.get("color_saturation", 1.0))
    val_contrast = entry.options.get("contrast", entry.data.get("contrast", 1.0))
    val_brightness = entry.options.get("brightness", entry.data.get("brightness", 1.0))

    # Universal Topic Map
    control_topic = "matrix-portal/marquee/source"
    metadata_topic = "matrix-portal/marquee/atv"  # Unified single channel destination
    get_atvs_topic = "matrix-portal/marquee/get-atvs"
    reply_atvs_topic = "matrix-portal/marquee/available-atvs"

    async def async_compile_and_send_unified_packet(entity_id: str):
        """Compiles media player data, fetches artwork, processes RGB565, and transmits via single JSON payload."""
        state_obj = hass.states.get(entity_id)
        if not state_obj:
            return

        # 1. Initialize core JSON data map
        payload = {
            "state": state_obj.state,
            "app_name": state_obj.attributes.get("app_name", "None"),
            "title": state_obj.attributes.get("media_title", "None"),
            "artist": state_obj.attributes.get("media_artist", "None"),
            "album": state_obj.attributes.get("media_album", "None"),
            "artwork_rgb565_b64": ""  # Default empty if no artwork present
        }

        image_path = state_obj.attributes.get("entity_picture")
        
        # 2. Extract and convert artwork if path exists
        if image_path:
            image_bytes = None

            try:
                # 1. Bypass the network: Fetch the image natively from HA component memory using the entity ID
                if hasattr(state_obj, "entity_id"):
                    # Look up the actual media player entity object from the core system registry
                    component = hass.data.get("media_player")
                    if component:
                        player = component.get_entity(state_obj.entity_id)
                        
                        # Execute the player's internal native thumbnail fetch method
                        if player and hasattr(player, "async_get_media_image"):
                            image_data = await player.async_get_media_image()
                            if image_data and isinstance(image_data, tuple):
                                # The native method returns a tuple: (bytes, content_type)
                                image_bytes = image_data[0]

                # 2. Fallback Network: ONLY run this if memory lookup failed and it's a true external URL
                if not image_bytes and image_path.startswith("http"):
                    session = async_get_clientsession(hass, verify_ssl=False)
                    async with asyncio.timeout(10):
                        async with session.get(image_path) as response:
                            if response.status == 200:
                                image_bytes = await response.read()

                # Execute the image conversion pipeline if bytes were retrieved successfully
                if image_bytes:
                    def process_image() -> bytes:
                        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
                        if val_saturation != 1.0:
                            img = ImageEnhance.Color(img).enhance(val_saturation)
                        if val_contrast != 1.0:
                            img = ImageEnhance.Contrast(img).enhance(val_contrast)
                        if val_brightness != 1.0:
                            img = ImageEnhance.Brightness(img).enhance(val_brightness)

                        img = img.resize((32, 32), Image.Resampling.LANCZOS)
                        
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

                    raw_frame = await hass.async_add_executor_job(process_image)
                    payload["artwork_rgb565_b64"] = base64.b64encode(raw_frame).decode("utf-8")
                else:
                    _LOGGER.error("Could not retrieve image data via Entity ID, HTTP, or Supervisor backend.")

            except Exception as e:
                _LOGGER.error(f"Matrix unified processing error: {e}")

        # 3. Transmit single comprehensive payload
        await ha_mqtt.async_publish(hass, metadata_topic, json.dumps(payload), qos=0, retain=True)
        _LOGGER.debug(f"Matrix Unified broadcast transmitted: {payload['title']} - {payload['artist']}")

    @callback
    def async_bind_player_listener(entity_id: str):
        """Binds an event listener tracking all state updates to a single chosen media player."""
        if TRACKER_DATA_KEY in hass.data.get(entry.entry_id, {}):
            try:
                hass.data[entry.entry_id][TRACKER_DATA_KEY]()
            except ValueError:
                _LOGGER.debug("Matrix: Previous tracking listener already released by core engine.")
            except Exception as e:
                _LOGGER.error(f"Matrix tracking cancellation error: {e}")
            
        _LOGGER.warning(f"Matrix now tracking: {entity_id}")

        async def async_state_changed_listener(event: Event):
            new_state = event.data.get("new_state")
            if not new_state:
                return
            
            # Fire combined packet logic safely inside task frame
            hass.async_create_task(async_compile_and_send_unified_packet(entity_id))

        unsub_func = async_track_state_change_event(hass, [entity_id], async_state_changed_listener)
        
        if entry.entry_id not in hass.data:
            hass.data[entry.entry_id] = {}
        hass.data[entry.entry_id][TRACKER_DATA_KEY] = unsub_func

        # Trigger immediate initial evaluation sync
        hass.async_create_task(async_compile_and_send_unified_packet(entity_id))

    # --- ROUTINE 1: ACTIVE PLAYER CHANGED TOPIC INTAKE ---
    async def async_mqtt_message_handler(msg: ha_mqtt.ReceiveMessage):
        raw_payload = msg.payload.strip()
        
        if not raw_payload.startswith("media_player."):
            target_player_id = f"media_player.{raw_payload}"
        else:
            target_player_id = raw_payload
            
        if not re.match(r"^media_player\.[a-zA-Z0-9_]+$", target_player_id):
            _LOGGER.warning(f"Matrix rejected invalid MQTT entity layout: {target_player_id}")
            return
        
        async_bind_player_listener(target_player_id)

    # --- ROUTINE 2: GENERATE AND REPLY AVAILABLE APPLETVS ---
    async def async_mqtt_get_players_handler(msg: ha_mqtt.ReceiveMessage):
        available_players = {}
        all_states = hass.states.async_all()
        entity_reg = er.async_get(hass)
        
        for state in all_states:
            if state.entity_id.startswith("media_player."):
                reg_entry = entity_reg.async_get(state.entity_id)
                
                is_apple_tv = False
                if reg_entry and reg_entry.platform == "apple_tv":
                    is_apple_tv = True
                else:
                    if "apple_tv" in state.entity_id or "appletv" in state.entity_id:
                        is_apple_tv = True
    
                if is_apple_tv:
                    slug = state.entity_id.split(".")[1]
                    friendly_name = state.attributes.get("friendly_name", slug)
                    available_players[slug] = friendly_name
    
        await ha_mqtt.async_publish(hass, reply_atvs_topic, json.dumps(available_players), qos=0, retain=True)

    # Attach network topic handlers
    await ha_mqtt.async_subscribe(hass, control_topic, async_mqtt_message_handler)
    await ha_mqtt.async_subscribe(hass, get_atvs_topic, async_mqtt_get_players_handler)

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