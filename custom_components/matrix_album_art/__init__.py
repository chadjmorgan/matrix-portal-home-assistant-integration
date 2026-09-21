"""Companion integration to handle image conversions, listings, and metadata extraction via MQTT."""
from __future__ import annotations

import io
import json
import logging
import asyncio

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, Event, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_track_state_change_event
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

    mqtt_topic = entry.options.get("mqtt_topic", entry.data.get("mqtt_topic", "appletv/matrix/album_art"))
    
    val_saturation = entry.options.get("color_saturation", entry.data.get("color_saturation", 1.0))
    val_contrast = entry.options.get("contrast", entry.data.get("contrast", 1.0))
    val_brightness = entry.options.get("brightness", entry.data.get("brightness", 1.0))

    # Universal Topic Map
    control_topic = "matrix-portal/marquee/source"
    metadata_topic = "matrix-portal/marquee/atv"
    get_atvs_topic = "matrix-portal/marquee/get-atvs"
    reply_atvs_topic = "matrix-portal/marquee/available-atvs"

    @callback
    def async_publish_metadata(entity_id: str):
        """Compiles media player state metadata parameters and fires them natively into MQTT."""
        state_obj = hass.states.get(entity_id)
        if not state_obj:
            return

        metadata_payload = {
            "state": state_obj.state,
            "app_name": state_obj.attributes.get("app_name", "None"),
            "title": state_obj.attributes.get("media_title", "None"),
            "artist": state_obj.attributes.get("media_artist", "None"),
            "album": state_obj.attributes.get("media_album", "None")
        }
        
        # Dispatch metadata asynchronously
        hass.async_create_task(
            ha_mqtt.async_publish(hass, metadata_topic, json.dumps(metadata_payload), qos=0, retain=True)
        )
        _LOGGER.debug(f"Matrix Metadata updated: {metadata_payload}")

    async def async_process_and_send(image_path: str):
        """Processes the extracted image path, resizes it, and sends it via native MQTT."""
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

        session = async_get_clientsession(hass, verify_ssl=False)
        headers = {"X-HA-Internal-Request": "1"} if "api/media_player_proxy" in full_url else {}

        try:
            async with asyncio.timeout(10):
                async with session.get(full_url, headers=headers) as response:
                    if response.status != 200:
                        return
                    image_bytes = await response.read()

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

            payload_bytes = await hass.async_add_executor_job(process_image)
            await ha_mqtt.async_publish(hass, mqtt_topic, payload_bytes, qos=0, retain=False)

        except Exception as e:
            _LOGGER.error(f"Matrix engine error: {e}")

    @callback
    def async_bind_player_listener(entity_id: str):
        """Binds a native event state change listener to a single chosen media player."""
        if TRACKER_DATA_KEY in hass.data.get(entry.entry_id, {}):
            try:
                hass.data[entry.entry_id][TRACKER_DATA_KEY]()
            except ValueError:
                _LOGGER.debug("Matrix: Previous tracking listener already released by core engine.")
            except Exception as e:
                _LOGGER.error(f"Matrix tracking cancellation error: {e}")
            
        _LOGGER.warning(f"Matrix now actively tracking state adjustments for: {entity_id}")

        async def async_state_changed_listener(event: Event):
            new_state = event.data.get("new_state")
            if not new_state:
                return
            
            # Fire data packet on ANY track state adjustment (Automation 1 alternative)
            async_publish_metadata(entity_id)

            img_path = new_state.attributes.get("entity_picture")
            if img_path:
                await async_process_and_send(img_path)

        unsub_func = async_track_state_change_event(hass, [entity_id], async_state_changed_listener)
        
        if entry.entry_id not in hass.data:
            hass.data[entry.entry_id] = {}
        hass.data[entry.entry_id][TRACKER_DATA_KEY] = unsub_func

        # Immediately execute state dumps and image scans upon source changes (Automation 2 alternative)
        async_publish_metadata(entity_id)
        current_state = hass.states.get(entity_id)
        if current_state and current_state.attributes.get("entity_picture"):
            hass.async_create_task(async_process_and_send(current_state.attributes.get("entity_picture")))

    # --- ROUTINE 1: ACTIVE PLAYER CHANGED TOPIC INTAKE ---
    async def async_mqtt_message_handler(msg: ha_mqtt.ReceiveMessage):
        raw_payload = msg.payload.strip()
        
        if not raw_payload.startswith("media_player."):
            target_player_id = f"media_player.{raw_payload}"
        else:
            target_player_id = raw_payload
            
        if not target_player_id.replace("media_player.", "").isalnum:
            return
        
        async_bind_player_listener(target_player_id)

    # --- ROUTINE 2: GENERATE AND REPLY AVAILABLE APPLETVS ---
    async def async_mqtt_get_players_handler(msg: ha_mqtt.ReceiveMessage):
        available_players = {}
        all_states = hass.states.async_all()
        
        # Fetch the entity registry to see which integration owns the media_player
        entity_reg = await hass.helpers.entity_registry.async_get_registry()
        
        for state in all_states:
            if state.entity_id.startswith("media_player."):
                entry = entity_reg.async_get(state.entity_id)
                
                is_apple_tv = False
                if entry and entry.platform == "apple_tv":
                    is_apple_tv = True
                else:
                    # 2. Fallback check: Look at naming conventions if registry lookup isn't enough
                    slug = state.entity_id.split(".")[1]
                    if "apple_tv" in slug or "appletv" in slug:
                        is_apple_tv = True

                # If it's an Apple TV, add it to the payload
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
