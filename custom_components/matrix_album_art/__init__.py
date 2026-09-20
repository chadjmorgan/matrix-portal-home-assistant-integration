"""Companion integration to convert and stream album art to an MQTT Matrix."""
from __future__ import annotations

import io
import logging
import asyncio
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers.aiohttp_client import async_get_clientsession

import aiohttp
from PIL import Image
import paho.mqtt.publish as publish
import paho.mqtt.client as mqtt

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up the integration from a UI config entry."""
    
    # Check options first (updated via Options Flow), fallback to initial setup data
    mqtt_broker = entry.options.get("mqtt_broker", entry.data.get("mqtt_broker"))
    mqtt_topic = entry.options.get("mqtt_topic", entry.data.get("mqtt_topic", "appletv/matrix/album_art"))

    async def async_send_album_art(call: ServiceCall):
        """The native Home Assistant action executed via automations."""
        image_path = call.data.get("image_path")
        if not image_path:
            _LOGGER.error("Matrix error: No image_path provided in action data.")
            return

        # 1. SMART PATH EXTRACTION
        if "cache=" in image_path:
            full_url = image_path.split("cache=")[-1]
        elif image_path.startswith("http"):
            full_url = image_path
        else:
            # Fallback to Home Assistant's preferred internal network configurations
            base_url = hass.config.internal_url or "http://localhost:8123"
            full_url = f"{base_url.rstrip('/')}/{image_path.lstrip('/')}"
            
        # 2. RENDER THE 32x32 APPLE IMAGE DIRECTLY
        if "{w}" in full_url or "{h}" in full_url:
            full_url = full_url.replace("{w}", "32").replace("{h}", "32")
            full_url = full_url.replace("{c}", "").replace("{f}", "png")

        _LOGGER.warning(f"Matrix attempting download from: {full_url}")
        
        # 3. DOWNLOAD THE IMAGE 
        session = async_get_clientsession(hass, verify_ssl=False)
        headers = {}
        
        # Bypass validation flags for internal loopback resources
        if "api/media_player_proxy" in full_url or "localhost" in full_url or "127.0.0.1" in full_url:
            headers["X-HA-Internal-Request"] = "1"

        try:
            async with asyncio.timeout(10):
                async with session.get(full_url, headers=headers) as response:
                    if response.status != 200:
                        _LOGGER.error(f"Matrix image download failed. Status code: {response.status} for URL: {full_url}")
                        return
                    image_bytes = await response.read()

            # 4. SCALE AND CONVERT TO 16-BIT RGB565 BYTES (Handled on separate worker thread)
            def process_image() -> bytes:
                img = Image.open(io.BytesIO(image_bytes))
                img = img.convert("RGB")
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

            # 5. STREAM TO MQTT BROKER (With explicit MQTT v2 compatibility mapping)
            def mqtt_publish_worker():
                publish.single(
                    topic=mqtt_topic,
                    payload=payload_bytes,
                    qos=0,
                    retain=False,
                    hostname=mqtt_broker,
                    port=1883,
                    protocol=mqtt.MQTTv311
                )

            await hass.async_add_executor_job(mqtt_publish_worker)
            _LOGGER.warning(f"Matrix SUCCESS: Published {len(payload_bytes)} bytes to topic '{mqtt_topic}' via broker '{mqtt_broker}'!")

        except Exception as e:
            _LOGGER.error(f"Matrix critical failure: {e}", exc_info=True)

    # Register the function as a native action/service
    hass.services.async_register(DOMAIN, "send_album_art", async_send_album_art)
    
    # Listen for live profile configuration updates via the "Configure" UI button
    entry.async_on_unload(entry.add_on_update_listener(async_update_listener))
    
    return True

async def async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle options update by reloading the integration instantly."""
    await hass.config_entries.async_reload(entry.entry_id)

async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload integration entry cleanly if deleted or reloaded."""
    hass.services.async_remove(DOMAIN, "send_album_art")
    return True
