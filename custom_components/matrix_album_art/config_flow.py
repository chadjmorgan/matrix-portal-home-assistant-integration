"""Config flow for Matrix Album Art integration."""
from __future__ import annotations

import logging
from typing import Any
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.data_entry_flow import FlowResult

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


class MatrixConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Matrix Album Art."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial setup step when adding the integration."""
        if user_input is not None:
            await self.async_set_unique_id(user_input["mqtt_broker"])
            self._abort_if_unique_id_configured()

            # Pass the initial setup values straight into the entry
            return self.async_create_entry(
                title=f"Matrix Display ({user_input['mqtt_broker']})",
                data=user_input,
            )

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({
                vol.Required("mqtt_broker"): str,
                vol.Optional("mqtt_topic", default="appletv/matrix/album_art"): str,
            })
        )

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> MatrixOptionsFlowHandler:
        """Link the configuration to the active Options Flow handler."""
        return MatrixOptionsFlowHandler(config_entry)


class MatrixOptionsFlowHandler(config_entries.OptionsFlow):
    """Handle updating options via the front-end Configure button."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        """Initialize options flow."""
        self.config_entry = config_entry

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Manage the configuration modifications requested by the user."""
        if user_input is not None:
            # Crucial: Merge your data dictionaries together so Home Assistant
            # doesn't lose your baseline data keys on saving.
            new_data = {**self.config_entry.data, **user_input}
            self.hass.config_entries.async_update_entry(self.config_entry, data=new_data)
            return self.async_create_entry(title="", data=user_input)

        # Pull existing parameters safely by falling back to the baseline data schema
        current_broker = self.config_entry.options.get(
            "mqtt_broker", self.config_entry.data.get("mqtt_broker", "")
        )
        current_topic = self.config_entry.options.get(
            "mqtt_topic", self.config_entry.data.get("mqtt_topic", "appletv/matrix/album_art")
        )

        # Render the form safely with valid string fallbacks
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema({
                vol.Required("mqtt_broker", default=str(current_broker)): str,
                vol.Required("mqtt_topic", default=str(current_topic)): str,
            }),
        )
