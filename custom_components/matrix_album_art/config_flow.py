"""Config flow for Your Product companion integration."""
from __future__ import annotations

import logging
from typing import Any
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResult

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

# This schema defines the UI fields your user will see and fill out
DATA_SCHEMA = vol.Schema(
    {
        vol.Required("mqtt_broker"): str,
        vol.Optional("mqtt_topic", default="appletv/matrix/album_art"): str,
    }
)

class MatrixConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Your Product."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial step when a user adds the integration."""
        errors: dict[str, str] = {}

        if user_input is not None:
            # Check if this exact device IP is already configured to prevent duplicates
            await self.async_set_unique_id(user_input["mqtt_broker"])
            self._abort_if_unique_id_configured()
            
            return self.async_create_entry(
              title=f"Matrix Display ({user_input['mqtt_broker']})",
              data=user_input,
            )

        return self.async_show_form(
            step_id="user",
            data_schema=DATA_SCHEMA,
            errors=errors,
        )