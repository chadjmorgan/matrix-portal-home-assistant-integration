# Matrix Album Art Companion

The official Home Assistant companion integration for your pixel matrix display. This plugin automatically extracts, formats, and converts live album artwork from your media players into 16-bit RGB565 raw bytes, streaming them directly over MQTT to your display panel.

---

## Features
* 🔄 **Real-Time Extraction:** Monitors media player endpoints (`entity_picture`) instantly.
* 📐 **Auto-Scaling:** Resizes artwork to an optimized 32x32 pixel layout.
* 🎨 **Color Accuracy:** Automatically maps standard 24-bit RGB space down to raw 16-bit Big-Endian RGB565 byte arrays.
* 📦 **Zero-Configuration Dependencies:** Automatically handles python script packages (`pillow`, `paho-mqtt`) via background system setups.

---

## 🛠️ Step 1: Installation via HACS

Because this companion integration is hosted on GitHub, you can install it seamlessly by adding it as a Custom Repository inside the **Home Assistant Community Store (HACS)**:

1. Open your **Home Assistant** instance.
2. Click on **HACS** in your sidebar.
3. Click the **three dots (⋮)** in the top right-hand corner and select **Custom repositories**.
4. Paste the URL of this GitHub repository into the **Repository** field:
   `https://github.com/chadjmorgan/matrix-portal-home-assistant-integration.git`
5. Change the **Category** dropdown menu to **Integration**.
6. Click **Add**, then click **Download** on the newly discovered card.
7. **Restart Home Assistant** to activate the plugin files.

---

## ⚙️ Step 2: Configuration Wizard

Set up your hardware destination entirely from the Home Assistant graphical interface:

1. Navigate to **Settings** ➡️ **Devices & Services**.
2. Click the **+ Add Integration** button in the bottom right corner.
3. Search for **Matrix Album Art Companion** and select it.
4. Fill out the configuration fields:
   * **MQTT Broker IP Address:** Enter your MQTT broker's host network IP (e.g., `192.168.1.1`).
   * **MQTT Topic Destination:** The target MQTT path your display listens to (Defaults to `appletv/matrix/album_art`).
5. Click **Submit** to finalize the setup.

---

## 🤖 Step 3: Fast Automation Setup (Blueprint)

Skip writing YAML or manual routines. Import our pre-configured automation script blueprint to link your media players directly with your pixel matrix panel using a one-click setup wizard:

[![Open your Home Assistant instance and show the blueprint import form with a prefilled URL.](https://home-assistant.io)](https://home-assistant.io)

### Manual Blueprint Setup (Alternative)
If the button above does not load, you can import it by going to **Settings** ➡️ **Automations & Scenes** ➡️ **Blueprints** ➡️ **Import Blueprint** and pasting this raw file path:
```text
https://raw.githubusercontent.com/chadjmorgan/matrix-portal-home-assistant-integration/31c5317eecc94de0cd2b2db36e8180d677d4e0d0/blueprints/automation/matrix_album_art_sync.yaml
```

Once imported, click **Create Automation**, pick your target **Media Player** (e.g., Apple TV) from the visual dropdown menu, and hit save!
