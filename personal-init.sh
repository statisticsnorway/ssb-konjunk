#!/bin/bash
# Set Jupyter autosave off
SETTINGS_DIR="$HOME/work/.jupyter/config/lab/user-settings/@jupyterlab/docmanager-extension"
SETTINGS_FILE="$SETTINGS_DIR/plugin.jupyterlab-settings"
mkdir -p "$SETTINGS_DIR"
cat > "$SETTINGS_FILE" <<EOF
{
    "autosave": false
}
EOF
