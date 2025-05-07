#!/bin/bash

SERVICE_FILE="/usr/lib/systemd/system/ctripapp@100023928.service"

function getEnv(){
    ENV=local
    if [ -f /opt/settings/server.properties ];then
        ENV=`cat /opt/settings/server.properties | egrep -i "^env" | awk -F= '{print $2}'`
    fi
    echo `toUpper $ENV`
}
ENV=`getEnv`
echo "current env:"$ENV

if [ "$ENV" != "PRO" ]; then
  if [ -f "$SERVICE_FILE" ]; then
    if grep -q "^TimeoutStartSec=" "$SERVICE_FILE"; then
      sudo sed -i 's/^TimeoutStartSec=.*/TimeoutStartSec=300/' "$SERVICE_FILE"
    else
      sudo sed -i '/^\[Service\]/a TimeoutStartSec=300' "$SERVICE_FILE"
    fi
    sudo systemctl daemon-reload
    echo "TimeoutStartSec=300 has been set in $SERVICE_FILE"
  else
    echo "Service file $SERVICE_FILE does not exist."
  fi
fi
