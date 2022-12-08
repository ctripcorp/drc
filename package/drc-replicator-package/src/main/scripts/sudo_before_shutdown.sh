sed -i '/\[Service\]/a KillMode=process' /usr/lib/systemd/system/ctripapp@100023498.service
systemctl daemon-reload