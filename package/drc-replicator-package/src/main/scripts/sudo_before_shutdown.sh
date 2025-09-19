sed -i '/\[Service\]/a KillMode=process' /usr/lib/systemd/system/ctripapp@100023498.service
systemctl daemon-reload
sh -c 'echo "fs.aio-max-nr=524288" >> /etc/sysctl.conf'
sysctl -p /etc/sysctl.conf