#!/bin/bash

function toUpper(){
    echo $(echo $1 | tr [a-z] [A-Z])
}
function getEnv(){
    ENV=local
    if [ -f /opt/settings/server.properties ];then
        ENV=`cat /opt/settings/server.properties | egrep -i "^env" | awk -F= '{print $2}'`
    fi
    echo `toUpper $ENV`
}

function getIdc(){
    IDC=local
    if [ -f /opt/settings/server.properties ];then
        IDC=`cat /opt/settings/server.properties | egrep -i "^idc" | awk -F= '{print $2}'`
    fi
    echo `toUpper $IDC`
}
ENV=`getEnv`
echo "current env:"$ENV
IDC=`getIdc`
echo "current idc:"$IDC

if [ "$ENV" = "FAT" ] || [ "$ENV" = "FWS" ] || [ "$ENV" = "UAT" ]; then
  sudo sh -c 'echo "madvise" > /sys/kernel/mm/transparent_hugepage/enabled'
elif [ "$ENV" = "PRO" ] && [ "$IDC" = "SGP-ALI" ]; then
  sudo sh -c 'echo "madvise" > /sys/kernel/mm/transparent_hugepage/enabled'
fi