#!/bin/sh
export DISPLAY=:1
source /opt/arcgis/server10.0/servercore/.Server/init_server.sh
source /opt/arcgis/server10.0/python26/setenv_python.sh
python /home/arcgis/virtpy/lib/python2.6/site-packages/cybercomq/arcgis/hotspots.py $@

