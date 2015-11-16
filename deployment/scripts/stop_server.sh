#!/bin/sh
[[ `service status noms-server 2> /dev/null` ]] && service stop noms-server