#!/bin/sh
initctl reload-configuration
start noms-server || restart noms-server