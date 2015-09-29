#! /bin/sh

pigeon "${1}" | goimports > "${1}".go