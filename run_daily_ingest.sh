#!/bin/bash

cd /zpool0/share/stellar-mods/



if { set -C; 2>/dev/null > run_daily_ingest.lock; }; then
	trap "rm -f run_daily_ingest.lock" EXIT
else
	echo "Lock file exists... script already running?"
	exit
fi

. /opt/esp/esp-idf/export.sh

python3 ./postgres_mod_creator_refresh.py

