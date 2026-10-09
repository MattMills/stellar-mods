#!/bin/bash

cd /zpool0/share/stellar-mods/



# flock: the kernel drops the lock when the holder exits or dies, so a
# crash or reboot can no longer leave a stale lock behind.
exec 9>run_daily_ingest.lock
if ! flock -n 9; then
	echo "Lock held by another run... script already running?"
	exit
fi

. /opt/esp/esp-idf/export.sh

python3 ./postgres_mod_creator_refresh.py

