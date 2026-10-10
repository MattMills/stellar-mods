#!/bin/bash
# Archive ingested snapshots, separately from the ingest (stellar-zipper.service).
#
#   complete/steam_workshop_data/<appid>/<snap>   ingested, waiting for zip (run_periodic_ingest.sh moves them here)
#   under_zip/steam_workshop_data/<appid>/<snap>  claimed by this script, being zipped
#   archive_steam_workshop_data/<appid>/<snap>.zip
#
# Paths inside the zip stay steam_workshop_data/<appid>/<snap>/N.json, as before.
# zip -m deletes the folder's files only after the archive is written, so
# anything left in under_zip/ by a crash is simply zipped again.
# ONCE=1 does a single pass and exits (for tests).
cd /zpool0/share/stellar-mods/ || exit 1
WORKERS=${ZIP_WORKERS:-3}

while true; do
	# Claim everything that is waiting.
	if [ -d complete ]; then
		(cd complete && find steam_workshop_data -mindepth 2 -maxdepth 2 -type d 2>/dev/null) | sort | while read -r s; do
			mkdir -p "under_zip/${s%/*}" && mv "complete/$s" "under_zip/$s"
		done
	fi
	# Zip what is claimed, oldest first, a few at a time.
	if [ -d under_zip ]; then
		(cd under_zip && find steam_workshop_data -mindepth 2 -maxdepth 2 -type d -print0 2>/dev/null | sort -z |
			xargs -0 -r -n 1 -P "$WORKERS" sh -c '
				mkdir -p "../archive_${1%/*}" && zip -q -r -m -9 -o -D "../archive_$1.zip" "$1"
				rmdir "$1" 2>/dev/null; exit 0' _)
	fi
	[ -n "$ONCE" ] && exit 0
	sleep 10
done
