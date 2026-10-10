#!/bin/bash

DATA_FOLDER='steam_workshop_data/'
HALT_FILE='ingest_halted'
BATCH=${INGEST_BATCH:-20}  # snapshots per batch-ingest transaction
cd /zpool0/share/stellar-mods/

. /opt/esp/esp-idf/export.sh
# SKIP_FETCH=1 starts the ingest straight away, without the ~10 min hourly fetch.
if [ -z "$SKIP_FETCH" ]; then
	echo "Fetching data from Steam workshop"
	python3 steam_workshop.py
fi

# flock: the kernel drops the lock when the holder exits or dies, so a
# crash or reboot can no longer leave a stale lock behind.
exec 9>run_periodic_ingest.lock
if ! flock -n 9; then
	echo "Lock held by another run... script already running?"
	exit
fi

# A batch whose ingest fails stays unarchived and the loop halts until someone
# looks. The batch is one transaction, so none of it was written: once the
# cause is fixed, removing the halt file retries it cleanly.
if [ -e $HALT_FILE ]; then
	echo "Ingest halted on $(cat $HALT_FILE); remove $PWD/$HALT_FILE to resume"
	exit 1
fi

# Waiting snapshot folders, oldest first. Without our own fetch first, an hourly
# fetch may still be writing its folder: leave anything that recent to the next run.
recent=()
[ -n "$SKIP_FETCH" ] && recent=(-mmin +20)
mapfile -d '' -t entries < <(find $DATA_FOLDER -iname '*-*-*T*:*:*.*' "${recent[@]}" -printf "%T@ %p\0" -type d | sort -zn)
folders=()
for e in "${entries[@]}"; do folders+=("${e#* }"); done

prefetch=""
for ((i = 0; i < ${#folders[@]}; i += BATCH)); do
	batch=("${folders[@]:i:BATCH}")
	next=("${folders[@]:i+BATCH:BATCH}")
	# Warm the page cache with the next batch while this one is ingested
	# (a cold snapshot takes ~2.7 s to read from zpool0 one file at a time).
	[ -n "$prefetch" ] && wait $prefetch
	prefetch=""
	if [ ${#next[@]} -gt 0 ]; then
		printf '%s\0' "${next[@]}" | xargs -0 -n 1 -P 8 sh -c 'cat "$1"/* > /dev/null 2>&1; exit 0' _ &
		prefetch=$!
	fi
	APPID=$(basename "$(dirname "${batch[0]}")")
	echo "Batch ingest: AppID $APPID, ${#batch[@]} snapshots, $(basename "${batch[0]}") .. $(basename "${batch[-1]}")"
	if ! python3 ./postgres_batch_ingest.py $APPID "${batch[@]}"; then
		echo "$APPID/$(basename "${batch[0]}") (batch of ${#batch[@]})" > $HALT_FILE
		logger -t stellar-ingest "batch ingest failed on $APPID/$(basename "${batch[0]}") (batch of ${#batch[@]}); nothing of it was written; halted, remove $PWD/$HALT_FILE to resume"
		wait
		exit 1
	fi
	# Hand the batch to the zipper (stellar-zipper.service, zip_completed.sh):
	# a rename into complete/, so the ingest never waits on zip.
	for f in "${batch[@]}"; do
		mkdir -p "complete/${f%/*}" && mv "$f" "complete/$f"
	done
done
wait

# Empty snapshot folders left by failed fetches. -mmin +60 stays clear of a
# fetch that has just created its folder and not yet written to it.
find $DATA_FOLDER -mindepth 2 -maxdepth 2 -iname '*-*-*T*:*:*.*' -type d -empty -mmin +60 -delete

python3 ./postgres_preview_download.py
python3 ./postgres_mod_download.py
#python3 ./postgres_mod_filelist_load.py
python3 ./postgres_mod_file_checksum.py
#python3 ./postgres_mod_stats_refresh.py
