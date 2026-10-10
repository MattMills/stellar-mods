#!/bin/bash

DATA_FOLDER='steam_workshop_data/'
PENDING='pending_zip/'     # zipped snapshots waiting for ingest
ARCHIVE_SUFFIX='archive_'
HALT_FILE='ingest_halted'
BATCH=${INGEST_BATCH:-20}      # snapshots per batch-ingest transaction
ZIPPERS=${INGEST_ZIPPERS:-8}   # parallel zip workers
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

# A batch whose ingest fails stays where it is and the loop halts until someone
# looks. The batch is one transaction, so none of it was written: once the
# cause is fixed, removing the halt file retries it cleanly.
if [ -e $HALT_FILE ]; then
	echo "Ingest halted on $(cat $HALT_FILE); remove $PWD/$HALT_FILE to resume"
	exit 1
fi

# Stage 1, in the background: zip every waiting snapshot folder into pending_zip/,
# so the ingest reads one zip instead of hundreds of small files. A folder younger
# than 20 minutes may still be being written by a fetch and waits for a later pass.
zippable() { find $DATA_FOLDER -mindepth 2 -maxdepth 2 -iname '*-*-*T*:*:*.*' -type d -mmin +20 -print0 | sort -z; }
prezip() {  # zip -m deletes the files only once the zip is written; then drop the folder
	mkdir -p "$PENDING${1%/*}" && zip -q -r -m -9 -o -D "$PENDING$1.zip" "$1"
	rmdir "$1" 2>/dev/null
	return 0
}
export -f prezip
export PENDING
zipper=""
zipped_set=""
start_zipper() {
	zipped_set=$(zippable | md5sum)
	zippable | xargs -0 -r -n 1 -P "$ZIPPERS" bash -c 'prezip "$1"' _ &
	zipper=$!
}

# Stage 2: ingest pending zips in snapshot order. A zip is taken only while no
# older snapshot is still a folder (not zipped yet): the ingest must apply
# snapshots oldest first, and the zip workers finish out of order.
next_batch() {
	local oldest z s
	oldest=$(find $DATA_FOLDER -mindepth 2 -maxdepth 2 -iname '*-*-*T*:*:*.*' -type d -printf '%f\n' | sort | head -1)
	batch=()
	while IFS= read -r -d '' z; do
		s=$(basename "$z" .zip)
		if [ -n "$oldest" ] && [[ ! "$s" < "$oldest" ]]; then break; fi
		batch+=("$z")
		[ ${#batch[@]} -ge "$BATCH" ] && break
	done < <(find $PENDING -mindepth 3 -maxdepth 3 -name '*.zip' -print0 2>/dev/null | sort -z)
}

start_zipper
while true; do
	next_batch
	if [ ${#batch[@]} -eq 0 ]; then
		if kill -0 "$zipper" 2>/dev/null; then sleep 5; continue; fi
		# Zip pass done: folders that came of age meanwhile get another pass, as
		# long as passes make progress (a folder zip cannot read stays a folder).
		if [ -n "$(zippable | head -c 1)" ] && [ "$(zippable | md5sum)" != "$zipped_set" ]; then
			start_zipper; continue
		fi
		break
	fi
	APPID=$(basename "$(dirname "${batch[0]}")")
	echo "Batch ingest: AppID $APPID, ${#batch[@]} snapshots, $(basename "${batch[0]}" .zip) .. $(basename "${batch[-1]}" .zip)"
	if ! python3 ./postgres_batch_ingest.py $APPID "${batch[@]}"; then
		echo "$APPID/$(basename "${batch[0]}" .zip) (batch of ${#batch[@]})" > $HALT_FILE
		logger -t stellar-ingest "batch ingest failed on $APPID/$(basename "${batch[0]}" .zip) (batch of ${#batch[@]}); nothing of it was written; halted, remove $PWD/$HALT_FILE to resume"
		kill "$zipper" 2>/dev/null  # no new zips; the running ones finish safely
		wait
		exit 1
	fi
	# Ingested: the zip is the archive. A rename, nothing to read or write again.
	for z in "${batch[@]}"; do
		rel=${z#$PENDING}
		mkdir -p "$ARCHIVE_SUFFIX${rel%/*}" && mv "$z" "$ARCHIVE_SUFFIX$rel"
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
