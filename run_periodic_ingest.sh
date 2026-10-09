#!/bin/bash

DATA_FOLDER='steam_workshop_data/'
ARCHIVE_SUFFIX='archive_'
HALT_FILE='ingest_halted'
cd /zpool0/share/stellar-mods/

. /opt/esp/esp-idf/export.sh
echo "Fetching data from Steam workshop"
python3 steam_workshop.py

# flock: the kernel drops the lock when the holder exits or dies, so a
# crash or reboot can no longer leave a stale lock behind.
exec 9>run_periodic_ingest.lock
if ! flock -n 9; then
	echo "Lock held by another run... script already running?"
	exit
fi

# A snapshot whose ingest fails stays unarchived and the loop halts until
# someone looks. Archiving it anyway silently lost the rest of the snapshot;
# retrying it every hour would duplicate the mod_stats rows of the pages that
# did commit.
if [ -e $HALT_FILE ]; then
	echo "Ingest halted on $(cat $HALT_FILE); remove $PWD/$HALT_FILE to resume"
	exit 1
fi

find $DATA_FOLDER -iname '*-*-*T*:*:*.*' -printf "%T@ %p\0" -type d  | sort -zn | while read -d $'\0' folder
do
	JUST_FOLDER=$(echo $folder | grep -Eo ' (.*?)$')
	BASENAME=$(basename $JUST_FOLDER)
	APPID_DIR=$(dirname $JUST_FOLDER)
	APPID=$(basename $APPID_DIR)
	echo "Periodic Ingest script ingesting data: AppID: $APPID, Date: $BASENAME"
	if ! python3 ./postgres_mod_metadata_ingest.py $APPID $JUST_FOLDER; then
		echo "$APPID/$BASENAME" > $HALT_FILE
		logger -t stellar-ingest "metadata ingest failed on $APPID/$BASENAME; halted, remove $PWD/$HALT_FILE to resume"
		exit 1
	fi
	echo "Zipping data: AppID: $APPID, Date: $BASENAME"
	mkdir -p $ARCHIVE_SUFFIX$DATA_FOLDER$APPID/
	zip -r -m -9 -o -D $ARCHIVE_SUFFIX$DATA_FOLDER$APPID/$BASENAME.zip $JUST_FOLDER
	# zip -m leaves the emptied folder behind; remove just this one rather than
	# re-reading every waiting snapshot (7.5 s each time with a large backlog).
	rmdir $JUST_FOLDER 2>/dev/null
done
[ "${PIPESTATUS[2]}" = 0 ] || exit 1

# Empty snapshot folders left by failed fetches. -mmin +60 stays clear of a
# fetch that has just created its folder and not yet written to it.
find $DATA_FOLDER -mindepth 2 -maxdepth 2 -iname '*-*-*T*:*:*.*' -type d -empty -mmin +60 -delete

python3 ./postgres_preview_download.py
python3 ./postgres_mod_download.py
#python3 ./postgres_mod_filelist_load.py
python3 ./postgres_mod_file_checksum.py
#python3 ./postgres_mod_stats_refresh.py

