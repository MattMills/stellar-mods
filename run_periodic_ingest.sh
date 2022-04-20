#!/bin/bash

DATA_FOLDER='steam_workshop_data/'
ARCHIVE_SUFFIX='archive_'
cd /zpool0/share/stellar-mods/

echo "Fetching data from Steam workshop"
python3 steam_workshop.py

find $DATA_FOLDER -iname '*-*-*T*:*:*.*' -printf "%T@ %p\0" -type d  | sort -zn | while read -d $'\0' folder 
do
	JUST_FOLDER=$(echo $folder | grep -Eo ' (.*?)$')
	BASENAME=$(basename $JUST_FOLDER)
	APPID_DIR=$(dirname $JUST_FOLDER)
	APPID=$(basename $APPID_DIR)
	echo "Periodic Ingest script ingesting data: AppID: $APPID, Date: $BASENAME"
	python3 ./postgres_mod_data_load.py $APPID $JUST_FOLDER
	echo "Zipping data: AppID: $APPID, Date: $BASENAME"
	mkdir -p $ARCHIVE_SUFFIX$DATA_FOLDER$APPID/
	zip -r -m -9 -o -D $ARCHIVE_SUFFIX$DATA_FOLDER$APPID/$BASENAME.zip $JUST_FOLDER
	find steam_workshop_data/ -iname '*-*-*T*:*:*.*' -type d -empty -delete
done

python3 ./postgres_preview_download.py
python3 ./postgres_mod_download.py
python3 ./postgres_mod_filelist_load.py
python3 ./postgres_mod_stats_refresh.py

