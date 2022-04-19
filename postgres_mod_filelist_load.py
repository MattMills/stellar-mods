from datetime import datetime
import os
from zipfile import ZipFile
import logging
import json
import psycopg2
import psycopg2.extras


start_time = datetime.now()

log_folder = 'logs/postgres_mod_filelist_ingest/'

# Setup logging
try:
    os.makedirs(log_folder)
except:
    pass

log = logging.getLogger()
log.setLevel(logging.DEBUG)

class msecFormatter(logging.Formatter):
    converter= datetime.fromtimestamp
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            s = ct.strftime("%Y-%m-%dT%H:%M:%S.%f")
        return s

log_formatter = msecFormatter(fmt='[%(asctime)s] %(name)s (%(levelname)s): %(message)s')

log_file_handler = logging.FileHandler('%s%s.log' % (log_folder, start_time,), )
log_file_handler.setLevel(logging.INFO)
log_file_handler.setFormatter(log_formatter)

log_console_handler = logging.StreamHandler()
log_console_handler.setLevel(logging.DEBUG)
log_console_handler.setFormatter(log_formatter)

log.addHandler(log_file_handler)
log.addHandler(log_console_handler)

log.info('Postgres mod filelist ingest start')

# Connect to Postgres
dbh = psycopg2.connect("dbname=stellar-mods user=postgres") or logging.critical('Unable to connect to database');
cur = dbh.cursor(cursor_factory = psycopg2.extras.RealDictCursor) #results return associative dicts instead of tuples.
cur2 = dbh.cursor() #used for updates
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(list, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(tuple, psycopg2.extras.Json)

cur.execute(
        'select '
        ' mods.uuid as mod_uuid, '
        ' mods.publishedfileid, '
        ' mods.creator_appid, '
        ' mods.revision_change_number,  '
        ' files.uuid as file_uuid '
        'from mods '
        ' join files on (mods.uuid = files.mod_uuid) '
        ' join file_types on (files.file_type = file_types.id) '
        'where '
        " file_types.type = 'hcontent_file' and "
        ' files.file_time is not null and '
        ' mods.uuid not in (select distinct mod_uuid from mods_filelist)'
        );

mods_to_index = {}
for mod_detail in cur.fetchall():
    mods_to_index[mod_detail['mod_uuid']] = mod_detail

stats = {}
stats['skipped_mods'] = 0
stats['to_index_mods'] = cur.rowcount
stats['indexed_zipfiles'] = 0
stats['indexed_mods'] = 0
stats['indexed_files'] = 0
stats['failed_zip'] = 0

index_dir = 'finished_mods'

dirs = os.listdir(index_dir)
for appid in dirs:
    if os.path.isdir('%s/%s' % (index_dir, appid)):
        app_dir = os.listdir('%s/%s' % (index_dir, appid))
        for mod_uuid in app_dir:
            if mod_uuid not in mods_to_index:
                #log.info('Skipping mod %s - not in mods_to_index' % mod_uuid)
                stats['skipped_mods'] += 1
                continue
            stats['indexed_mods'] += 1
            try:
                mod_dir_path = '%s/%s/%s' % (index_dir, appid, mod_uuid)
                mod_dir = os.listdir(mod_dir_path)
            except Exception as e:
                log.error('Failed to listdir %s - %s %s', (mod_dir_path, type(e), e))
                continue

            for zipfile in mod_dir:
                filename_full = '%s/%s/%s/%s' % (index_dir, appid, mod_uuid, zipfile)
                insert_query = 'insert into mods_filelist (mod_uuid, filename, filesize) values '.encode('utf-8')
                if os.path.isfile(filename_full) and filename_full[-4:].lower() == '.zip':
                    try:
                        with ZipFile(filename_full, 'r') as current_zip:
                            stats['indexed_zipfiles'] += 1
                            log.info('Ingest start %s %s' % (mod_uuid, zipfile,))
                            for info in current_zip.infolist():
                                stats['indexed_files'] += 1
                                insert_query += cur.mogrify('(%s, %s, %s),', (mod_uuid, info.filename, info.file_size))

                            insert_query = insert_query[:-1] # remove comma
                            cur.execute(insert_query)

                            log.info('Ingest complete %s %s' % (mod_uuid, zipfile))
                    except Exception as e:
                        stats['failed_zip'] += 1
                        log.error('Zipfile failed to open - %s %s %s' % (mod_uuid, type(e), e))
                    finally:
                        dbh.commit()
log.info(stats)
