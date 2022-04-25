import psycopg2
import psycopg2.extras
import json
import time
from datetime import datetime
import os
import logging
import sys
from zipfile import ZipFile


script_name = sys.argv[0]


parse_dir = 'archive_steam_workshop_data/281990/'

if parse_dir[:-1] != '/':
    parse_dir += '/'

parse_date = os.path.basename(os.path.dirname(parse_dir))
start_time = datetime.now()

steam_default_hcontent_preview = '18446744073709551615'


# Setup logging
try:
    os.makedirs('logs/%s/' % (script_name,))
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

log_formatter = msecFormatter(fmt='[%(asctime)s] (%(levelname)s): %(message)s')

log_file_handler = logging.FileHandler('logs/%s/%s.log' % (script_name, start_time,), )
log_file_handler.setLevel(logging.INFO)
log_file_handler.setFormatter(log_formatter)

log_console_handler = logging.StreamHandler()
log_console_handler.setLevel(logging.DEBUG)
log_console_handler.setFormatter(log_formatter)

log.addHandler(log_file_handler)
log.addHandler(log_console_handler)

log.info('%s start (%s)' % (script_name, parse_dir))



# Connect to Postgres
dbh = psycopg2.connect("dbname=stellar-mods user=postgres") or logging.critical('Unable to connect to database');
cur = dbh.cursor(cursor_factory = psycopg2.extras.RealDictCursor) #results return associative dicts instead of tuples.
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(list, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(tuple, psycopg2.extras.Json)

existing_mod_detail = {}
seen_mod_detail = {}

def refresh_existing_mod_detail():
    cur.execute('select uuid,publishedfileid,revision_change_number from mods where creator_steam_id is null order by publishedfileid, revision_change_number desc');

    existing_mod_detail = {}
    for mod_detail in  cur.fetchall():
        if mod_detail['publishedfileid'] not in existing_mod_detail:
            existing_mod_detail[mod_detail['publishedfileid']] = {}
        if mod_detail['revision_change_number'] in existing_mod_detail[mod_detail['publishedfileid']]:
            log.error("Duplicate change number!")
            exit()

        existing_mod_detail[mod_detail['publishedfileid']][mod_detail['revision_change_number']] = mod_detail['uuid']

    return existing_mod_detail

existing_mod_detail = refresh_existing_mod_detail()

#init stats variables
stats = {}
stats['file_count'] = 0
stats['zip_file_count'] = 0
stats['total_mod_count'] = 0
stats['file_mod_count'] = 0
stats['existing_mod_count'] = 0
stats['existing_revision_count'] = 0
stats['existing_mod_update_count'] = 0
stats['failed_mod_count'] = 0
stats['pushed_mod_count'] = 0

for filename in os.listdir(parse_dir):
    full_filename = os.path.join(parse_dir, filename)
    if os.path.isfile(full_filename):
        logging.info('(%s) %s - Parse start' % (stats['file_count'], full_filename))
        with ZipFile(full_filename) as zipfile:
            stats['zip_file_count'] += 1
            for filename in zipfile.namelist():
                with zipfile.open(filename, 'r') as fh:
                    stats['file_count'] += 1
                    data = json.load(fh)
                    sql_statement_mods = (
                            'insert into mods ('
                                'publishedfileid, creator_appid, consumer_appid, revision_change_number, creator_steam_id'
                            ') values '
                            ).encode('utf-8')

                    if 'publishedfiledetails' not in data['response']:
                        continue # Last file is empty, we don't list dir in order
                        
                    for file_details in data['response']['publishedfiledetails']:
                        file_details['publishedfileid'] = int(file_details['publishedfileid'])

                        try:
                            file_details['revision_change_number'] = int(file_details['revision_change_number'])
                        except:
                            pass # Failed mods have weird structure, may be missing field.
                        
                        stats['file_mod_count'] += 1
                        if file_details['publishedfileid'] not in existing_mod_detail:
                            stats['existing_mod_count'] += 1
                            continue
                        else:
                            if file_details['revision_change_number'] not in existing_mod_detail[file_details['publishedfileid']]:
                                stats['existing_revision_count'] += 1
                                continue

                        try:
                            sql_statement_mods += cur.mogrify((
                                '('
                                    '%(publishedfileid)s, %(creator_appid)s, %(consumer_appid)s, %(revision_change_number)s, %(creator)s'
                                '),'
                            ), file_details)
                            stats['pushed_mod_count'] += 1
                        except Exception as e:
                            stats['failed_mod_count'] += 1
                            logging.error('(%s) %s - PARSE FAILED (%s/%s)' % (stats['file_count'], full_filename, stats['file_mod_count'], stats['total_mod_count']))
                            try:
                                logging.error('Failed Published File ID: %s - %s' % (file_details['publishedfileid'], type(e)))
                            except:
                                logging.error('Failed Published File ID also caused exception - %s' % (type(e),))
                
    
                sql_statement_mods = sql_statement_mods[:-1] #remove last comma
                sql_statement_mods += cur.mogrify(
                    ' ON CONFLICT ON CONSTRAINT mod_uniqueness_constraint DO UPDATE SET '
                    'creator_steam_id = EXCLUDED.creator_steam_id'
                )
                if stats['pushed_mod_count'] > 0:
                    cur.execute(sql_statement_mods)
                dbh.commit()
                stats['pushed_mod_count'] = 0

                # if we've inserted any new uuids we need to get them before we can do stats and files on this input file, not super efficient but plenty fast for now.
                existing_mod_detail = refresh_existing_mod_detail()



                stats['total_mod_count'] += stats['file_mod_count']
                logging.info('(%s) %s - Parse complete (%s/%s)' % (stats['file_count'], full_filename, stats['file_mod_count'], stats['total_mod_count']))
                stats['file_mod_count'] = 0


            logging.info('Zip parsing complete: %s' % (full_filename))
            logging.info('Parse stats: %s' % (stats))

            import platform
            import sys
            import os
            config_metadata = {
                    'host': platform.node(),
                    'python_version': platform.python_version(),
                    'user': os.getlogin(),
                    'argv': sys.argv,
                    'pid' : os.getpid(),
                    'folder': parse_dir,
                    }

            cur.execute(
                    'insert into ingest_event '
                    '(start_timestamp, end_timestamp, stats, type, config_metadata) '
                    'values ( %s, %s, %s, %s, %s)',
                    (start_time, datetime.now(), stats, script_name, config_metadata)
            )

            dbh.commit()

dbh.commit()
cur.close()
dbh.close()
