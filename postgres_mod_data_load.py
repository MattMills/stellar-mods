import psycopg2
import json
import time
from datetime import datetime
import os
import logging
import sys



if len(sys.argv) < 2 or not os.path.isdir(sys.argv[2]):
    print("Error: invalid argument syntax")
    exit()

parse_dir = sys.argv[2]

#parse_dir = 'steam_workshop_data/281990/2022-04-07T00:20:52.551535/' #temporary variable, should be passed on command line to currently processing dir of json files.
#parse_dir = 'steam_workshop_data/281990/2022-04-07T11:59:26.826724/'
#parse_dir = 'steam_workshop_data/281990/2022-04-07T19:10:32.985600'
#parse_dir = 'steam_workshop_data/281990/2022-04-08T03:04:30.773086/'

if parse_dir[:-1] != '/':
    parse_dir += '/'

parse_date = os.path.basename(os.path.dirname(parse_dir))
start_time = datetime.now()

steam_default_hcontent_preview = '18446744073709551615'


# Setup logging
try:
    os.makedirs('logs/metadata_ingest/')
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

log_file_handler = logging.FileHandler('logs/metadata_ingest/%s.log' % (start_time,), )
log_file_handler.setLevel(logging.INFO)
log_file_handler.setFormatter(log_formatter)

log_console_handler = logging.StreamHandler()
log_console_handler.setLevel(logging.DEBUG)
log_console_handler.setFormatter(log_formatter)

log.addHandler(log_file_handler)
log.addHandler(log_console_handler)

log.info('Steam workshop metadata ingest start (%s)' % parse_dir)



# Connect to Postgres
dbh = psycopg2.connect("dbname=stellar-mods user=postgres") or logging.critical('Unable to connect to database');
cur = dbh.cursor()


existing_mod_detail = {}
seen_mod_detail = {}

def refresh_existing_mod_detail():
    cur.execute('select distinct ON (publishedfileid) uuid,publishedfileid,revision_change_number from mods  order by publishedfileid, revision_change_number desc');

    existing_mod_detail = {}
    for mod_detail in  cur.fetchall():
        if mod_detail[1] in existing_mod_detail:
            logging.critical('Duplicate mod publishedfileid detected in database - %s %s %s' % (mod_detail[1], existing_mod_detail[mod_detail[1]]['uuid'], mod_detail[0]))
            exit(10);
        existing_mod_detail[mod_detail[1]] = {}
        existing_mod_detail[mod_detail[1]]['uuid'] = mod_detail[0]
        existing_mod_detail[mod_detail[1]]['publishedfileid'] = mod_detail[1]
        existing_mod_detail[mod_detail[1]]['revision_change_number'] = mod_detail[2]

    return existing_mod_detail

def refresh_file_type_ids():
    cur.execute('select id, type from file_types');
    file_type_ids = {}
    for file_type in cur.fetchall():
        file_type_ids[file_type[1]] = file_type[0]

    return file_type_ids

existing_mod_detail = refresh_existing_mod_detail()
file_type_ids = refresh_file_type_ids()

#init stats variables
stats = {}
stats['file_count'] = 0
stats['total_mod_count'] = 0
stats['file_mod_count'] = 0
stats['existing_mod_count'] = 0
stats['existing_mod_update_count'] = 0
stats['failed_mod_count'] = 0

for filename in os.listdir(parse_dir):
    full_filename = os.path.join(parse_dir, filename)
    if os.path.isfile(full_filename):
        stats['file_count'] += 1
        logging.info('(%s) %s - Parse start' % (stats['file_count'], full_filename))
        with open(full_filename, 'r') as fh:
            data = json.load(fh)
            sql_statement_mods = (
                    'insert into mods ('
                        'publishedfileid, creator_appid, consumer_appid, consumer_shortcutid,'
                        'steam_time_created, steam_time_updated, steam_visibility, flags,'
                        'workshop_file, workshop_accepted, banned, ban_reason,'
                        'banner, can_be_deleted, file_type, can_subscribe,'
                        'language, maybe_inappropriate_sex, maybe_inappropriate_violence,'
                        'revision_change_number, ban_text_check_result, title, our_time_updated'
                    ') values '
                    ).encode('utf-8')

            sql_statement_stats = (
                    'insert into mod_stats ('
                        'mod_uuid, num_comments_public, subscriptions, favorited,'
                        'followers, lifetime_subscriptions, lifetime_favorited, lifetime_followers,'
                        'views, num_children, num_reports, votes_score, votes_up, votes_down, timestamp'
                    ') values '
                    ).encode('utf-8')

            sql_statement_files = (
                    'insert into files ('
                        'mod_uuid, file_type, file_name ,file_url,'
                        'file_size, sort_order, steam_id'
                    ') values '
                    ).encode('utf-8')

            if 'publishedfiledetails' not in data['response']:
                continue # Last file is empty, we don't list dir in order

            for file_details in data['response']['publishedfiledetails']:
                file_details['publishedfileid'] = int(file_details['publishedfileid'])
                file_details['parse_date'] = parse_date #datestamp from folder name - time when steam API was accessed

                try:
                    file_details['revision_change_number'] = int(file_details['revision_change_number'])
                except:
                    pass # Failed mods have weird structure, may be missing field.

                stats['file_mod_count'] += 1
                if file_details['publishedfileid'] in existing_mod_detail: #mod already exists in DB
                    stats['existing_mod_count'] += 1
                    if file_details['revision_change_number'] != existing_mod_detail[file_details['publishedfileid']]['revision_change_number']:
                            #Update DB
                            stats['existing_mod_update_count'] += 1
                try:
                    file_details['visibility'] = bool(file_details['visibility'])
                    sql_statement_mods += cur.mogrify((
                        '('
                            '%(publishedfileid)s, %(creator_appid)s, %(consumer_appid)s, %(consumer_shortcutid)s,'
                            '%(time_created)s, %(time_updated)s, %(visibility)s, %(flags)s,'
                            '%(workshop_file)s, %(workshop_accepted)s, %(banned)s, %(ban_reason)s,'
                            '%(banner)s, %(can_be_deleted)s, %(file_type)s, %(can_subscribe)s,'
                            '%(language)s, %(maybe_inappropriate_sex)s, %(maybe_inappropriate_violence)s,'
                            '%(revision_change_number)s, %(ban_text_check_result)s, %(title)s, %(parse_date)s'
                        '),'
                    ), file_details)
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
                    'our_time_updated = EXCLUDED.our_time_updated, '
                    'steam_time_updated = EXCLUDED.steam_time_updated,'
                    'steam_visibility = EXCLUDED.steam_visibility,'
                    'flags = EXCLUDED.flags,'
                    'workshop_file = EXCLUDED.workshop_file,'
                    'workshop_accepted = EXCLUDED.workshop_accepted,'
                    'banned = EXCLUDED.banned,'
                    'ban_reason = EXCLUDED.ban_reason,'
                    'banner = EXCLUDED.banner,'
                    'can_be_deleted = EXCLUDED.can_be_deleted,'
                    'file_type = EXCLUDED.file_type,'
                    'can_subscribe = EXCLUDED.can_subscribe,'
                    'language = EXCLUDED.language,'
                    'maybe_inappropriate_sex = EXCLUDED.maybe_inappropriate_sex,'
                    'maybe_inappropriate_violence = EXCLUDED.maybe_inappropriate_violence,'
                    'ban_text_check_result = EXCLUDED.ban_text_check_result,'
                    'title = EXCLUDED.title'
                )
            cur.execute(sql_statement_mods)

            # if we've inserted any new uuids we need to get them before we can do stats and files on this input file, not super efficient but plenty fast for now.
            existing_mod_detail = refresh_existing_mod_detail()


            for file_details in data['response']['publishedfiledetails']:
                if file_details['publishedfileid'] not in existing_mod_detail:
                    continue #ignore anything not in DB by this point, as it's likely one of the failed mods above.
                file_details['parse_date'] = parse_date
                file_details['mod_uuid'] = existing_mod_detail[file_details['publishedfileid']]['uuid']
                file_details['votes_score'] = file_details['vote_data']['score']
                file_details['votes_up'] = file_details['vote_data']['votes_up']
                file_details['votes_down'] = file_details['vote_data']['votes_down']
                sql_statement_stats += cur.mogrify((
                    '('
                        '%(mod_uuid)s, %(num_comments_public)s, %(subscriptions)s, %(favorited)s,'
                        '%(followers)s, %(lifetime_subscriptions)s, %(lifetime_favorited)s, %(lifetime_followers)s,'
                        '%(views)s, %(num_children)s, %(num_reports)s, %(votes_score)s, %(votes_up)s, %(votes_down)s, %(parse_date)s'
                    '),'), file_details)


                if file_details['hcontent_preview'] != steam_default_hcontent_preview:
                    sql_statement_files += cur.mogrify((
                        '('
                            '%(mod_uuid)s, %(file_type)s, %(file_name)s, %(file_url)s,'
                            '%(file_size)s, %(sort_order)s, %(steam_id)s'
                        '),'),
                        { 
                            'mod_uuid': existing_mod_detail[file_details['publishedfileid']]['uuid'],
                            'file_type': file_type_ids['hcontent_preview'],
                            'file_name': file_details['hcontent_preview'],
                            'file_url': file_details['preview_url'],
                            'file_size': file_details['preview_file_size'],
                            'sort_order': 0,
                            'steam_id': file_details['hcontent_preview'],
                        })

                sql_statement_files += cur.mogrify((
                    '('
                        '%(mod_uuid)s, %(file_type)s, %(file_name)s, %(file_url)s,'
                        '%(file_size)s, %(sort_order)s, %(steam_id)s'
                    '),'),
                    {
                        'mod_uuid': existing_mod_detail[file_details['publishedfileid']]['uuid'],
                        'file_type': file_type_ids['hcontent_file'],
                        'file_name': file_details['filename'],
                        'file_url': file_details['url'],
                        'file_size': file_details['file_size'],
                        'sort_order': 0,
                        'steam_id': file_details['hcontent_file'],
                    })

                if 'previews' in file_details:
                        for preview in file_details['previews']:
                            try:
                                if preview['preview_type'] == 1:
                                    preview['filename'] = preview['youtubevideoid']
                                    preview['url'] = 'https://www.youtube.com/watch?v=%s' % (preview['youtubevideoid'],)
                                    preview['size'] = 0
                                elif preview['preview_type'] == 2:
                                    preview['filename'] = preview['external_reference']
                                    preview['url'] = 'https://sketchfab.com/3d-models/%s' % (preview['external_reference'],)
                                    preview['size'] = 0
                                sql_statement_files += cur.mogrify((
                                    '('
                                        '%(mod_uuid)s, %(file_type)s, %(file_name)s, %(file_url)s,'
                                        '%(file_size)s, %(sort_order)s, %(steam_id)s'
                                    '),'),
                                    {
                                        'mod_uuid': existing_mod_detail[file_details['publishedfileid']]['uuid'],
                                        'file_type': file_type_ids['preview_type_%s' % preview['preview_type']],
                                        'file_name': preview['filename'],
                                        'file_url': preview['url'],
                                        'file_size': preview['size'],
                                        'sort_order': preview['sortorder'],
                                        'steam_id': preview['previewid'],
                                    })
                            except Exception as e:
                                log.warning('Failed to upsert preview: %s %s %s' % (preview,e, type(e)))



                #TODO: Do we want to parse the timestamp out of the folder name instead of using now() for backlogged data?

            
            sql_statement_stats = sql_statement_stats[:-1] #remove last comma
            cur.execute(sql_statement_stats)

            sql_statement_files = sql_statement_files[:-1] #remove last comma
            sql_statement_files += cur.mogrify(' ON CONFLICT ON CONSTRAINT file_uniqueness_constraint DO UPDATE SET last_seen = %s, delete_timestamp = null', (parse_date,))
            cur.execute(sql_statement_files)

        stats['total_mod_count'] += stats['file_mod_count']
        logging.info('(%s) %s - Parse complete (%s/%s)' % (stats['file_count'], full_filename, stats['file_mod_count'], stats['total_mod_count']))
        stats['file_mod_count'] = 0


logging.info('All parsing complete: %s' % (parse_dir))
logging.info('Parse stats: %s' % (stats))

cur.execute('insert into ingest_event (folder, start_timestamp, end_timestamp, file_count, total_mod_count, existing_mod_count, existing_mod_update_count, failed_mod_count) values( %s, %s, %s, %s, %s, %s, %s, %s)', (parse_dir, start_time, datetime.now(), stats['file_count'], stats['total_mod_count'], stats['existing_mod_count'], stats['existing_mod_update_count'], stats['failed_mod_count']))

dbh.commit()
cur.close()
dbh.close()
