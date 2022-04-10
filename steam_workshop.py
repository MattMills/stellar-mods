#!/usr/bin/python3


import requests
import json
import pprint
import time
from datetime import datetime
import os


url = 'http://api.steampowered.com/IPublishedFileService/QueryFiles/v1/' #Getting weird TLS errors, don't need TLS anyway

params = {}

# SECRET
params['key']               = '' #Steam API key go here
# SECRET

params['query_type']        = 1 #https://steam.readthedocs.io/en/stable/api/steam.enums.html RankedByPublicationDate= 1
params['page']              = 0
params['cursor']            = "*" #no cursor yet
params['numperpage']        = 100
params['creator_appid']     = 281990 #stellaris
params['app_id']            = 281990 #stellaris
params['return_vote_data']  = True
params['return_tags']       = True
params['return_previews']   = True
params['return_metadata']   = True


output_dir = 'steam_workshop_data/%s/%s' % (params['app_id'], datetime.utcnow().isoformat())

try:
    os.makedirs(output_dir)
except:
    pass

last_cursor = ''
iterator = 0
#{   'response': {   'next_cursor': 'AoJU5qbMMHLGm59N',
#                    'publishedfiledetails': [   {
#                       ...
#                           }],
#                   'total': 21488}}
while(params['cursor'] != last_cursor):
    result = requests.get(url, params=params)
    with open('%s/%s.json' % (output_dir, iterator), 'a') as fh:
        fh.write(result.text)

    json_result = json.loads(result.text)
    last_cursor = params['cursor']
    params['cursor'] = json_result['response']['next_cursor']
    iterator = iterator + 1
    print('[%s]: (%s/%s) %s' % (datetime.utcnow().isoformat(), iterator, params['cursor'], result.status_code))
    time.sleep(1)
   



