#!/usr/bin/python3

import re
import os

workshop_log = '/zpool0/store0/steam_local/logs/workshop_log.txt'
published_ids = 'published_file_ids.txt'
completed_ids = 'completed_file_ids.txt'
script = 'steamcmd_script.txt'

command_prefix = 'download_item 281990 '
command_suffix = ''

completed_id_list = []

with open(workshop_log) as fh:
    compiled_re = re.compile(r"Download item\s([0-9]*)\sresult\s:\sOK$")
    for line in fh:
        results = compiled_re.findall(line)
        if len(results) > 0:
            completed_id_list.append(results[0])


with open(published_ids) as fh:
    with open(completed_ids, 'a') as fh2:
        with open('%s_updateinprogress' % published_ids, 'w') as fh_new:
            for line in fh:
                if line.strip() in completed_id_list:
                    print("Completed %s" % line.strip())
                    fh2.write(line)
                else:
                    fh_new.write(line)

os.remove(published_ids)
os.rename('%s_updateinprogress' % published_ids, published_ids)

with open(published_ids) as fh:
    with open(script, 'w') as fh_script:
        for line in fh:
            fh_script.write('%s%s%s\n' % (command_prefix, line.strip(), command_suffix))
    
