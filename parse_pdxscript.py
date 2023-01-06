from zipfile import ZipFile, Path, BadZipFile
import json
import os
import io


alpha_lower = 'abcdefghijklmnopqrstuvwxyz'
alpha_upper = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
num = '0123456789'

alpha = alpha_lower + alpha_upper
alphanum = alpha + num

non_control_special = '.:_-/!@$%^&*()'
control_special = '{}="'
comment = '#'
newline = '\n\r'

whitespace = ' \t' + newline

all_non_control = alphanum+non_control_special 


# parser for PDXScript
class parser:
    # fh = file handle to open script file
    # enable_positional = Turn on/off source file position detail in output (prints x position from original file in state for each section, used for contextual highlighting)

    def __init__(self, fh, enable_positional = True, enable_text = True):
        self.fh = fh
        self.file_size = -1
        self.enable_positional = enable_positional
        self.enable_text = enable_text

        self.fh_pos = 0

        self.buf = ''
        self.depth = 0
        self.max_depth = 0

        self.assignment = False
        self.quote = False
        self.comment = False

        self.root_state = {}
        self.state_chain = []
        self.this_state = self.root_state
        self.comment_scope = None

    # Parses a zip filename out of the input filename
    def get_section(self):

        loops = 0
        x = 0
        local_buf = ''
        local_buf_left = ''
        local_buf_right = ''

        self.depth = 0
        self.max_depth = 0
        self.root_state = {}
        self.state_chain = []
        self.this_state = self.root_state

        while True:
            while x < len(self.buf):
                c = self.buf[x]

                if len(local_buf) > 0 and self.comment == False and self.quote == False and self.assignment == False and c in whitespace+'}#':
                    #this entire section is to support unquoted lists within {} ie, { a\nb\nc\n }
                    x_backtrack = x
                    while x_backtrack > 0 and self.buf[x_backtrack] in whitespace:
                        x_backtrack -= 1

                    x_forwardtrack = x
                    while x_forwardtrack < len(self.buf)-1 and self.buf[x_forwardtrack] in whitespace:
                        x_forwardtrack += 1

                    if self.buf[x_backtrack] in all_non_control+'"{#' and self.buf[x_forwardtrack] != '=':
                        local_buf_left = '__no_assignment__'
                        if local_buf_left not in self.this_state:
                            self.this_state[local_buf_left] = []

                        self.this_state[local_buf_left].append((x, local_buf))
                        local_buf = ''

                if self.comment and c not in newline:
                    local_buf += c
                elif c in control_special:
                    if c == '{':
                        self.depth += 1
                        if local_buf_left not in self.this_state:
                            try:
                                self.this_state[local_buf_left] = []
                            except:
                                print('---------------------------------------')
                                print(self.root_state)
                                print(self.buf)
                                print(local_buf_left)
                                print('---------------------------------------')
                                raise

                        self.state_chain.append(self.this_state)
                        self.this_state = self.this_state[local_buf_left]

                        self.this_state.append({'__position__': x})
                        self.state_chain.append(self.this_state)
                        self.this_state = self.this_state[-1]

                        self.assignment = False
                    elif c == '}':
                        self.depth -= 1
                        try:
                            self.this_state = self.state_chain.pop()
                        except IndexError  as e:
                            if '__errors__' not in self.root_state:
                                self.root_state['__errors__'] = []
                            self.root_state['__errors__'].append('Unbalanced {}, (too many!), position: %s x: %s' % (self.fh_pos, x ))
                            #print(self.root_state)
                            #print(self.this_state)
                            #print('%s %s' % (self.depth, self.max_depth))

                        if type(self.this_state) != type(dict()): 
                            self.this_state = self.state_chain.pop()
                    elif c == '=':
                        local_buf_left = local_buf
                        local_buf = ''
                        self.assignment = True
                    elif c == '"':
                        if self.quote == False:
                            self.quote = True
                        else:
                            if self.buf[x-1] == "\\":
                                x += 1
                                continue
                            #closing quote
                            self.quote = False
                            if self.assignment != True:
                                local_buf_left = '__no_assignment__'
                            if local_buf_left not in self.this_state:
                                self.this_state[local_buf_left] = []

                            if self.enable_positional:
                                self.this_state[local_buf_left].append((x, '"' + local_buf + '"'))
                            else:
                                self.this_state[local_buf_left].append('"' + local_buf + '"')

                            self.assignment = False
                            local_buf = ''
                elif c in all_non_control:
                    local_buf += c
                elif c in whitespace:
                    if self.assignment == True and self.quote == False and len(local_buf.strip()) > 0:

                        if local_buf_left not in self.this_state:
                            self.this_state[local_buf_left] = []

                        if self.enable_positional:
                            try:
                                self.this_state[local_buf_left].append((x, local_buf))
                            except Exception as e:
                                print('-----------------------------------------------')
                                print('Exception: %s - %s' % (type(e), e))
                                print(x)
                                print(local_buf)
                                print(self.buf.rstrip())
                                print(self.root_state)
                                print(self.this_state)
                                print('-----------------------------------------------')
                        else:
                            self.this_state[local_buf_left].append(local_buf)

                        self.assignment = False
                        local_buf = ''
                    if self.comment == True and (c == '\n' or ((len(self.buf.rstrip()) == 0 or 1+x+self.fh_pos >= len(self.buf)))):
                        if 'comments' not in self.this_state:
                            self.this_state['comments'] = []
                        if self.enable_positional:
                            self.this_state['comments'].append((x,local_buf))
                        else:
                            self.this_state['comments'].append(local_buf)
                        self.comment = False
                        local_buf = ''
                elif c in comment and not self.quote:
                    self.comment = True
                    local_buf += c



                if self.depth > self.max_depth:
                    self.max_depth = self.depth
    
                if self.max_depth == 0 and c == "\n" and x > 1:
                    self.max_depth += 1

                if (self.depth <= 0 and self.max_depth != 0): # or (x+self.fh_pos >= self.file_size) :
                    #36 11 0 37
                    #24 23 12 37
                    #print('%s %s %s %s' % (len(self.buf), x, self.fh_pos, self.file_size))
                    section = self.buf[:x+1]
                    self.buf = self.buf[x+1:]

                    if self.enable_positional:
                        self.root_state['__position__'] = [self.fh_pos, len(section)]



                    self.fh_pos += x+1

                    if self.enable_text:
                        self.root_state['__section_text__'] = section
                    return self.root_state

                x += 1


            read_length = 1024
            new_buf = None
            for attempt in range(1,10):
                try:
                    new_buf = self.fh.read(read_length)
                    break
                except UnicodeError:
                    read_length += 1
                except UnicodeError:
                    read_length += 1
                except:
                    raise


            if new_buf == None:
                new_buf = self.fh.read(read_length)
            self.buf += new_buf


            if self.root_state != {} and loops > 0 and (len(self.buf.rstrip()) == 0 or 1+x >= len(self.buf)):
                section = self.buf
                self.buf = self.buf[x+1:]

                if self.enable_positional:
                    self.root_state['__position__'] = [self.fh_pos, len(section)]

                self.fh_pos += x+1

                if self.enable_text:
                    self.root_state['__section_text__'] = section
                return self.root_state
            elif self.root_state == {} and loops > 0 and (len(self.buf.rstrip()) == 0 or 1+x+self.fh_pos >= len(self.buf)):
                return
            elif loops > 100000:
                print('---------------------------------')
                print(self.buf)
                print(local_buf)
                print(self.root_state)
                print('depth: %s, maxdepth: %s, loops: %s, buflen: %s, x: %s, fh_pos: %s, file_size: %s' % (self.depth, self.max_depth, loops, len(self.buf), x, self.fh_pos, self.file_size))
                print('----------------------------------')

                raise Exception('Exceeded max loops, something is broken')
            else:
                loops += 1
                #print('depth: %s, maxdepth: %s, loops: %s, buflen: %s' % (self.depth, self.max_depth, loops, len(self.buf)))

            if len(new_buf) == 0:
                if self.buf[-1:] != '\n':
                    self.buf += '\n\n'

            if len(new_buf) == 0 and self.depth > 0:
                self.buf += '}\n\n'
                if '__errors__' not in self.root_state:
                    self.root_state['__errors__'] = []
                self.root_state['__errors__'].append('Reached EOF with unbalanced {}, added closing } x1, position: %s' % (self.fh_pos, ))


def check_path_pdxscript_txt(filename):
    if filename[-4:].lower() != '.txt':
        return False

    dir_parts = filename.split('/')

    if dir_parts[0] == 'common':
        return True
    if dir_parts[0] == 'events':
        return True
    if dir_parts[0] == 'flags':
        return True
    if dir_parts[0] == 'map':
        return True
    if dir_parts[0] == 'music':
        return True
    if dir_parts[0] == 'prescripted_countries':
        return True

    if dir_parts[0] == 'interface' and dir_parts[1] == 'resource_groups':
        return True

    if dir_parts[0] == 'gfx':
        if dir_parts[1] == 'advisorwindow':
            return True
        if dir_parts[1] == 'pingmap':
            return True
        if dir_parts[1] == 'portraits':
            if dir_parts[2] == 'asset_selectors':
                return True
            if dir_parts[2] == 'portraits':
                return True
        if dir_parts[1] == 'projectiles':
            return True
        if dir_parts[1] == 'shipview':
            return True
        if dir_parts[1] == 'worldgfx':
            return True


    

    return False

def parse_zip_file(stats, zip_filename, target_files, enable_position = True, enable_text = True):

    with ZipFile(zip_filename) as modzip:
        for target_file in target_files:
            if not check_path_pdxscript_txt(target_file):
                stats['skipped_files'] += 1
                continue
            with modzip.open(target_file) as modfile:
                with io.TextIOWrapper(modfile, encoding='utf-8-sig', errors='replace') as wrappedfile:
                    p = parser(wrappedfile,  enable_position, enable_text)

                    section = ''

                    x = 0
                    try:
                        while section := p.get_section():
                            print('%s %s %s' % (target_file, x, section))
                            x+=1
                            yield {'filename': target_file, 'order': x, 'section': section }
                    except Exception as e:
                        print('EXCEPTION! %s - %s - file: %s parsed: %s zip: %s' % (type(e), e, target_file, x, zip_filename))
                        raise
                    finally:
                        stats['total_sections'] += x


if __name__ == '__main__':
    #zip_filename = 'finished_mods/281990/8117eba5-3907-11ed-ae89-a502293073b2/281990_1121692237_814.zip'

    stats = {}


    stats['total_rows'] = 0;
    stats['total_zip_files'] = 0;
    stats['skipped_mods'] = 0;
    stats['error_zip_files'] = 0;


    stats['total_sections'] = 0;
    stats['total_files'] = 0;
    stats['total_parsed_files'] = 0
    last_file = ''

    start_path = 'finished_mods/281990/'
    dirs = os.listdir(start_path)
    for uuid in dirs:
        files = os.listdir('%s%s' % (start_path, uuid))
        for this_file in files:
            if this_file[-4:].lower() == '.zip':
                try:
                    zip_filename = '%s%s/%s' % (start_path, uuid, this_file)
                    parse_zip_file(stats, zip_filename)
                except BadZipFile as e:
                    print('BadZip: size: %s\t%s' % (os.path.getsize(zip_filename), zip_filename))
                except:
                    print('%s %s' % (zip_filename, last_file))
                    raise

    print(stats)


