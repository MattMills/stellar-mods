from pprint import PrettyPrinter
from zipfile import ZipFile, Path
import json

pp = PrettyPrinter(width=10, indent=1)

alpha_lower = 'abcdefghijklmnopqrstuvwxyz'
alpha_upper = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
num = '0123456789'

alpha = alpha_lower + alpha_upper
alphanum = alpha + num

non_control_special = '.:_-'
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
                if self.comment and c not in newline:
                    local_buf += c
                elif c in control_special:
                    if c == '{':
                        self.depth += 1
                        self.this_state[local_buf_left] = {}
                        self.state_chain.append(self.this_state)
                        self.this_state = self.this_state[local_buf_left]
                        self.assignment = False
                    elif c == '}':
                        self.depth -= 1
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
                                print(self.buf)
                                print(self.root_state)
                                print(self.this_state)
                                print('-----------------------------------------------')
                        else:
                            self.this_state[local_buf_left].append(local_buf)

                        self.assignment = False
                        local_buf = ''
                    if self.comment == True and c == '\n':
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

                if self.depth == 0 and self.max_depth != 0:

                    section = self.buf[:x]
                    self.buf = self.buf[x+1:]

                    if self.enable_positional:
                        self.root_state['__position__'] = [self.fh_pos, len(section)]



                    self.fh_pos += x+1

                    if self.enable_text:
                        self.root_state['__text__'] = section
                    return json.dumps(self.root_state)

                x += 1

            new_buf = self.fh.read(1024).decode('utf-8')
            self.buf += new_buf


            if loops > 0 and len(self.buf.rstrip()) == 0:
                return
            elif loops > 1000:
                print(self.buf)
                print(local_buf)
                print('depth: %s, maxdepth: %s, loops: %s, buflen: %s' % (self.depth, self.max_depth, loops, len(self.buf)))

                raise Exception('Exceeded max loops, something is broken')
            else:
                loops += 1
                #print('depth: %s, maxdepth: %s, loops: %s, buflen: %s' % (self.depth, self.max_depth, loops, len(self.buf)))

            if len(new_buf) == 0 and self.buf[-1:1] != '\n':
                self.buf += '\n'




if __name__ == '__main__':
    with ZipFile('finished_mods/281990/8117eba5-3907-11ed-ae89-a502293073b2/281990_1121692237_814.zip') as modzip:
        for info in modzip.infolist():
            if('.txt' in info.filename and 'common' in info.filename):
                with modzip.open(info) as modfile:
                    p = parser(modfile)

                    section = ''

                    x = 0
                    try:
                        while section := p.get_section():
                            x+=1
                    finally:
                        print('file: %s parsed: %s' % (info.filename, x))


