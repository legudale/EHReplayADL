import re
import sys

current_avro = None
with open(sys.argv[1]) as f:
    for line in f:
        avro_rx = 'processing (.*)...'
        m = re.match(avro_rx, line)
        if m:
            current_avro = m.group(1)

        if 'exceeds the limit' in line and current_avro:
            print(current_avro)
            current_avro = None
