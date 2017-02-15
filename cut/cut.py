#!/usr/bin/env python

import sys
from content_extractor import ContentExtractor
if __name__ == '__main__':
    files = sys.argv[1:]
    if not files:
        print('usage: cut files')
        sys.exit(1)
    for f in files:
        c = ContentExtractor(f)
        c.dump_cut()
