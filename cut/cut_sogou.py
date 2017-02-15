import os
from cut.content_extractor import ContentExtractor

if __name__ == 'main':
    dirname = '/Users/junix/ml/data/txt'
    for root, _, files in os.walk(dirname):
        for f in files:
            if f.endswith('.txt'):
                path = '{root}/{file}'.format(root=root, file=f)
                c = ContentExtractor(
                    path,
                    encoding='gb18030',
                    content_anchor='<content>',
                    trim_head='<content>',
                    trim_tail='</content>')
                c.dump_cut()
