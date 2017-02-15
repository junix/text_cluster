import os
import jieba


class ContentExtractor:
    @classmethod
    def cut_dir_and_dump(cls, dirname='/Users/junix/ml/data/txt', encoding='gb18030'):
        for root, _, files in os.walk(dirname):
            for f in files:
                if f.endswith('.txt'):
                    path = '{root}/{file}'.format(root=root, file=f)
                    c = ContentExtractor(path, encoding=encoding)
                    c.dump_cut()

    def __init__(self, file_path, encoding=None):
        self.file_path = file_path
        with open(file_path, 'r', encoding=encoding) as f:
            head_len = len('<content>')
            tail_len = len('</content>')
            self.data = [l[head_len:-tail_len] for l in f.readlines() if '<content>' in l]

    def __repr__(self):
        return self.file_path

    def cut(self):
        for l in self.data:
            yield (' '.join(list(jieba.cut(l))))

    def dump_cut(self):
        cut_file = '{base}.cut'.format(base=self.file_path)
        with open(cut_file, 'w') as f:
            for c in self.cut():
                f.write(c)
                f.write(' ')
