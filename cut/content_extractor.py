import jieba


class ContentExtractor:
    def __init__(self, file_path, encoding=None, content_anchor=None, trim_head='', trim_tail=''):
        self.file_path = file_path
        with open(file_path, 'r', encoding=encoding) as f:
            if content_anchor:
                head_len = len(trim_head)
                tail_len = len(trim_tail)
                self.data = [l[head_len:-tail_len] for l in f.readlines() if content_anchor in l]
            else:
                self.data = f.readlines()

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
