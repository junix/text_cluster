import jieba


class ContentExtractor:
    def __init__(self, file_path, encoding=None, content_anchor=None, trim_head_len=0, trim_tail_len=0):
        self.file_path = file_path
        self.trim_head = trim_head_len
        self.trim_tail = trim_tail_len
        self.content_anchor = content_anchor
        with open(file_path, 'r', encoding=encoding) as f:
            self.data = f.readlines()
        self.clean()

    def clean(self):
        if self.content_anchor:
            self.data = [
                l[self.trim_head:-self.trim_tail]
                for l in self.data if self.content_anchor in l]

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
