#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv, re, json, heapq
import os.path as op
import jieba.analyse
import jieba.posseg as pseg
from db.mysqlExe import MysqlExe
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer

jieba.load_userdict('./dict_user.txt')
# jieba.enable_parallel()


class TagExtraction:
    def __init__(self, table_name, file_name):
        self.conn_with_slave = MysqlExe('172.17.128.172', 'yxt', 'pwdasdwx', 'skyeye')
        self.table = table_name
        self._dirname = op.dirname(op.abspath(__file__))
        self.records_path = op.join(self._dirname, self.table, 'records.csv')
        self.tags_path = op.join(self._dirname, self.table, file_name)

    def fetch_record_from_repo(self):
        sql = """
            SELECT
                productId id,
                productName,
                description,
                directoryName
            FROM
                {table}
        """.format(table=self.table)
        # fmt: {'id': '1', 'productname': '谁是受欢迎的上司','description': '...', 'directoryname': '人际沟通'}
        return self.conn_with_slave.fetch_records(sql)

    def filter_pos(self, records):
        pattern = re.compile(r'[\s+\d+\.\!\?\/_,$%^*()+\"\']+|[+——！，。？、~@#￥%……&*（）【】《》“”：；]')
        for r in records:
            for (k, v) in r.items():
                if k == 'id' or k == 'directoryname':
                    continue

                try:
                    words = self.cut_sentence_pos(re.sub(pattern, ' ', v))
                    word_pos = ('n', 'ns', 'nt', 'nr', 'nx', 'vn', 'j', 'l', 'eng')
                    words = [word for word, flag in words if len(word) > 1 and flag in word_pos]
                    r[k] = ' '.join(words)
                    r[k] = re.sub(r'\s+', ' ', r[k])
                except Exception as e:
                    print('fetch_text(): ', e)
                    r[k] = ''
        return records

    @classmethod
    def cut_sentence_pos(cls, sentence):
        # return jieba.cut(sentence)
        return pseg.cut(sentence)

    def dump_records(self):
        records = self.filter_pos(self.fetch_record_from_repo())
        rows = records
        headers = rows[0].keys()

        with open(self.records_path, 'w', encoding='utf-8') as f:
            f_csv = csv.DictWriter(f, headers)
            f_csv.writeheader()
            f_csv.writerows(rows)

    def read_record(self, id):
        with open(self.records_path, encoding='utf-8') as f:
            f_csv = csv.DictReader(f)
            record = ''
            for row in f_csv:
                if row['id'] == id:
                    record = row
            return record

    def read_records(self):
        with open(self.records_path, encoding='utf-8') as f:
            records = [r for r in csv.DictReader(f)]
        return records

    @classmethod
    def extract_tags(cls, records, k):
        # sentence = '\n'.join([','.join([record['productname'], record['description']]) for record in records])
        # tags = jieba.analyse.extract_tags(sentence, topK=k, allowPOS=('n', 'ns', 'nt', 'nr', 'nx', 'vn', 'j', 'l'))
        #
        # directory = [record['directoryname'] for record in records if record['directoryname']]
        # tags = list(set(directory)) + tags
        # return list(set(tags))

        vectorizer = CountVectorizer()

        corpus = [','.join([r['productname'], r['description'], r['directoryname']]) for r in records]
        vectorizer.fit_transform(corpus)

        # transformer = TfidfTransformer()
        # transformer.fit_transform(vectorizer.fit_transform(corpus))

        return list(vectorizer.get_feature_names())

    def read_tags(self):
        with open(self.tags_path, encoding='utf-8') as f:
            return json.loads(f.read())

    def dump_tags(self, k):
        # records = self.fetch_text()
        records = self.read_records()
        tags = self.extract_tags(records, k)
        with open(self.tags_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(tags, ensure_ascii=False))


if __name__ == '__main__':
    e = TagExtraction('product', 'tags.json')
    e.dump_records()
    e.dump_tags(10000)
