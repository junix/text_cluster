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

    def fetch_text(self):
        sql = """
            SELECT
                productId id,
                productName,
                description,
                directoryName
            FROM
                {table}
        """.format(table=self.table)
        records = self.conn_with_slave.fetch_records(sql)

        # records = [
        #   {'id': '1',
        #    'productname': '谁是受欢迎的上司',
        #    'description': '...',
        #    'directoryname': '人际沟通'}
        # ]
        return records

    def clean_text(self, records):
        pattern = re.compile(r'[\s+\d+\.\!\?\/_,$%^*()+\"\']+|[+——！，。？、~@#￥%……&*（）【】《》“”：；]')
        for record in records:
            for (k, v) in record.items():
                if k == 'id' or k == 'directoryname':
                    continue

                try:
                    words = self.cut_sentence_pos(re.sub(pattern, ' ', v))
                    word_classes = ('n', 'ns', 'nt', 'nr', 'nx', 'vn', 'j', 'l', 'eng')
                    words = [word for word, flag in words if len(word) > 1 and flag in word_classes]
                    record[k] = ' '.join(words)
                    record[k] = re.sub(r'\s+', ' ', record[k])
                except Exception as e:
                    print('fetch_text(): ', e)
                    record[k] = ''
        return records

    @classmethod
    def cut_sentence_pos(cls, sentence):
        # return jieba.cut(sentence)
        return pseg.cut(sentence)

    def dump_records(self):
        records = self.clean_text(self.fetch_text())
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
        transformer = TfidfTransformer()

        corpus = [','.join([record['productname'], record['description'], record['directoryname']]) for record in records]
        tfidf = vectorizer.fit_transform(corpus) # transformer.fit_transform(vectorizer.fit_transform(corpus))

        tfidf = tfidf.toarray()
        featureNames = vectorizer.get_feature_names()
        tags = featureNames
        # tags = set()
        # for weight in tfidf:
        #     _ = list(zip(weight.tolist(), featureNames))
        #     _topTags = heapq.nlargest(k, _, key=lambda x:x[0])
        #     tags.update([pair[1] for pair in _topTags if pair[0]!=0])

        return list(tags)

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
    tag_extraction = TagExtraction('product', 'tags.json')
    tag_extraction.dump_records()
    tag_extraction.dump_tags(10000)

    # from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
    #
    # records = tag_extraction.read_records()
    # corpus = [record['description'] for record in records]
    # ids = [record['id'] for record in records]
    #
    # vectorizer = CountVectorizer()
    # transformer = TfidfTransformer()
    #
    # tfidf = transformer.fit_transform(vectorizer.fit_transform(corpus))
    # word = vectorizer.get_feature_names()
    # weight = tfidf.toarray()
    #
    # vectors = {ids[index]: weight[index].tolist() for index in range(len(ids))}
    # with open(tag_extraction.tags_path, 'w', encoding='utf-8') as f:
    #         f.write(json.dumps(vectors, ensure_ascii=False))



