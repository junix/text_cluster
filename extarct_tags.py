#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv, re, json, heapq
import os.path as osPath
import jieba
import jieba.analyse
import jieba.posseg as pseg
from db.mysqlExe import MysqlExe
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer

jieba.load_userdict('./dict_user.txt')
# jieba.enable_parallel()

class TagExtraction():
    def __init__(self, tableName, fileName):
        self.conn_with_slave = MysqlExe('172.17.128.172', 'yxt', 'pwdasdwx', 'skyeye')
        self.table = tableName
        self._dirname = osPath.dirname(osPath.abspath(__file__))
        self.records_path = osPath.join(self._dirname, self.table, 'records.csv')
        self.tags_path = osPath.join(self._dirname, self.table, fileName)

    def fetch_text(self):
        sql = """
            SELECT
                productId id,
                productName,
                description,
                directoryName
            FROM
                {}
        """.format(self.table)
        records = self.conn_with_slave.fetch_records(sql)

        # records = [
        #     {'id': '1',
        #      'productname': '谁是受欢迎的上司',
        #      'description': '在对待下属员工时，如果能够多一些尊重，不吝啬自己的安慰和激励，相信你肯定会成为颇受欢迎的上司。',
        #      'directoryname': '人际沟通'},
        #     {'id': '2',
        #      'productname': '如何在组织内与90后建立良好的关系？',
        #      'description': '管理者与90后普遍缺乏切中要点的真诚沟通，各自都筑起高墙，关系逐渐撕裂。消除“刻板印象”，减少各自发出的干扰“噪音”，增强有效沟通，帮助90后彻底融入团队，建立可靠的心灵默契。',
        #      'directoryname': '员工管理'},
        #     {'id': '3',
        #      'productname': '如何帮助90后尽快成为合格员工',
        #      'description': '新90后员工的过度热情往往让很多管理者倍感压力。到底要给他们泼冷水还是给他们“挖渠”？90后在呵护备至的环境中成长，出来社会需要领导帮助树立正确的角色感，才能充分发挥其自身价值。',
        #      'directoryname': '培养下属'},
        #     {'id': '4',
        #      'productname': 'Excel效率筛选数据',
        #      'description': '教你以最快的速度，找出自己想要的数据，筛选后一目了然。',
        #      'directoryname': 'office办公'},
        #     {'id': '5',
        #      'productname': 'Excel数据处理技巧',
        #      'description': '为日常工作中大量的数据处理提供便捷的方式，以清晰的步骤学会数据处理的技巧。',
        #      'directoryname': 'office办公'},
        #     {'id': '6',
        #      'productname': 'iOS培训开发视频教程-UI教程',
        #      'description': '业内最具深度免费《iOS开发视频教程》全套ios视频教程，是千锋教育欧阳大神又一力作。本视频集为第1季，包括iOS开发中的UI开发视频的UILabel、UIView、UITextField、UIWindows、UIViewController、UIButton、UIImage、UIView、UIToolbar等各种控件高级用法，后面将会持续更新iOS开发视频教程，尽请期待！',
        #      'directoryname': '移动开发'},
        #     {'id': '7',
        #      'productname': '风险管理',
        #      'description': '本课程针对《风险管理》（2013版）教材，针对2015年考试，讲解教材所有考点并补充常考但教材没有的知识，通过大量真题练习，强化知识点的记忆，原题命中率高达30%以上，考点命中率80%左右。',
        #      'directoryname': '银行'},
        #     {'id': '8',
        #      'productname': '互联网创新商业模式【传统企业升级电商6-6】',
        #      'description': '新创企业的颠覆式定位图、低端市场破坏性创新策略和新市场破坏性创新策略；网络效应、开放策略和管制策略、平台覆盖等揭示360、余额宝、微信、天猫等品牌迅速成功奥秘。',
        #      'directoryname': '互联网营销'},
        #     {'id': '9',
        #      'productname': '丁捷：用互联网思维重构学习生态圈',
        #      'description': '在互联网大环境下，学习生态圈要走出自嗨的状态，与客户用户建立更强大的交流，必须带着我们的用户一起玩。',
        #      'directoryname': '培训与发展'},
        #     {'id': '10',
        #      'productname': '财务人员的角色和定位',
        #      'description': '当今的财务管理者需要掌握哪些核心能力和专业技能？不同能力与财务各项工作有什么样的对应关系？在整个财务体系里又呈现怎样的分布？欢迎收看本期课程！',
        #      'directoryname': '财务综合'},
        # ]
        return records

    def clean_text(self, records):
        pattern = re.compile(r'[\s+\d+\.\!\?\/_,$%^*()+\"\']+|[+——！，。？、~@#￥%……&*（）【】《》“”：；]')
        for record in records:
            for (k, v) in record.items():
                if k == 'id' or k == 'directoryname':
                    continue

                try:
                    words = self.cut_sentence_POS(re.sub(pattern, ' ', v))
                    record[k] = ' '.join([word for word, flag in words if len(word) > 1 and flag in ('n', 'ns', 'nt', 'nr', 'nx', 'vn', 'j', 'l', 'eng')])
                    record[k] = re.sub(r'\s+', ' ', record[k])
                except Exception as e:
                    # print('fetch_text(): ', e)
                    record[k] = ''
        return records


    def cut_sentence_POS(self, sentence):
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
            f_csv = csv.DictReader(f)
            records = [row for row in f_csv]
        return records

    def extract_tags(self, records, k):
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

        print(len(tags))
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



