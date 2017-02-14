#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os.path as op
import numpy as np
from sklearn.cluster import KMeans
from sklearn.decomposition import LatentDirichletAllocation
from convert_data import ConvertData
from extarct_tags import TagExtraction


def print_top_words(model, feature_names, n_top_words):
    for topic_idx, topic in enumerate(model.components_):
        print("Topic #%d:" % topic_idx)
        print(" ".join(
            [feature_names[i] for i in topic.argsort()[:-n_top_words - 1:-1]]))
    print()


def dump_cluster(label, tags, records):
    pwd = op.dirname(op.abspath(__file__))
    with open(op.join(pwd, 'product', 'clusters', str(label) + '_tags.json'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(tags, ensure_ascii=False))

    with open(op.join(pwd, 'product', 'clusters', str(label) + '_records.json'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(records, ensure_ascii=False))


if __name__ == '__main__':
    tagExtraction = TagExtraction('product', 'tags.json')
    converter = ConvertData('product')

    tags = tagExtraction.read_tags()
    records = tagExtraction.read_records()
    records = {record['id']: [record['productname'], record['description'], record['directoryname']] for record in records}

    vectors_id = converter.read_vectors()
    ids = list(vectors_id.keys())
    vectors = list(vectors_id.values())

    lda = LatentDirichletAllocation(n_topics=100)
    lda_vectors = lda.fit_transform(vectors)
    print_top_words(lda, tags, 20)

    cls = KMeans(n_clusters=500).fit(lda_vectors)
    labels = cls.labels_
    label_map = dict(zip(ids, labels))

    tagExtraction = TagExtraction('product', 'tags.json')
    tags = tagExtraction.read_tags()
    records = tagExtraction.read_records()
    records = {record['id']: [record['productname'], record['description']] for record in records}

    output = {}
    for k, v in label_map.items():
        if v in output:
            output[v].append(k)
        else:
            output[v] = [k]

    for k, v in output.items():
        print(k, len(v))

        sum = np.zeros(len(vectors[0]))
        for id in v:
            na = np.array(vectors_id[id], dtype=np.float64)
            na[na != 0] = 1
            sum += na
        sum = sum.tolist()

        _tags = [(tag, count) for tag, count in zip(tags, sum) if count != 0]
        _records = [[id] + records[id] for id in v]
        # _records = [[id] + records[id] + [[(index, value) for index,value in enumerate(vectors_id[id]) if value != 0]] for id in v]
        # _records = [vectors_id[id] for id in v]
        dump_cluster(k, sorted(_tags, key=lambda x: x[1], reverse=True), _records)
        # print([tags[index] for index, count in enumerate(sum) if count > len(v)*0.2])



    # while(True):
    #     productId = input('productId\n')
    #     label = label_map[productId]
    #     indexs = [index for index, item in enumerate(labels) if item == label]
    #     for index in indexs:
    #         print(records[ids[index]])
