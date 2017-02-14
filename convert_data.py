#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os.path as osPath
import json
import numpy as np
import matplotlib.pyplot as plt
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.decomposition import PCA
from sklearn.preprocessing import normalize
from mpl_toolkits.mplot3d import Axes3D


class ConvertData():
    def __init__(self, tableName, tags=[]):
        self.tags = tags
        self.table = tableName
        self._dirname = osPath.dirname(osPath.abspath(__file__))
        self.vectors_path = osPath.join(self._dirname, self.table, 'vectors.json')

    def strings_to_vectors(self, corpus, k=5):
        vectorizer = CountVectorizer(vocabulary=self.tags)
        # transformer = TfidfTransformer()
        tfidf = vectorizer.fit_transform(corpus) # transformer.fit_transform(vectorizer.fit_transform(corpus))
        # tfidf = normalize(tfidf, norm='l2')

        weight = tfidf.toarray()
        # weight = np.where(weight!=0, 1, 0)
        # for vector in weight:
        #     threshold = 0
        #     try:
        #         threshold = sorted(vector, reverse=True)[k]
        #     except:
        #         pass
        #
        #     vector[vector <= threshold] = 0

        # pca = PCA(n_components=3)
        # newData = pca.fit_transform(weight)
        #
        # x =[]
        # y =[]
        # z =[]
        # i=0
        # while i<len(newData):
        #     x.append(newData[i][0])
        #     y.append(newData[i][1])
        #     z.append(newData[i][2])
        #     i +=1
        #
        # fig = plt.figure()
        # ax = fig.add_subplot(111, projection='3d')
        # ax.scatter(x, y, z)
        # plt.show()

        return weight.tolist()

    def dump_vectors(self, records):
        corpus_id = [[record['id'], record['description']] for record in records]
        ids = [record[0] for record in corpus_id]
        corpus = [record[1] for record in corpus_id]

        weight = self.strings_to_vectors(corpus)
        vectors = dict(zip(ids, weight))

        with open(self.vectors_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(vectors))

    def read_vectors(self):
        with open(self.vectors_path, encoding='utf-8') as f:
            return json.loads(f.read())


if __name__ == '__main__':
    from extarct_tags import TagExtraction

    # from sklearn.decomposition import LatentDirichletAllocation

    tag_extraction = TagExtraction('product', 'tags.json')
    tags = tag_extraction.read_tags()
    samples = tag_extraction.read_records()
    corpus_id = [[sample['id'], ' '.join([sample['directoryname']] + [sample['productname']] + [sample['description']])]
                 for sample in samples]
    ids = [record[0] for record in corpus_id]
    corpus = [record[1] for record in corpus_id]

    dataConverter = ConvertData('product', tags)
    dataConverter.dump_vectors(samples)

    # tf_vectorizer = CountVectorizer(vocabulary=tags)
    # tf = tf_vectorizer.fit_transform(corpus)
    #
    # lda = LatentDirichletAllocation(n_topics=20, max_iter=50)
    # lda.fit(tf)
    #
    # def print_top_words(model, feature_names, n_top_words):
    #     for topic_idx, topic in enumerate(model.components_):
    #         print("Topic #%d:" % topic_idx)
    #         print(" ".join([feature_names[i]
    #                         for i in topic.argsort()[:-n_top_words - 1:-1]]))
    #     print()
    #
    # tf_feature_names = tf_vectorizer.get_feature_names()
    # print_top_words(lda, tf_feature_names, 20)
