"""
utils.py
住所コード変換関連Utils
"""

#ログ関連設定
import os
import pathlib
import logging.config
logging.config.fileConfig(pathlib.Path(__file__).parent / 'config/logging.ini')
logger = logging.getLogger("bizmerge")

import os
import codecs
from gensim.models import word2vec
import janome.tokenizer
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelBinarizer

class TextToArrays:
    """
    文字列ベクトル化クラス
    """

    maxlen = 0
    tokenizer = None
    word2vecModel = None

    def __init__(self, maxlen=10):
        """
        __init__メソッド

        Args:
            maxlen (int): 最大文字列の長さ
        """

        self.maxlen = maxlen
        #20200917
        self.tokenizer = janome.tokenizer.Tokenizer(str(pathlib.Path(__file__).parent / 'data/東京都住所コード辞書.csv'), udic_enc='utf8')

    def fit_from_file(self, file_path):
        logger.info('住所データを読み込みます')
        #住所データを読み込む
        data = pd.read_csv(file_path, sep='\t', encoding='utf-8', dtype=object, usecols=['住所'])

        sentences = data['住所'].values

        logger.info('住所データをベクトル化します')
        #20200917
        tokenizer = janome.tokenizer.Tokenizer(str(pathlib.Path(__file__).parent / 'data/東京都住所コード辞書.csv'), udic_enc='utf8')
        texts = [tokenizer.tokenize(s, wakati=True) for s in sentences]

        logger.info('住所データモデルを作成します')
        #住所コードを元にベクトル化モデルを作成する
        self.word2vecModel = word2vec.Word2Vec(texts, min_count=1)

    def save(self):
        #モデルを保存する
        logger.info('住所データモデルを保存します')
        self.word2vecModel.save(str(pathlib.Path(__file__).parent / 'model/01_住所ベクトル化.model'))

    def load(self):
        #モデルを読み込む
        logger.info('住所データモデルを読み込みます')
        self.word2vecModel = word2vec.Word2Vec.load(str(pathlib.Path(__file__).parent / 'model/01_住所ベクトル化.model'))

    def word2vec(self, text:str):
        """
        文字列をベクトル化する

        Args:
            text (str): 文字列

        Returns:
            ベクトル化された文字列
        """

        #文字列を分かち書きして単語に分離す津
        wordList = self.tokenizer.tokenize(text, wakati=True)
        #最大長さより大きい単語は捨てる
        if len(wordList) > self.maxlen:
            del wordList[self.maxlen:]

        vectorList = []
        #単語をベクトル化する
        for word in wordList:
            try:
                vectorList.append(self.word2vecModel.wv[word])
            except KeyError:
                #logger.warn('辞書データから[%s]が存在しません', word)
                vectorList.append(np.zeros(100, dtype=np.float32))

        #ベクトル化した長さが最大長さ未満の場合、0を埋めて固定長にする
        if len(vectorList) < self.maxlen:
            for _ in range(self.maxlen - len(vectorList)):
                vectorList.append(np.zeros(100, dtype=np.float32))

        return vectorList

class AddressCodeBinarizer:
    """
    住所コードOne-Hotエンコーディング／デコーディングクラス
    """

    file_path:str = None

    #都道府県コード
    TDFKN_CD_LabelBinarizer:LabelBinarizer = None
    #市区町村コード
    SCYOSN_CD_LabelBinarizer:LabelBinarizer = None
    #大字通称コード
    OAZA_TSHUM_CD_LabelBinarizer:LabelBinarizer = None
    #字丁目コード
    AZCHM_CD_LabelBinarizer:LabelBinarizer = None
    #住所コード
    ADDR_CD_LabelBinarizer:LabelBinarizer = None
    #郵便番号
    ZIPCODE_LabelBinarizer:LabelBinarizer = None

    TDFKN_CD_LEN:int = 0
    SCYOSN_CD_LEN:int = 0
    OAZA_TSHUM_CD_LEN:int = 0
    AZCHM_CD_LEN:int = 0
    ADDR_CD_LEN:int = 0
    ZIPCODE_LEN:int = 0

    def __init__(self, file_path:str, maxlen:int=10):
        """
        __init__メソッド

        Args:
            maxlen (int): 最大文字列の長さ
        """
        self.file_path = file_path
        self.TDFKN_CD_LabelBinarizer = LabelBinarizer()
        self.SCYOSN_CD_LabelBinarizer = LabelBinarizer()
        self.OAZA_TSHUM_CD_LabelBinarizer = LabelBinarizer()
        self.AZCHM_CD_LabelBinarizer = LabelBinarizer()
        self.ADDR_CD_LabelBinarizer = LabelBinarizer()
        self.ZIPCODE_LabelBinarizer = LabelBinarizer()

        addresscode_list = pd.read_csv(self.file_path, sep='\t', encoding='utf-8', dtype=object, usecols=['都道府県コード','市区町村コード','大字通称コード','字丁目コード','住所コード','郵便番号'])

        self.TDFKN_CD_LabelBinarizer.fit(np.unique(addresscode_list['都道府県コード'].astype(str).values))
        self.TDFKN_CD_LEN = len(self.TDFKN_CD_LabelBinarizer.classes_)

        self.SCYOSN_CD_LabelBinarizer.fit(np.unique(addresscode_list['市区町村コード'].astype(str).values))
        self.SCYOSN_CD_LEN = len(self.SCYOSN_CD_LabelBinarizer.classes_)

        self.OAZA_TSHUM_CD_LabelBinarizer.fit(np.unique(addresscode_list['大字通称コード'].astype(str).values))
        self.OAZA_TSHUM_CD_LEN = len(self.OAZA_TSHUM_CD_LabelBinarizer.classes_)

        self.AZCHM_CD_LabelBinarizer.fit(np.unique(addresscode_list['字丁目コード'].astype(str).values))
        self.AZCHM_CD_LEN = len(self.AZCHM_CD_LabelBinarizer.classes_)

        self.ADDR_CD_LabelBinarizer.fit(np.unique(addresscode_list['住所コード'].astype(str).values))
        self.ADDR_CD_LEN = len(self.ADDR_CD_LabelBinarizer.classes_)

        self.ZIPCODE_LabelBinarizer.fit(np.unique(addresscode_list['郵便番号'].astype(str).values))
        self.ZIPCODE_LEN = len(self.ZIPCODE_LabelBinarizer.classes_)

    def transform_TDFKN_CD(self, code):
        """
        都道府県コードをOne-Hotエンコーディングする

        Args:
            text (str): 都道府県コード(2桁)

        Returns:
            One-HotエンコーディングされたTuple
        """
        return self.TDFKN_CD_LabelBinarizer.transform(code)

    def inverse_transform_TDFKN_CD(self, onehot):
        """
        One-Hotエンコーディングされた都道府県コードをデコーディングする

        Args:
            onehot (tuple): One-Hotエンコーディングされた都道府県コード

        Returns:
            都道府県コード(2桁)
        """
        return self.TDFKN_CD_LabelBinarizer.inverse_transform(onehot)

    def transform_SCYOSN_CD(self, code):
        """
        市区町村コードをOne-Hotエンコーディングする

        Args:
            text (str): 市区町村コード(3桁)

        Returns:
            One-HotエンコーディングされたTuple
        """
        return self.SCYOSN_CD_LabelBinarizer.transform(code)

    def inverse_transform_SCYOSN_CD(self, onehot):
        """
        One-Hotエンコーディングされた市区町村コードをデコーディングする

        Args:
            onehot (tuple): One-Hotエンコーディングされた市区町村コード

        Returns:
            市区町村コード(3桁)
        """
        return self.SCYOSN_CD_LabelBinarizer.inverse_transform(onehot)

    def transform_OAZA_TSHUM_CD(self, code):
        """
        大字通称コードをOne-Hotエンコーディングする

        Args:
            text (str): 大字通称コード(3桁)

        Returns:
            One-HotエンコーディングされたTuple
        """
        return self.OAZA_TSHUM_CD_LabelBinarizer.transform(code)

    def inverse_transform_OAZA_TSHUM_CD(self, onehot):
        """
        One-Hotエンコーディングされた大字通称コードをデコーディングする

        Args:
            onehot (tuple): One-Hotエンコーディングされた大字通称コード

        Returns:
            大字通称コード(3桁)
        """
        return self.OAZA_TSHUM_CD_LabelBinarizer.inverse_transform(onehot)

    def transform_AZCHM_CD(self, code):
        """
        字丁目コードをOne-Hotエンコーディングする

        Args:
            text (str): 字丁目コード(3桁)

        Returns:
            One-HotエンコーディングされたTuple
        """
        return self.AZCHM_CD_LabelBinarizer.transform(code)

    def inverse_transform_AZCHM_CD(self, onehot):
        """
        One-Hotエンコーディングされた字丁目コードをデコーディングする

        Args:
            onehot (tuple): One-Hotエンコーディングされた字丁目コード

        Returns:
            字丁目コード(3桁)
        """
        return self.AZCHM_CD_LabelBinarizer.inverse_transform(onehot)

    def transform_ADDR_CD(self, code):
        """
        住所コードをOne-Hotエンコーディングする

        Args:
            text (str): 住所コード(11桁)

        Returns:
            One-HotエンコーディングされたTuple
        """
        return self.ADDR_CD_LabelBinarizer.transform(code)

    def inverse_transform_ADDR_CD(self, onehot):
        """
        One-Hotエンコーディングされた住所コードをデコーディングする

        Args:
            onehot (tuple): One-Hotエンコーディングされた住所コード

        Returns:
            住所コード(11桁)
        """
        return self.ADDR_CD_LabelBinarizer.inverse_transform(onehot)

    def transform_ZIPCODE(self, code):
        """
        住所コードをOne-Hotエンコーディングする

        Args:
            text (str): 住所コード(7桁)

        Returns:
            One-HotエンコーディングされたTuple
        """
        return self.ZIPCODE_LabelBinarizer.transform(code)

    def inverse_transform_ZIPCODE(self, onehot):
        """
        One-Hotエンコーディングされた住所コードをデコーディングする

        Args:
            onehot (tuple): One-Hotエンコーディングされた住所コード

        Returns:
            住所コード(7桁)
        """
        return self.ZIPCODE_LabelBinarizer.inverse_transform(onehot)

if __name__ == "__main__":
    lb = AddressCodeBinarizer('./data/学習用住所コードデータ.csv')
    aaa = lb.transform_OAZA_TSHUM_CD(['100'])
    print(aaa[0])
    bbb = lb.transform_AZCHM_CD(['100'])
    print(bbb[0])