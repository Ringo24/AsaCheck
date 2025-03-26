"""
12_郵便番号予測.py
住所を元に郵便番号を予測して結果を保存する
"""

#ログ関連設定
import os
import pathlib
import logging.config
logging.config.fileConfig(pathlib.Path(__file__).parent / 'config/logging.ini')
logger = logging.getLogger("bizmerge")

#FutureWarning警告を出力しない
import warnings
warnings.simplefilter(action="ignore", category=FutureWarning)
warnings.simplefilter(action="ignore", category=DeprecationWarning)

#一般的に必要なものインポート
import codecs
import re
import numpy as np
import pandas as pd
import jaconv as jc
import matplotlib.pyplot as plt
import keras

#AIに必要なものをインポート
from keras import layers
from keras import models
from keras.utils import np_utils
from sklearn.model_selection import train_test_split

from utils import TextToArrays
from utils import AddressCodeBinarizer
from classifiers import ZIPCODE_Classifier

def main():
    """メイン関数
    
    12_郵便番号予測メイン関数

    """

    logger.info('住所データを読み込みます')
    #住所データを読み込む
    data_val = pd.read_csv(pathlib.Path(__file__).parent / 'data/検証用住所コードデータ.csv', sep='\t', encoding='utf-8', dtype=object)

    lb = AddressCodeBinarizer(pathlib.Path(__file__).parent / 'data/学習用住所コードデータ.csv')
    t2a = TextToArrays(maxlen=10)
    t2a.load()

    #分類機を読み込む
    classifier = ZIPCODE_Classifier(lb, t2a)
    classifier.load_model()

    #学習モデルで予測_検証データ
    pred = classifier.predict(address_list=data_val['住所'].values, batch_size=2, verbose=1)

    data_val['郵便番号予測'] = pred

    #Excelに結果を書き込む
    data_val.to_excel(pathlib.Path(__file__).parent / 'data/11_郵便番号_検証データ.xlsx', index=False)

    #学習モデルの評価_検証データ
    print(classifier.metrics_names)
    print(classifier.evaluate_model(x=data_val['住所'].values, y=data_val['郵便番号'].values, batch_size=2))

if __name__ == "__main__":
    main()