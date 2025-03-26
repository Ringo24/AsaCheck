"""
09_字丁目コード予測.py
住所+都道府県コード+市区町村コード+大字通称コードを元に字丁目コードを予測して結果を保存する
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
from classifiers import AZCHM_CD_Classifier

def main():
    """メイン関数
    
    09_字丁目コード予測メイン関数

    """

    logger.info('住所データを読み込みます')
    #住所データを読み込む
    data_val = pd.read_csv(pathlib.Path(__file__).parent / 'data/検証用住所コードデータ.csv', sep='\t', encoding='utf-8', dtype=object)

    lb = AddressCodeBinarizer(pathlib.Path(__file__).parent / 'data/学習用住所コードデータ.csv')
    t2a = TextToArrays(maxlen=10)
    t2a.load()

    #分類機を読み込む
    classifier = AZCHM_CD_Classifier(lb, t2a)
    classifier.load_model()

    #学習モデルで予測_学習データ
    pred = classifier.predict(address_list=data_val['住所'].values, tdfkn_cd_list=data_val['都道府県コード'].values, scyosn_cd_list=data_val['市区町村コード'].values, oaza_tshum_cd_list=data_val['大字通称コード'].values, batch_size=2, verbose=1)

    data_val['字丁目コード予測'] = pred
    data_val["字丁目コード予測結果"] = data_val['字丁目コード'] == pred

    #Excelに結果を書き込む
    data_val.to_excel(pathlib.Path(__file__).parent / 'data/08_字丁目コード予測_検証データ.xlsx', index=False)

    #学習モデルの評価_検証データ
    print(classifier.metrics_names)
    print(classifier.evaluate_model(data_val['住所'].values, data_val['都道府県コード'].values, data_val['市区町村コード'].values, data_val['大字通称コード'].values, data_val['字丁目コード'].values, batch_size=2))

if __name__ == "__main__":
    main()