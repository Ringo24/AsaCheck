"""
04_市区町村コード予測Model作成.py
住所+都道府県コードを元に市区町村コードを予測するモデルを作成して保存する
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

#AIに必要なものをインポート
from keras import layers
from keras import models
from keras import utils
from sklearn.model_selection import train_test_split

from utils import TextToArrays
from utils import AddressCodeBinarizer
from classifiers import SCYOSN_CD_Classifier

def main():
    """メイン関数
    
    04_市区町村コード予測Model作成メイン関数

    """

    #モデル組み立て
    lb = AddressCodeBinarizer(pathlib.Path(__file__).parent / 'data/学習用住所コードデータ.csv')
    t2a = TextToArrays(maxlen=10)
    t2a.load()

    classifier = SCYOSN_CD_Classifier(lb, t2a)
    classifier.assemble_model()

    #機械学習
    classifier.fit_from_file(
        train_file_path=pathlib.Path(__file__).parent / 'data/学習用住所コードデータ.csv',
        val_file_path=pathlib.Path(__file__).parent / 'data/検証用住所コードデータ.csv',
        batch_size=8000,
        epochs=64)

    #モデルを保存する
    classifier.save_model()

    #訓練の正確度と損失グラフを保存する
    classifier.save_training_accuracy()
    classifier.save_training_loss()

if __name__ == "__main__":
    main()