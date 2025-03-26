"""
01_住所辞書データ作成.py
住所データを読込、ベクトル化するためのモデルを作成して保存する
"""

#ログ関連設定
import os
import pathlib
import logging.config
logging.config.fileConfig(pathlib.Path(__file__).parent / 'config/logging.ini')
logger = logging.getLogger("bizmerge")

# FutureWarning警告を出力しない
import warnings
warnings.simplefilter(action="ignore", category=FutureWarning)
warnings.simplefilter(action="ignore", category=DeprecationWarning)

#01_辞書データ作成に必要なものインポート
from gensim.models import word2vec
import janome.tokenizer
import pandas as pd

from utils import TextToArrays

def main():
    """メイン関数
    
    01_住所辞書データ作成メイン関数

    """

    t2a = TextToArrays(maxlen=10)
    t2a.fit_from_file(pathlib.Path(__file__).parent / 'data/学習用住所コードデータ.csv')
    t2a.save()

if __name__ == "__main__":
    main()
