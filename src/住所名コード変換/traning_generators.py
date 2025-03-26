"""
generator.py
住所コード学習データGenerator
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
import csv

#AIに必要なものをインポート
from keras import layers
from keras import models
from keras import utils
from sklearn.model_selection import train_test_split

from utils import TextToArrays
from utils import AddressCodeBinarizer
from keras.utils import Sequence

class TDFKN_CD_generator(Sequence):
    """都道府県コード generator"""

    file_path:str = None
    length:int = None
    batch_size:int = None
    num_batches_per_epoch:int = None
    t2a = None
    lb = None
    csv_file = None
    csv_reader = None

    def __init__(self, lb, t2a, file_path:str, batch_size:int=1):
        """construction
        :param file_path: addresscode filepath
        :param batch_size: Batch size
        """

        self.lb = lb
        self.t2a = t2a
        self.file_path = file_path
        self.batch_size = batch_size
        #ファイルを読込行数を数える
        with open(file_path, 'r', 65536, encoding='utf-8') as f:
            for i, _ in enumerate(f):
                pass
        #ヘッダを考慮すること
        self.length = i

        self.num_batches_per_epoch = int((self.length - 1) / batch_size) + 1

    def __getitem__(self, idx):
        """Get batch data
        :param idx: Index of batch
        :return train_住所V_list: numpy array of address
        :return train_都道府県コードV_list: numpy array of 都道府県コード
        """

        if idx == 0:
            #logger.info(self.file_path + 'ファイルを読み込みます')
            self.csv_file = open(self.file_path, 'r', 65536, encoding='utf-8')
            self.csv_reader = csv.DictReader(self.csv_file, delimiter='\t', quotechar='"')

        start_pos = self.batch_size * idx
        end_pos = start_pos + self.batch_size
        if end_pos > self.length:
            end_pos = self.length

        logger.info('idx:%d,start_pos:%d,end_pos:%d' % (idx, start_pos, end_pos))
        address_list = []
        tdfkn_cd_list = []
        count = 0
        for row in self.csv_reader:
            address_list.append(self.t2a.word2vec(row['住所']))
            tdfkn_cd_list.append(row['都道府県コード'])
            count = count + 1
            if count >= self.batch_size:
                break

        return {'addressname_input': np.array(address_list)}, {'tdfkn_cd_output': self.lb.transform_TDFKN_CD(tdfkn_cd_list)}

    def __len__(self)->int:
        """Batch length"""
        return self.num_batches_per_epoch

    def on_epoch_end(self)->None:
        """Task when end of epoch"""
        if self.csv_file != None:
            logger.info(self.file_path + 'ファイルを閉じます')
            self.csv_file.close()
            self.csv_file = None
            self.csv_reader = None

class SCYOSN_CD_generator(Sequence):
    """市区町村コード generator"""

    file_path:str = None
    length:int = None
    batch_size:int = None
    num_batches_per_epoch:int = None
    t2a = None
    lb = None
    csv_file = None
    csv_reader = None

    def __init__(self, lb, t2a, file_path:str, batch_size:int=1):
        """construction
        :param file_path: addresscode filepath
        :param batch_size: Batch size
        """

        self.lb = lb
        self.t2a = t2a
        self.file_path = file_path
        self.batch_size = batch_size
        #ファイルを読込行数を数える
        with open(file_path, 'r', 65536, encoding='utf-8') as f:
            for i, _ in enumerate(f):
                pass
        #ヘッダを考慮すること
        self.length = i

        self.num_batches_per_epoch = int((self.length - 1) / batch_size) + 1

    def __getitem__(self, idx):
        """Get batch data
        :param idx: Index of batch
        :return train_住所V_list: numpy array of address
        :return train_都道府県コードV_list: numpy array of 都道府県コード
        """

        if idx == 0:
            logger.info(self.file_path + 'ファイルを読み込みます')
            self.csv_file = open(self.file_path, 'r', 65536, encoding='utf-8')
            self.csv_reader = csv.DictReader(self.csv_file, delimiter='\t', quotechar='"')

        start_pos = self.batch_size * idx
        end_pos = start_pos + self.batch_size
        if end_pos > self.length:
            end_pos = self.length

        logger.info('idx:%d,start_pos:%d,end_pos:%d' % (idx, start_pos, end_pos))
        address_list = []
        tdfkn_cd_list = []
        scyosn_cd_list = []
        count = 0
        for row in self.csv_reader:
            address_list.append(self.t2a.word2vec(row['住所']))
            tdfkn_cd_list.append(row['都道府県コード'])
            scyosn_cd_list.append(row['市区町村コード'])
            count = count + 1
            if count >= self.batch_size:
                break

        return {
            'addressname_input': np.array(address_list),
            'tdfkn_cd_input': self.lb.transform_TDFKN_CD(tdfkn_cd_list)
            }, {
            'scyosn_cd_output': self.lb.transform_SCYOSN_CD(scyosn_cd_list)
            }

    def __len__(self)->int:
        """Batch length"""
        return self.num_batches_per_epoch

    def on_epoch_end(self)->None:
        """Task when end of epoch"""
        if self.csv_file != None:
            logger.info(self.file_path + 'ファイルを閉じます')
            self.csv_file.close()
            self.csv_file = None
            self.csv_reader = None

class OAZA_TSHUM_CD_generator(Sequence):
    """大字通称コード generator"""

    file_path:str = None
    length:int = None
    batch_size:int = None
    num_batches_per_epoch:int = None
    t2a = None
    lb = None
    csv_file = None
    csv_reader = None

    def __init__(self, lb, t2a, file_path:str, batch_size:int=1):
        """construction
        :param file_path: addresscode filepath
        :param batch_size: Batch size
        """

        self.lb = lb
        self.t2a = t2a
        self.file_path = file_path
        self.batch_size = batch_size
        #ファイルを読込行数を数える
        with open(file_path, 'r', 65536, encoding='utf-8') as f:
            for i, _ in enumerate(f):
                pass
        #ヘッダを考慮すること
        self.length = i

        self.num_batches_per_epoch = int((self.length - 1) / batch_size) + 1

    def __getitem__(self, idx):
        """Get batch data
        :param idx: Index of batch
        :return train_住所V_list: numpy array of address
        :return train_都道府県コードV_list: numpy array of 都道府県コード
        """

        if idx == 0:
            logger.info(self.file_path + 'ファイルを読み込みます')
            self.csv_file = open(self.file_path, 'r', 65536, encoding='utf-8')
            self.csv_reader = csv.DictReader(self.csv_file, delimiter='\t', quotechar='"')

        start_pos = self.batch_size * idx
        end_pos = start_pos + self.batch_size
        if end_pos > self.length:
            end_pos = self.length

        logger.info('idx:%d,start_pos:%d,end_pos:%d' % (idx, start_pos, end_pos))
        address_list = []
        tdfkn_cd_list = []
        scyosn_cd_list = []
        oaza_tshum_cd_list = []
        count = 0
        for row in self.csv_reader:
            address_list.append(self.t2a.word2vec(row['住所']))
            tdfkn_cd_list.append(row['都道府県コード'])
            scyosn_cd_list.append(row['市区町村コード'])
            oaza_tshum_cd_list.append(row['大字通称コード'])
            count = count + 1
            if count >= self.batch_size:
                break

        return {
            'addressname_input': np.array(address_list),
            'tdfkn_cd_input': self.lb.transform_TDFKN_CD(tdfkn_cd_list),
            'scyosn_cd_input': self.lb.transform_SCYOSN_CD(scyosn_cd_list)
            }, {
            'oaza_tshum_cd_output': self.lb.transform_OAZA_TSHUM_CD(oaza_tshum_cd_list)
            }

    def __len__(self)->int:
        """Batch length"""
        return self.num_batches_per_epoch

    def on_epoch_end(self)->None:
        """Task when end of epoch"""
        if self.csv_file != None:
            logger.info(self.file_path + 'ファイルを閉じます')
            self.csv_file.close()
            self.csv_file = None
            self.csv_reader = None

class AZCHM_CD_generator(Sequence):
    """字丁目コード generator"""

    file_path:str = None
    length:int = None
    batch_size:int = None
    num_batches_per_epoch:int = None
    t2a = None
    lb = None
    csv_file = None
    csv_reader = None

    def __init__(self, lb, t2a, file_path:str, batch_size:int=1):
        """construction
        :param file_path: addresscode filepath
        :param batch_size: Batch size
        """

        self.lb = lb
        self.t2a = t2a
        self.file_path = file_path
        self.batch_size = batch_size
        #ファイルを読込行数を数える
        with open(file_path, 'r', 65536, encoding='utf-8') as f:
            for i, _ in enumerate(f):
                pass
        #ヘッダを考慮すること
        self.length = i

        self.num_batches_per_epoch = int((self.length - 1) / batch_size) + 1

    def __getitem__(self, idx):
        """Get batch data
        :param idx: Index of batch
        :return train_住所V_list: numpy array of address
        :return train_都道府県コードV_list: numpy array of 都道府県コード
        """

        if idx == 0:
            logger.info(self.file_path + 'ファイルを読み込みます')
            self.csv_file = open(self.file_path, 'r', 65536, encoding='utf-8')
            self.csv_reader = csv.DictReader(self.csv_file, delimiter='\t', quotechar='"')

        start_pos = self.batch_size * idx
        end_pos = start_pos + self.batch_size
        if end_pos > self.length:
            end_pos = self.length

        logger.info('idx:%d,start_pos:%d,end_pos:%d' % (idx, start_pos, end_pos))
        address_list = []
        tdfkn_cd_list = []
        scyosn_cd_list = []
        oaza_tshum_cd_list = []
        azchm_cd_list = []
        count = 0
        for row in self.csv_reader:
            address_list.append(self.t2a.word2vec(row['住所']))
            tdfkn_cd_list.append(row['都道府県コード'])
            scyosn_cd_list.append(row['市区町村コード'])
            oaza_tshum_cd_list.append(row['大字通称コード'])
            azchm_cd_list.append(row['字丁目コード'])
            count = count + 1
            if count >= self.batch_size:
                break

        return {
            'addressname_input': np.array(address_list),
            'tdfkn_cd_input': self.lb.transform_TDFKN_CD(tdfkn_cd_list),
            'scyosn_cd_input': self.lb.transform_SCYOSN_CD(scyosn_cd_list),
            'oaza_tshum_cd_input': self.lb.transform_OAZA_TSHUM_CD(oaza_tshum_cd_list)
            },{
            'azchm_cd_output': self.lb.transform_AZCHM_CD(azchm_cd_list)
            }

    def __len__(self)->int:
        """Batch length"""
        return self.num_batches_per_epoch

    def on_epoch_end(self)->None:
        """Task when end of epoch"""
        if self.csv_file != None:
            logger.info(self.file_path + 'ファイルを閉じます')
            self.csv_file.close()
            self.csv_file = None
            self.csv_reader = None

class ZIPCODE_generator(Sequence):
    """郵便番号コード generator"""

    file_path:str = None
    length:int = None
    batch_size:int = None
    num_batches_per_epoch:int = None
    t2a = None
    lb = None
    csv_file = None
    csv_reader = None

    def __init__(self, lb, t2a, file_path:str, batch_size:int=1):
        """construction
        :param file_path: addresscode filepath
        :param batch_size: Batch size
        """

        self.lb = lb
        self.t2a = t2a
        self.file_path = file_path
        self.batch_size = batch_size
        #ファイルを読込行数を数える
        with open(file_path, 'r', 65536, encoding='utf-8') as f:
            for i, _ in enumerate(f):
                pass
        #ヘッダを考慮すること
        self.length = i

        self.num_batches_per_epoch = int((self.length - 1) / batch_size) + 1

    def __getitem__(self, idx):
        """Get batch data
        :param idx: Index of batch
        :return train_住所V_list: numpy array of address
        :return train_郵便番号V_list: numpy array of 郵便番号
        """

        if idx == 0:
            logger.info(self.file_path + 'ファイルを読み込みます')
            self.csv_file = open(self.file_path, 'r', 65536, encoding='utf-8')
            self.csv_reader = csv.DictReader(self.csv_file, delimiter='\t', quotechar='"')

        start_pos = self.batch_size * idx
        end_pos = start_pos + self.batch_size
        if end_pos > self.length:
            end_pos = self.length

        logger.info('idx:%d,start_pos:%d,end_pos:%d' % (idx, start_pos, end_pos))
        address_list = []
        zipcode_list = []
        count = 0
        for row in self.csv_reader:
            address_list.append(self.t2a.word2vec(row['住所']))
            zipcode_list.append(row['郵便番号'])
            count = count + 1
            if count >= self.batch_size:
                break

        return {'addressname_input': np.array(address_list)}, {'zipcode_output': self.lb.transform_ZIPCODE(zipcode_list)}

    def __len__(self)->int:
        """Batch length"""
        return self.num_batches_per_epoch

    def on_epoch_end(self)->None:
        """Task when end of epoch"""
        if self.csv_file != None:
            logger.info(self.file_path + 'ファイルを閉じます')
            self.csv_file.close()
            self.csv_file = None
            self.csv_reader = None
