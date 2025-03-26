# 標準ライブラリインポート
import logging
import logging.config
import warnings
import decimal
import sqlite3
import tempfile
import shelve

# サードパーティライブラリインポート
import cx_Oracle
import pymssql
import jaconv as jc
import json5
import matplotlib.pyplot as plt
from tensorflow.keras import callbacks

# プロジェクトライブラリインポート
from . import const


__logger__: logging.Logger = None

def getLogger() -> logging.Logger:
    '''
    ロガーを取得する
    '''
    global __logger__
    if __logger__ == None:
        with open(const.LOG_CONFIG_FILE_PATH, 'r') as json_file:
            conf_file = json5.load(json_file)
            logging.config.dictConfig(conf_file)
            warnings.simplefilter(action='ignore', category=FutureWarning)
            warnings.simplefilter(action='ignore', category=DeprecationWarning)
            __logger__ = logging.getLogger('bizmerge')
    return __logger__

def to_upper_wide_charactor(inStr: str) -> str:
    '''
    文字列を全角大文字に変換する
    引数のタイプが文字列以外の場合、空文字を返却する

    Args:
        inStr: 変換対象文字列

    Returns:
        変換された文字列
    '''

    if inStr == None:
        return ''

    return jc.h2z(inStr.upper(), digit=True, ascii=True)

def get_sqlite_connection() -> sqlite3.Connection:
    '''
    ローカルDBに接続する
    '''

    return sqlite3.connect(const.SQLITE_DB_PATH)

def get_crmdb_connection() -> pymssql.Connection:
    '''
    CRMDBに接続する
    '''
    if const.CRMDB_CONN_USE_ID_PASSWORD_AUTHENTICATION:
        return pymssql.connect(server=const.CRMDB_CONN_SERVER, database=const.CRMDB_CONN_DATABASE, user=const.CRMDB_CONN_ID, password=const.CRMDB_CONN_PW)
    else:
        return pymssql.connect(server=const.CRMDB_CONN_SERVER, database=const.CRMDB_CONN_DATABASE)

def get_nwmdb_connection() -> cx_Oracle.Connection:
    '''
    NWMDBに接続する
    '''

    #DBに接続
    return cx_Oracle.connect(const.NWMDB_CONN_ID, const.NWMDB_CONN_PW, const.NWMDB_CONN_SID)

def save_training_accuracy_and_loss(history: callbacks.History, output_filename: str, figure: bool = True) -> None:
    history_dict = history.history
    acc = history_dict['accuracy']
    loss = history_dict['loss']

    epochs = range(1, len(loss) + 1)

    #訓練と検証正確度をイメージで保存
    plt.plot(epochs, acc, 'bo', label='Training acc')      #bo:青い点線
    plt.plot(epochs, loss, 'b', label='Training loss')     #b:青い実線
    plt.title('Training accuracy and loss')
    plt.xlabel('Epochs')
    plt.ylabel('Accuracy and Loss')
    plt.legend()
    plt.savefig(const.APP_DATA_PATH / output_filename)
    if figure:
        plt.figure()

def save_training_and_validation_loss(history: callbacks.History, output_filename: str, figure: bool = True) -> None:
    history_dict = history.history
    loss = history_dict['loss']
    val_loss = history_dict['val_loss']

    epochs = range(1, len(loss) + 1)

    #訓練と検証正確度をイメージで保存
    plt.plot(epochs, loss, 'bo', label='Training loss')      #bo:青い点線
    plt.plot(epochs, val_loss, 'b', label='Validation loss') #b:青い実線
    plt.title('Training and validation loss')
    plt.xlabel('Epochs')
    plt.ylabel('Loss')
    plt.legend()
    plt.savefig(const.APP_DATA_PATH / output_filename)
    if figure:
        plt.figure()

def xstr(s: str):
    '''
    str関数拡張
    str対象がNoneの場合には空文字を返却する
    '''
    if not s:
        return ''
    return str(s)
