# 標準ライブラリインポート
import logging
import dataclasses

# サードパーティライブラリインポート
import numpy as np
import pandas as pd

# プロジェクトライブラリインポート
from . import const
from . import utils

class BaseTask(object):
    '''
    タスク基底クラス
    '''

    def __init__(self):
        '''
        コンストラクタ
        '''
        # ロガー
        self._logger: logging.Logger = utils.getLogger()

    @property
    def logger(self) -> logging.Logger:
        '''
        ロガー
        '''

        return self._logger


@dataclasses.dataclass
class TaskResult(object):
    '''
    タスク結果クラス
    '''

    # 処理結果コード
    resultCode: int
    # 処理結果データ
    resultData: pd.DataFrame
    # エラーデータ
    errorData: pd.DataFrame

    def __init__(self, resultCode: int = 0, resultData: pd.DataFrame = None, errorData: pd.DataFrame = None):
        '''
        初期化関数

        Args:
        resultCode:処理結果コード(0:正常 1:異常 2:警告)
        resultData:処理結果データ
        errorData:エラーデータ
        '''

        # 処理結果コード
        self.resultCode = resultCode
        # 処理結果データ
        self.resultData = resultData
        # エラーデータ
        self.errorData = errorData

    def __str__(self):
        return "resultCode:{0}\r\nresultData:{1}\r\nerrorData:{2}".format(self.resultCode, self.resultData, self.errorData)