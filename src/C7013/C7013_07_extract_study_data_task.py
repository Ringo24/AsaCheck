# 標準ライブラリインポート
import os
import datetime

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
import inject
import jaconv

# プロジェクトライブラリインポート
from .task import BaseTask, TaskResult
from .dao.sqlite_dao import SqliteDao
from . import const

# DataFrameカラム名
# ランク
RANK = 'rank'
# ランク(システム)
RANK_SYSTEM = 'rank_system'

class C7013_07_extract_study_data_task(BaseTask):
    '''
    学習データ抽出クラス
    '''

    @inject.autoparams()
    def __init__(self, sqlite_dao:SqliteDao):
        '''
        初期化関数

        Args:
            sqlite_dao: SqliteDao
        '''
        self.__sqlite_dao = sqlite_dao
        #親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self, jidou_sahai_rev, target_period_from, target_period_to)->TaskResult:
        '''
        学習データを抽出

        Args:
            jidou_sahai_rev: 自動差配リビジョン
            target_period_from: 対象期間(開始)
            target_period_to: 対象期間(終了)
        Returns:
            TaskResult: タスク結果クラス
        '''

        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
        # 対象期間(開始/終了)：規定値前日
        if not target_period_from:
            target_period_from = yesterday.strftime("%Y%m%d")
        if not target_period_to:
            target_period_to = yesterday.strftime("%Y%m%d")

        target_period_from = target_period_from[:8].ljust(12, '0')
        target_period_to = target_period_to[:8].ljust(12, '9')

        self.__sqlite_dao.init_custom_tables()

        df = self.__sqlite_dao.select_custom_table(jidou_sahai_rev,
                                                   target_period_from,
                                                   target_period_to)

        df[RANK] = df[RANK].astype(pd.Int64Dtype())
        df[RANK_SYSTEM] = df[RANK_SYSTEM].astype(pd.Int64Dtype())

        return TaskResult(resultCode=const.BATCH_SUCCESS, resultData=df)
