# 標準ライブラリインポート
import os
import datetime

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
import inject
import jaconv

# プロジェクトライブラリインポート
from . import const
from .task import BaseTask, TaskResult
from .dao.sqlite_dao import SqliteDao

class C7013_07_correct_answer_rate_task(BaseTask):
    '''
    正答率出力クラス
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

    def execute(self, jidou_sahai_rev, target_period_from, target_period_to)->pd.DataFrame:
        '''
        ランク(システム)とランクの情報を元に正答率データを抽出出力する。

        Args:
            jidou_sahai_rev: 自動差配リビジョン
            target_period_from: 対象期間(開始)
            target_period_to: 対象期間(終了)
        Returns:
            TaskResult: タスク結果クラス
        '''
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
        if not target_period_from:
            target_period_from = yesterday.strftime("%Y%m%d")
        if not target_period_to:
            target_period_to = yesterday.strftime("%Y%m%d")

        target_period_from = target_period_from[:8].ljust(12, '0')
        target_period_to = target_period_to[:8].ljust(12, '9')

        self.__sqlite_dao.init_custom_tables()

        df = self.__sqlite_dao.correct_answer_rate(jidou_sahai_rev,
                                                   target_period_from,
                                                   target_period_to)

        return TaskResult(resultCode=const.BATCH_SUCCESS, resultData=df)
