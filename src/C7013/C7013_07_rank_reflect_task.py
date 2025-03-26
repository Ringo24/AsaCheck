# 標準ライブラリインポート
import os

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
import inject
import jaconv

# プロジェクトライブラリインポート
from . import const
from .task import BaseTask, TaskResult
from .dao.crmdb_dao import CrmDBDao
from .dao.sqlite_dao import SqliteDao
from .C7013_07_extract_study_data_task import C7013_07_extract_study_data_task

class C7013_07_rank_reflect_task(BaseTask):
    '''
    学習データ反映クラス
    '''

    @inject.autoparams()
    def __init__(
        self,
        extractStudyDataTask:C7013_07_extract_study_data_task,
        dao:CrmDBDao,
        sqlite_dao:SqliteDao
        ):
        '''
        初期化関数

        Args:
            extractStudyDataTask: 学習データ抽出タスク
            dao: CrmDBDao
        '''
        self.__extractStudyDataTask = extractStudyDataTask
        self.__dao = dao
        self.__sqlite_dao = sqlite_dao
        #親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self, jidou_sahai_rev, target_period_from, target_period_to)->TaskResult:
        '''
        学習データを反映

        Args:
            jidou_sahai_rev: 自動差配リビジョン
            target_period_from: 対象期間(開始)
            target_period_to: 対象期間(終了)
        Returns:
            TaskResult: タスク結果クラス
        '''
        # 対象データ抽出
        task_result = self.__extractStudyDataTask.execute(jidou_sahai_rev, target_period_from, target_period_to)

        df_target = task_result.resultData.copy()

        if not jidou_sahai_rev:
            # リビジョン指定なしの場合は重複の取次(GUID)の除外
            df_target = df_target[~df_target.duplicated(subset='commissionid_guid')]

        conn = self.__sqlite_dao.conn()
        df_target.apply(self._exec, axis='columns', rev=jidou_sahai_rev)
        conn.commit()

        return TaskResult(resultCode=const.BATCH_SUCCESS)

    def _exec(self, row: pd.Series, rev):
        '''
        取次Eから取得したランクを学習データ(DBファイル)に反映
        
        Args:
            row: 学習データSeries
            rev: 自動差配リビジョン
        '''
        commissionid_guid = row['commissionid_guid']

        # 取次Eのランク取得
        if commissionid_guid:
            df = self.__dao.retrive_new_commission_from_new_commissionid(commissionid_guid)

            if len(df) != 0:
                rank = df['rank'].iloc[0]
            
                # 学習データへのランク反映
                self.__sqlite_dao.init_custom_tables()
                self.__sqlite_dao.update_custom_table_rank(rev, commissionid_guid, rank)
        