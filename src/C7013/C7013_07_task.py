# 標準ライブラリインポート
import os
import datetime
import csv

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
import inject

# プロジェクトライブラリインポート
from .task import BaseTask, TaskResult
from .dao.sqlite_dao import SqliteDao
from . import const, message

# DataFrameカラム名
# 自動差配リビジョン
JIDOU_SAHAI_REV = 'jidou_sahai_rev'
# 更新日時
UPDATE_DATE = 'update_date'
# ランク
RANK = 'rank'
# ランク(システム)
RANK_SYSTEM = 'rank_system'

convert_list = [ 'commissionid_guid'
                ,'comprehensivecompany_guid'
                ,'division_guid'
                ,'section_guid'
                ,'unit_guid'
                ,'autoagentid_guid'
                ,'agent_window_comprehensivecompany_guid'
                ,'agent_window_division_guid'
                ,'agent_window_section_guid'
                ,'agent_window_unit_guid'
                ,'accountperson_incharge_guid'
                ,'to_agent_comprehensivecompany_guid'
                ,'to_agent_division_guid'
                ,'to_agent_section_guid'
                ,'to_agent_unit_guid'
                ,'agent_object_unit_guid_list'
                ]

class C7013_07_task(BaseTask):
    '''
    学習データ蓄積クラス
    '''

    # 機能ID
    APP_ID: str = 'C7013_07'
    # 機能名
    APP_NAME: str = 'データ蓄積'

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

    def execute(self, input_data:pd.DataFrame)->TaskResult:
        '''
        学習データを蓄積

        Args:
            input_data: 自動差配済情報DataFrame
        Returns:
            TaskResult: タスク結果クラス
        '''
        # 処理開始メッセージ出力
        self.logger.info(message.MSG['MSG0001'], C7013_07_task.APP_ID, C7013_07_task.APP_NAME)

        df = input_data.copy()

        if not JIDOU_SAHAI_REV in df.columns:
            # 自動差配リビジョン
            jidou_sahai_rev = const.APP_CONFIG['global_config']['jidou_sahai_rev']
            df[JIDOU_SAHAI_REV] = jidou_sahai_rev

        if not UPDATE_DATE in df.columns:
            # 更新日時
            updatetime = datetime.datetime.now().strftime("%Y%m%d%H%M")
            df[UPDATE_DATE] = updatetime

        if RANK in df.columns:
            try:
                df[RANK] = df[RANK].astype(pd.Int64Dtype())
            except Exception as ex:
                #キャッチして例外をログに記録
                self.logger.error(f'rankに数値以外の不正なデータがあります。')
                raise Exception(ex) from ex

        if RANK_SYSTEM in df.columns:
            try:
                df[RANK_SYSTEM] = df[RANK_SYSTEM].astype(pd.Int64Dtype())
            except Exception as ex:
                #キャッチして例外をログに記録
                self.logger.error(f'rank_systemに数値以外の不正なデータがあります。')
                raise Exception(ex) from ex

        self._upsert_custom_table(df)

        # 処理終了メッセージ出力
        self.logger.info(message.MSG['MSG0002'], C7013_07_task.APP_ID, C7013_07_task.APP_NAME)
        
        return TaskResult(const.BATCH_SUCCESS)
        
    def _upsert_custom_table(self, df):

        # 接続
        conn = self.__sqlite_dao.conn()

        self.__sqlite_dao.init_custom_tables()

        if len(df) != 0:
            self._convert_GUID_to_str(df)
            # upsertはないのでdelete&insert
            self.__sqlite_dao.delete_custom_table_by_commissionid_guid(df)
            df.to_sql("custom_table", conn, if_exists='append', index=False)
            conn.commit()
        
        conn.close()

    def _convert_GUID_to_str(self, df):
        """
        custom_tableへInsertするため、GUIDをstrに変換する
        """
        for guid_name in convert_list:
            for index, data in df[guid_name].iteritems():
                if not pd.isna(data):
                    df.at[index, guid_name] = str(data)
