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

# 施策キーワード
POLICY_KEYWORDS = 'policy_keywords'

# 施策キーワードの切り捨て文字数
MAX_KEYWORD_LENGTH = 100

# 施策キーワード設定E.キーワード
NEW_AUTOAGENT_POLICY_KEYWORD = 'new_autoagent_policy_keyword'

class C7013_05_policy_keyword_assignment_task(BaseTask):
    '''
    施策キーワード設定タスククラス
    '''

    @inject.autoparams()
    def __init__(self, dao:CrmDBDao):
        '''
        初期化関数

        Args:
            dao: CrmDBDao
        '''
        self.__dao = dao
        #親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self, input_data:pd.DataFrame)->pd.DataFrame:
        '''
        施策キーワード設定

        Args:
            input_data: ランク付与済情報DataFrame
        Returns:
            TaskResult: タスク結果クラス
        '''
        df = input_data.copy()

        # 初期登録
        # 施策キーワード
        df[POLICY_KEYWORDS] = ""
        
        self._policy_keyword_set(df)

        return TaskResult(resultCode=const.BATCH_SUCCESS, resultData=df)

    def _policy_keyword_set(self, df):
        '''
        取次内容（クレンジング済み）に施策キーワード一覧に存在するキーワードを連結し、施策キーワードに設定する

        Args:
            df: ランク付与済情報DataFrame
        '''
        dic_keyword ={}

        for index, row in df.iterrows():
            if not pd.isna(row.autoagentid_guid):
                if not row.autoagentid_guid in dic_keyword:
                    dic_keyword[row.autoagentid_guid] = self._get_df_keyword(row.autoagentid_guid)

                policy_keywords = self._set_policy_keywords(row, dic_keyword[row.autoagentid_guid])
                # 施策キーワードを切り捨てて設定（100文字まで）
                df.at[index, POLICY_KEYWORDS] = policy_keywords[:MAX_KEYWORD_LENGTH]

    def _set_policy_keywords(self, row, df_keyword):
        '''
        施策キーワード設定

        取次内容（クレンジング済み）に施策キーワード一覧のキーワードが存在する場合、
        登録値を"："(コロン)で連結する。
        異なる登録値が複数含まれる場合には、登録値を"："(コロン)で連結する。
        同一登録値が複数含まれる場合には１つ目の登録値にする。
        正規表現でのメタ文字が記載されていてもそのまま検索する。
        判定を行う場合は、全角化および大文字化した状態で比較する。

        Args:
            row: ランク付与済情報DataFrame（1レコード）
            df_keyword: 施策キーワード一覧
        Returns:
            str: 登録値連結文字列
        '''
        match_list = []

        if pd.isna(row.contents_commission_cleansing):
            # 取次内容（クレンジング済み）がnanの場合は空文字を返却
            return ""

        for row_keyword in df_keyword.itertuples():
            # 取次内容（クレンジング済み）に施策キーワード一覧のキーワードが存在するか
            if row_keyword.new_autoagent_policy_keyword in row.contents_commission_cleansing:
                if not row_keyword.new_policy_word in match_list:
                    # 同一登録値が複数含まれる場合には１つ目の登録値にする
                    if row_keyword.new_policy_word:
                        match_list.append(row_keyword.new_policy_word)  

        return '：'.join(match_list)

    def _get_df_keyword(self, autoagentid_guid):
        '''
        施策キーワード設定Eよりキーワードと登録値を取得する。

        Args:
            autoagentid_guid: 自動差配設定(GUID)
        '''
        df_new_autoagent_policy_keyword = self.__dao.retrive_new_autoagent_policy_keyword(autoagentid_guid)

        # 大文字化
        df_new_autoagent_policy_keyword[NEW_AUTOAGENT_POLICY_KEYWORD] \
            = df_new_autoagent_policy_keyword[NEW_AUTOAGENT_POLICY_KEYWORD].str.upper()

        # 全角化
        s_keyword = df_new_autoagent_policy_keyword[NEW_AUTOAGENT_POLICY_KEYWORD]
        for num in range(0, len(s_keyword)):
            s_keyword[num] = jaconv.h2z(s_keyword[num], digit=True, ascii=True)
        df_new_autoagent_policy_keyword[NEW_AUTOAGENT_POLICY_KEYWORD] = s_keyword
        
        return df_new_autoagent_policy_keyword
