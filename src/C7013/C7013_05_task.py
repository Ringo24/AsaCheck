# 標準ライブラリインポート
import os

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
import inject

# プロジェクトライブラリインポート
from .task import BaseTask, TaskResult
from . import const, message
from .C7013_05_accountperson_specify_task import C7013_05_accountperson_specify_task
from .C7013_05_policy_keyword_assignment_task import C7013_05_policy_keyword_assignment_task

class C7013_05_task(BaseTask):
    '''
        アカウント特定・施策キーワード設定
    '''

    # 機能ID
    APP_ID: str = 'C7013_05'
    # 機能名
    APP_NAME: str = 'アカウント特定／施策キーワード設定'

    @inject.autoparams()
    def __init__(
        self,
        accountperson_specify_task:C7013_05_accountperson_specify_task,
        policy_keyword_assignment_task:C7013_05_policy_keyword_assignment_task
        ):
        '''
        初期化関数

        Args:
        accountperson_specify_task:アカウント特定タスク
        policy_keyword_assignment_task:施策キーワード設定タスク
        '''
        self.__accountperson_specify_task = accountperson_specify_task
        self.__policy_keyword_assignment_task = policy_keyword_assignment_task
        #親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self, input_data:pd.DataFrame)->pd.DataFrame:
        '''
        ランク付与済情報をもとにアカウント担当者特定、施策キーワード設定を行うクラス

        Args:
            input_data: 入力データ
        Returns:
            TaskResult: アカウント/施策キーワード付与済みのタスク結果クラス
        '''
        # 処理開始メッセージ出力
        self.logger.info(message.MSG['MSG0001'], C7013_05_task.APP_ID, C7013_05_task.APP_NAME)

        df = input_data.copy()
        # アカウント特定
        task_result_accountperson = self.__accountperson_specify_task.execute(df)
        # 施策キーワード設定
        task_result = self.__policy_keyword_assignment_task.execute(task_result_accountperson.resultData)

        # 処理終了メッセージ出力
        self.logger.info(message.MSG['MSG0002'], C7013_05_task.APP_ID, C7013_05_task.APP_NAME)
        
        return task_result

