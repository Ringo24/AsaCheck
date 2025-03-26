# 標準ライブラリインポート
import os

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
import inject

# プロジェクトライブラリインポート
from ..lib import const, message, utils
from .task import BaseTask, TaskResult
from .C7013_01_task import C7013_01_task
from .C7013_02_task import C7013_02_task
from .C7013_03_task import C7013_03_task
from .C7013_04_task import C7013_04_task
from .C7013_05_task import C7013_05_task
from .C7013_06_task import C7013_06_task
from .C7013_07_task import C7013_07_task

class C7013_task(BaseTask):
    '''
    '''

    @inject.autoparams()
    def __init__(
        self,
        task_01: C7013_01_task,
        task_02: C7013_02_task,
        task_03: C7013_03_task,
        task_04: C7013_04_task,
        task_05: C7013_05_task,
        task_06: C7013_06_task,
        task_07: C7013_07_task,
        ):
        '''
        '''

        self.__task_01: C7013_01_task = task_01
        self.__task_02: C7013_02_task = task_02
        self.__task_03: C7013_03_task = task_03
        self.__task_04: C7013_04_task = task_04
        self.__task_05: C7013_05_task = task_05
        self.__task_06: C7013_06_task = task_06
        self.__task_07: C7013_07_task = task_07

        #親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self) -> TaskResult:
        '''
        C7013_取次自動差配 メインタスク
        '''
        
        # 取次自動差配処理結果
        result = const.BATCH_SUCCESS
        
        # 戻り値初期化
        taskResult = TaskResult()
        taskResultForReturn = TaskResult()
        
        # 持ち回り用DataFrame初期化
        df_tmp = None
        
        self.logger.debug(f'タスクを実行します。')
        
        # ================================
        # データ抽出/事前チェック呼出し
        # ================================
        
        # 「データ抽出/事前チェック」処理呼出し
        taskResult = self.__task_01.execute(None)
        df_tmp = taskResult.resultData
        
        # 「データ抽出/事前チェック」処理結果確認
        result = taskResult.resultCode
        
        # 取次情報DataFrame件数確認
        if df_tmp is None or len(df_tmp) == 0:
            # 0件の場合にはここで処理終了
            return taskResult
        
        # ================================
        # データクレンジング呼出し
        # ================================
        
        # 「データクレンジング」処理呼出し
        taskResult = self.__task_02.execute(df_tmp)
        df_tmp = taskResult.resultData
        
        # 「データクレンジング」処理結果確認
        if taskResult.resultCode == const.BATCH_ERROR:
            # 異常終了の場合には結果に異常を設定して処理終了
            taskResultForReturn.resultCode = taskResult.resultCode
            return taskResultForReturn
        elif taskResult.resultCode == const.BATCH_WARNING and result != const.BATCH_ERROR:
            # 警告終了の場合には 内部処理結果が異常以外の場合のみ上書きで処理続行
            result = taskResult.resultCode
            
        # ================================
        # ランク判定用フラグ追加呼出し
        # ================================
        
        # 「ランク判定用フラグ追加」処理呼出し
        taskResult = self.__task_03.execute(df_tmp)
        df_tmp = taskResult.resultData
        
        # 「ランク判定用フラグ追加」処理結果確認
        if taskResult.resultCode == const.BATCH_ERROR:
            # 異常終了の場合には結果に異常を設定して処理終了
            taskResultForReturn.resultCode = taskResult.resultCode
            return taskResultForReturn
        elif taskResult.resultCode == const.BATCH_WARNING and result != const.BATCH_ERROR:
            # 警告終了の場合には 内部処理結果が異常以外の場合のみ上書きで処理続行
            result = taskResult.resultCode

        # ================================
        # ランク判定呼出し
        # ================================
        # 「ランク判定」処理呼出し
        taskResult = self.__task_04.execute(df_tmp)
        df_tmp = taskResult.resultData
        
        # 「ランク判定」処理結果確認
        if taskResult.resultCode == const.BATCH_ERROR:
            # 異常終了の場合には結果に異常を設定して処理終了
            taskResultForReturn.resultCode = taskResult.resultCode
            return taskResultForReturn
        elif taskResult.resultCode == const.BATCH_WARNING and result != const.BATCH_ERROR:
            # 警告終了の場合には 内部処理結果が異常以外の場合のみ上書きで処理続行
            result = taskResult.resultCode
        
        # ================================
        # アカウント特定/施策キーワード設定呼出し
        # ================================
        # 「アカウント特定/施策キーワード設定」処理呼出し
        taskResult = self.__task_05.execute(df_tmp)
        df_tmp = taskResult.resultData
        
        # 「アカウント特定/施策キーワード設定」処理結果確認
        if taskResult.resultCode == const.BATCH_ERROR:
            # 異常終了の場合には結果に異常を設定して処理終了
            taskResultForReturn.resultCode = taskResult.resultCode
            return taskResultForReturn
        elif taskResult.resultCode == const.BATCH_WARNING and result != const.BATCH_ERROR:
            # 警告終了の場合には 内部処理結果が異常以外の場合のみ上書きで処理続行
            result = taskResult.resultCode
 
        # ================================
        # 自動差配呼出し
        # ================================
        # 「自動差配」処理呼出し
        taskResult = self.__task_06.execute(df_tmp)
        df_tmp = taskResult.resultData
        
        # 「自動差配」処理結果確認
        if taskResult.resultCode == const.BATCH_ERROR:
            # 異常終了の場合には結果に異常を設定して処理続行
            result = taskResult.resultCode
        elif taskResult.resultCode == const.BATCH_WARNING and result != const.BATCH_ERROR:
            # 警告終了の場合には 内部処理結果が異常以外の場合のみ上書きで処理続行
            result = taskResult.resultCode

        # ================================
        # 学習データ蓄積呼出し
        # ================================
        # 「学習データ蓄積」処理呼出し
        taskResult = self.__task_07.execute(df_tmp)
        
        # 「学習データ蓄積」処理結果確認
        if taskResult.resultCode == const.BATCH_ERROR:
            # 異常終了の場合には結果に異常を設定する
            result = taskResult.resultCode
        elif taskResult.resultCode == const.BATCH_WARNING and result != const.BATCH_ERROR:
            # 警告終了の場合には 内部処理結果が異常以外の場合のみ上書きで処理続行
            result = taskResult.resultCode

        # 内部処理結果の値を設定して処理終了
        taskResultForReturn.resultCode = result
        
        return taskResultForReturn

