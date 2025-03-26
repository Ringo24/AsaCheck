# 標準ライブラリインポート
import os
import re
import csv

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
import inject
import pathlib
import jaconv

# プロジェクトライブラリインポート
from .task import BaseTask, TaskResult
from ..lib import const, message, utils

class C7013_02_task(BaseTask):
    '''
    データクレンジング処理を行うタスククラス
    '''
    # 機能ID
    APP_ID: str = 'C7013_02'
    # 機能名
    APP_NAME: str = 'データクレンジング'

    @inject.autoparams()
    def __init__(self):
        '''
        データクレンジングの初期化
        '''
        #親クラスの初期化関数を呼び出す
        super().__init__()


    def execute(self, input_data: pd.DataFrame)->TaskResult:
        '''
        データ抽出/事前チェックで出力したデータに対して、
        ランク判定やアカウント特定の精度を高めるためにクレンジングを行う。
        '''
        self.logger.debug(f'タスクを実行します。')

        #開始ログ出力
        self.logger.info(message.MSG['MSG0001'], C7013_02_task.APP_ID, C7013_02_task.APP_NAME)

        df = input_data.copy()
        
        df = df.apply(self._excec, axis='columns')

        result = TaskResult(const.BATCH_SUCCESS, df, None)

        # 正常終了ログ出力
        self.logger.info(message.MSG['MSG0002'], C7013_02_task.APP_ID, C7013_02_task.APP_NAME)

        return result
    
    def _excec(self, row: pd.Series) -> pd.Series:
        '''
        インプットのDataFrameの行毎のクレンジング処理を行う。
        '''
        # 契約者名(クレンジング後)初期化
        contractorname_cleansing = row['contractorname']

        if not pd.isna(contractorname_cleansing) and contractorname_cleansing != '' and const.CLENSING_CONFIG_NAMECONTRACTOR_NAME.get('cleansing_settings'):

            # 置換処理実施
            for key in const.CLENSING_CONFIG_NAMECONTRACTOR_NAME['cleansing_settings']:
                
                #キー(置換元文字列)が設定されていた場合
                if key['patternstring'] != '':
                    beforeStr = key['patternstring']
                    afterStr = key['replacementstring']
                    contractorname_cleansing = re.sub(beforeStr, afterStr, contractorname_cleansing)

        # 契約者名(クレンジング後)保存
        row['contractorname_cleansing'] = contractorname_cleansing


        # 取次内容(クレンジング後)初期化
        contents_commission_cleansing = row['contents_commission']

        if not pd.isna(contents_commission_cleansing) and contents_commission_cleansing != '':

            # 変換処理(大文字変換、全角文字変換)
            contents_commission_cleansing = utils.to_upper_wide_charactor(contents_commission_cleansing)

            # 改行コードの削除
            contents_commission_cleansing = contents_commission_cleansing.replace('\r\n', '').replace('\n', '')

            if const.CLENSING_CONFIG_CONTENTS_COMMISSION.get('cleansing_settings'):

                # 置換処理実施
                for key in const.CLENSING_CONFIG_CONTENTS_COMMISSION['cleansing_settings']:
                
                    #キー(置換元文字列)が設定されていた場合
                    if key['patternstring'] != '':
                        beforeStr = key['patternstring']
                        afterStr = key['replacementstring']
                        contents_commission_cleansing = re.sub(beforeStr, afterStr, contents_commission_cleansing)
        
        # 取次内容(クレンジング後)保存
        row['contents_commission_cleansing'] = contents_commission_cleansing
        

        # 会社名(情報発信元)(クレンジング後)初期化
        sourcecompany_cleansing = row['sourcecompany']

        if not pd.isna(sourcecompany_cleansing) and sourcecompany_cleansing != '':
            # 変換処理(大文字変換、全角文字変換)
            sourcecompany_cleansing = utils.to_upper_wide_charactor(sourcecompany_cleansing)
        
        # クレンジング済み会社名(情報発信元)保存
        row['sourcecompany_cleansing'] = sourcecompany_cleansing

        
        # 担当者(クレンジング後)(クレンジング後)初期化
        personincharge_cleansing = row['personincharge']

        if not pd.isna(personincharge_cleansing) and personincharge_cleansing != '':
            # 変換処理(大文字変換、全角文字変換)
            personincharge_cleansing = utils.to_upper_wide_charactor(personincharge_cleansing)
        
        # クレンジング済み担当者保存
        row['personincharge_cleansing'] = personincharge_cleansing

        return row
