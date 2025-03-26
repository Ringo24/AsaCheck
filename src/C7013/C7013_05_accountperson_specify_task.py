# 標準ライブラリインポート
import os

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
import inject
import jaconv

# プロジェクトライブラリインポート
from . import const, message
from .task import BaseTask, TaskResult
from .dao.crmdb_dao import CrmDBDao
from .dao.nwmdb_dao import NwmDBDao
from .fastsearch_helper import FastSearchHelper

# アカウント担当者のユーザ(GUID)
ACCOUNTPERSON_INCHARGE_GUID = 'accountperson_incharge_guid'
# アカウント担当者名
ACCOUNTPERSON_INCHARGE_NAME = 'accountperson_incharge_name'

class C7013_05_accountperson_specify_task(BaseTask):
    '''
    アカウント担当者特定タスククラス
    '''

    @inject.autoparams()
    def __init__(self, dao:CrmDBDao, nwm_dao:NwmDBDao, fastSearch:FastSearchHelper):
        '''
        初期化関数

        Args:
            dao: CrmDBDao
            nwm_dao: NwmDBDao
        '''
        self.__dao = dao
        self.__nwm_dao = nwm_dao
        self.__fastSearch = fastSearch
        #親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self, input_data:pd.DataFrame)->pd.DataFrame:
        '''
        アカウント担当者を特定する

        Args:
            input_data: ランク付与済情報DataFrame
        Returns:
            TaskResult: タスク結果クラス
        '''
        df = input_data.copy()

        # 初期登録
        # アカウント担当者のユーザ(GUID)
        df[ACCOUNTPERSON_INCHARGE_GUID] = ""
        # アカウント担当者名
        df[ACCOUNTPERSON_INCHARGE_NAME] = ""

        self._accountperson_specify(df)

        return TaskResult(resultCode=const.BATCH_SUCCESS, resultData=df)

    def _accountperson_specify(self, df):
        '''
        入力データに応じてアカウント担当者特定を行う

        Args:
            df: ランク付与済情報DataFrame
        '''
        # 当初注文内容リスト取得
        ordercontents_config_list = \
            const.APP_CONFIG['accountperson_policy_word_config']['customer_search_ordercontents']

        first_conn_fastsearch = True
        for index, row in df.iterrows():
            dic_find_account_by_fastsearch = {}
            dic_find_account_by_contract_id = {}
            # 企業ID
            cust_id = ""
            # 設置場所住所コード
            setloc_addr_cd = ""
            
            # 当初注文内容が設定内容リストに設定されている場合
            if row.ordercontents in ordercontents_config_list:
                if first_conn_fastsearch:
                    self.__fastSearch.Conn()
                    first_conn_fastsearch = False
                # お客様情報検索
                dic_find_account_by_fastsearch = self._find_account_by_fastsearch(row)
                if dic_find_account_by_fastsearch is None:
                    self.logger.info(message.MSG['MSG0003'], row.contractorname_cleansing,
                                     row.next_account_code)
                    continue
                cust_id = dic_find_account_by_fastsearch.get('customerid')
                setloc_addr_cd = dic_find_account_by_fastsearch.get('accountaddresscode')
            else:
                # 契約ID検索
                if not pd.isna(row.contract_id):
                    # 企業IDと設置場所住所コードを取得する。
                    dic_find_account_by_contract_id = self._find_account_by_contract_id(row)

                if dic_find_account_by_contract_id is None or pd.isna(row.contract_id):
                    if pd.isna(row.ordertelephonenumber):
                        self.logger.info(message.MSG['MSG0004'],
                                         row.contract_id if not pd.isna(row.contract_id) else "",
                                         "")
                    else:
                        # 注文電話番号検索
                        tel, df_find_account_by_telephonenumber = self._find_account_by_telephonenumber(row)
                        if len(df_find_account_by_telephonenumber) == 0:
                            self.logger.info(message.MSG['MSG0004'], 
                                             row.contract_id if not pd.isna(row.contract_id) else "",
                                             tel)
                        elif pd.isna(df_find_account_by_telephonenumber.at[0, 'accountperson_incharge_name']):
                            self.logger.info(message.MSG['MSG0005'],
                                             row.contract_id if not pd.isna(row.contract_id) else "",
                                             tel)
                        else:
                            df.at[index, ACCOUNTPERSON_INCHARGE_GUID] =\
                                df_find_account_by_telephonenumber.at[0, 'accountpersonincharge_guid']
                            df.at[index, ACCOUNTPERSON_INCHARGE_NAME] =\
                                df_find_account_by_telephonenumber.at[0, 'accountperson_incharge_name']
                    continue
                else:
                    cust_id = dic_find_account_by_contract_id.get('CUST_ID')
                    setloc_addr_cd = dic_find_account_by_contract_id.get('SETLOC_ADDR_CD')

                setloc_addr_cd = str(setloc_addr_cd).rjust(11, '0')

            # アカウント担当者情報取得
            df_find_account_by_account = self._find_account_by_account(cust_id, setloc_addr_cd)
            if len(df_find_account_by_account) == 0:
                self.logger.info(message.MSG['MSG0006'], '{}{}'.format(cust_id, setloc_addr_cd))
            elif pd.isna(df_find_account_by_account.at[0, 'accountperson_incharge_name']):
                self.logger.info(message.MSG['MSG0007'], '{}{}'.format(cust_id, setloc_addr_cd))
            else:
                df.at[index, ACCOUNTPERSON_INCHARGE_GUID] = \
                    df_find_account_by_account.at[0, 'accountpersonincharge_guid']
                df.at[index, ACCOUNTPERSON_INCHARGE_NAME] = \
                    df_find_account_by_account.at[0, 'accountperson_incharge_name']
        
        if not first_conn_fastsearch:
            self.__fastSearch.Close()
                
    def _find_account_by_fastsearch(self, row):
        '''
        SharePoint FastSearchを利用し、お客様検索を行う。

        Args:
            row: ランク付与済情報Series
        Returns:
            {}: お客様情報（検索結果が存在しない場合はNoneを返却する）
        '''
        # SharePoint FastSearch
        dic_res = self.__fastSearch.FindAccount(row.contractorname_cleansing, row.next_account_code)

        return dic_res

    def _find_account_by_contract_id(self, row):
        '''
        契約IDをキーにMERCURY-NWM．リスト検索用番号Tから顧客IDと設置場所住所コードを取得する。

        Args:
            row: ランク付与済情報Series
        Returns:
            {}: お客様情報（検索結果が存在しない場合はNoneを返却する）
        '''
        dic_res = self.__nwm_dao.find_account_by_contract_id(row.contract_id)

        return dic_res

    def _find_account_by_telephonenumber(self, row):
        '''
        注文電話番号検索

        注文電話番号をキーに電話番号E・顧客事業所Eからアカウント担当者を取得する。

        Args:
            row: ランク付与済情報Series
        Returns:
            {}: お客様情報Dataframe
        '''
        # 半角化し、ハイフン除去
        tel = jaconv.z2h(row.ordertelephonenumber, digit=True, ascii=True).replace('-','')
        
        # 注文電話番号検索
        df_acc_from_telephonenumber = self.__dao.retrive_accountperson_incharge_from_telephonenumber(tel)
        return tel, df_acc_from_telephonenumber

    def _find_account_by_account(self, customerid, accountaddresscode):
        '''
        アカウント担当者情報取得

        取得した顧客情報を元に顧客事業所Eを検索し、アカウント担当者を取得する。

        Args:
            customerid: 企業ID
            accountaddresscode: 設置場所住所コード
        Returns:
            {}: アカウント担当者Dataframe
        '''
        # アカウント担当者情報取得
        df_acc_from_account = \
            self.__dao.retrive_accountperson_incharge_from_account(customerid, accountaddresscode)
        
        return df_acc_from_account