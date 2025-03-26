# 標準ライブラリインポート
import os

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
import inject
import pymssql
import json5

# プロジェクトライブラリインポート
from . import const, message, utils
from .task import BaseTask, TaskResult
from .dao.crmdb_dao import CrmDBDao
from .dto.dcrm_sdk import Entity, EntityReference, OptionSetValue
from .dcrm_helper import DcrmHelper
from .C7013_04_addresscode_prediction_task import C7013_04_addresscode_prediction_task

class C7013_01_task(BaseTask):
    '''
    処理対象の取次データの抽出し、および事前チェックを行う。
    '''

    # 機能ID
    APP_ID: str = 'C7013_01'
    # 機能名
    APP_NAME: str = 'データ抽出/事前チェック'

    # 担当者選定状況（担当者選定中）
    STATUS_COMMISSION_SELECTION: int = 1

    @inject.autoparams()
    def __init__(self, crmDbDao: CrmDBDao, dcrmHelper: DcrmHelper):
        '''
        初期化関数
        '''

        self.__dao: CrmDBDao = crmDbDao
        self.__conn: pymssql.Connection = self.__dao.conn()
        self.__helper: DcrmHelper = dcrmHelper
        self.__helper.Conn()

        self.__addresscode_prediction_task: C7013_04_addresscode_prediction_task = C7013_04_addresscode_prediction_task()

        self.__result: TaskResult = TaskResult()

        # 親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self, input_data: pd.DataFrame) -> TaskResult:
        '''
        自動差配ユニットに取り次がれた取次を抽出し、
        取次区分判定や必須チェック等の事前チェックを行う。
        また新設置場所から住所コードの特定を行う。
        '''
        self.logger.info(f'タスクを実行します。')

        # ↓↓ここにビジネスロジックを実装します。
        # ================================
        # 初期処理
        # ================================

        # 処理開始メッセージ出力
        self.logger.info(message.MSG['MSG0001'], C7013_01_task.APP_ID, C7013_01_task.APP_NAME)

        # プログラム引数解析
        # → C7013_01_batchで実施

        # 変数・リソース初期化
        autoagent_commission_class_list = []
        autoagent_untargeted_keyword_dict = {}
        new_commission_len = 0
        autoagent_untargeted_len = 0

        # 自動差配対象取次区分設定値取得
        json_file = json5.load(open(const.APP_CONFIG_PATH / 'autoagent_object_class.json', 'r', encoding='utf-8'))
        if 'autoagent_object_class' in json_file.keys():
            autoagent_commission_class_list = [item.get('class') for item in json_file['autoagent_object_class']]
            autoagent_commission_class_list = [item for item in autoagent_commission_class_list if item is not None]
        self.logger.debug('自動差配対象取次区分設定値：%s', autoagent_commission_class_list)

        # ================================
        # 取次情報取得
        # ================================

        # 取次情報取得
        new_commission_df = self._select_autoagent_target_commission()
        new_commission_len = len(new_commission_df)
        self.logger.debug('取次情報抽出件数：%d', new_commission_len)

        # 取次情報取得結果判定
        if new_commission_len == 0:
            # ログ出力
            self.logger.info(message.MSG['MSG0015'])
            # 終了処理
            self.logger.info(message.MSG['MSG0008'], C7013_01_task.APP_ID, C7013_01_task.APP_NAME, new_commission_len, new_commission_len, autoagent_untargeted_len)
            self.__result.resultCode = const.BATCH_SUCCESS
            return self.__result

        # ================================
        # 取次情報ごとの繰返処理
        # ================================

        new_commission_dict = new_commission_df.to_dict(orient='records')
        for index, row in enumerate(new_commission_dict):

            message_for_memo = None
            commission_classification = row['commissionclassification']
            autoagentid_guid = row['autoagentid_guid']
            self.logger.debug('取次区分：%s, 自動差配設定(GUID)：%s', commission_classification, autoagentid_guid)

            # 取次区分判定

            # 自動差配対象取次区分判定
            if pd.isna(commission_classification) or commission_classification not in autoagent_commission_class_list:
                # 自動差配対象外取次区分時処理
                message_for_memo = message.MSG['MSG3001']

            # 必須チェック

            if message_for_memo is None:
                # 契約者名チェック
                message_for_memo = self._check_required(row, 'contractorname', '契約者名')

            if message_for_memo is None:
                # 新設置場所チェック
                message_for_memo = self._check_required(row, 'next_account', '新設置場所')

            if message_for_memo is None:
                # 担当者チェック
                message_for_memo = self._check_required(row, 'personincharge', '担当者')

            if message_for_memo is None:
                # 連絡先電話番号1チェック
                message_for_memo = self._check_required(row, 'connectiontelephonenumber1', '連絡先電話番号1')

            if message_for_memo is None:
                # 当初注文内容チェック
                message_for_memo = self._check_required(row, 'ordercontents', '当初注文内容')

            # 住所コード特定

            if message_for_memo is None:
                # 住所コード特定処理呼び出し
                address_code = self.__addresscode_prediction_task.address2addresscode(row['next_account'])
                self.logger.debug('住所コード：%s', address_code)
                # 住所コード特定処理結果確認
                if address_code is None:
                    # 住所コード特定不能時処理
                    message_for_memo = message.MSG['MSG3003']
                else:
                    new_commission_df.at[index, 'next_account_code'] = address_code

            # 事業部エリアチェック

            if message_for_memo is None:
                # 担当エリア検索
                handle_area_df = self._select_handle_area(address_code, autoagentid_guid)
                handle_area_len = len(handle_area_df)
                self.logger.debug('担当エリア検索結果：%d', handle_area_len)
                # 担当エリア検索結果確認
                if handle_area_len == 0:
                    # 自事業部担当エリア外時処理
                    message_for_memo = message.MSG['MSG3004']

            # 自動差配対象外キーワードチェック

            if message_for_memo is None:
                # 自動差配対象外キーワード一覧存在チェック
                self.logger.debug('自動差配対象外キーワード一覧：%s', autoagent_untargeted_keyword_dict)

                if autoagentid_guid not in autoagent_untargeted_keyword_dict:
                    # 自動差配対象外キーワード取得
                    # データ取得
                    autoagent_untargeted_keyword_df = self._select_autoagent_untargeted_keyword(autoagentid_guid)
                    # データ取得結果処理
                    autoagent_untargeted_keyword_list = autoagent_untargeted_keyword_df.iloc[:, 0].values.tolist()
                    self.logger.debug('自動差配対象外キーワード取得結果：%s', autoagent_untargeted_keyword_list)
                    autoagent_untargeted_keyword_dict[autoagentid_guid] = autoagent_untargeted_keyword_list

                # 自動差配対象外キーワード判定

                # 自動差配対象外キーワード文字列初期化
                autoagent_untargeted_keyword_str = ''

                # キーワードリスト取得
                keyword_list = autoagent_untargeted_keyword_dict[autoagentid_guid]

                # キーワードリストレコードごとの処理
                for keyword in keyword_list:
                    # キーワード判定
                    if keyword in row['contents_commission']:
                        autoagent_untargeted_keyword_str += keyword
                self.logger.debug('自動差配対象外キーワード文字列：%s', autoagent_untargeted_keyword_str)

                # キーワードリストレコードごとの処理結果判定
                if autoagent_untargeted_keyword_str != '':
                    # 自動差配対象外キーワードあり時処理
                    message_for_memo = message.MSG['MSG3005'] % (autoagent_untargeted_keyword_str)

            # 差配担当窓口取次
            self.logger.debug('メモ用メッセージ：%s', message_for_memo)

            if message_for_memo is not None:
                isUpdateSuccess = False

                # 取次E更新
                try:
                    isUpdateSuccess = self._update_new_commission(row)
                # 取次E更新結果確認
                except Exception:
                    self.__result.resultCode = const.BATCH_ERROR
                    self.logger.error(message.MSG['MSG2003'], row['commissionid_guid'])

                if isUpdateSuccess:
                    # メモE登録
                    try:
                        self._insert_annotation(row, message_for_memo)
                    # メモE登録結果確認
                    except Exception:
                        self.__result.resultCode = const.BATCH_ERROR
                        self.logger.error(message.MSG['MSG2004'], row['commissionid_guid'], message_for_memo)

                # 取次情報レコード除外
                new_commission_df.drop(index, inplace=True)
                autoagent_untargeted_len += 1
                self.logger.debug('自動差配対象外件数：%d', autoagent_untargeted_len)

        # ================================
        # 取次情報CSV出力
        # ================================
        # → C7013_01_batchで実施

        # ================================
        # 終了処理
        # ================================

        # 処理終了メッセージ出力
        if self.__result.resultCode == const.BATCH_SUCCESS:
            self.logger.info(message.MSG['MSG0008'], C7013_01_task.APP_ID, C7013_01_task.APP_NAME, new_commission_len, len(new_commission_df), autoagent_untargeted_len)
        elif self.__result.resultCode == const.BATCH_ERROR:
            self.logger.error(message.MSG['MSG2002'], C7013_01_task.APP_ID, C7013_01_task.APP_NAME)
        else:
            self.logger.warning(message.MSG['MSG1002'], C7013_01_task.APP_ID, C7013_01_task.APP_NAME)

        # 戻り値返却
        self.__result.resultData = new_commission_df

        return self.__result
        # ↑↑ここにビジネスロジックを実装します。

    def _select_autoagent_target_commission(self) -> pd.DataFrame:
        '''
        自動差配対象の取次を抽出します。
        '''

        sql = """
            SELECT
              -- 取次GUID
              E1.new_commissionid AS commissionid_guid,
              -- 自動差配ユニットユニット情報
              E1.new_comprehensivecompany_to AS comprehensivecompany_guid,
              E1.new_division_to AS division_guid,
              E1.new_section_to AS section_guid,
              E1.new_unit_to AS unit_guid,
              -- 取次情報
              E1.new_commissionclassification AS commissionclassification,
              E1.new_sourcecompany AS sourcecompany,
              E1.new_contractorname AS contractorname,
              E1.new_next_account AS next_account,
              ISNULL(E1.new_third_person_application, 0) AS third_person_application,
              E1.new_ordertelephonenumber AS ordertelephonenumber,
              E1.new_contract_id AS contract_id,
              ISNULL(E1.new_colab_line, 0) AS colab_line,
              E1.new_contents_commission AS contents_commission,
              E1.new_primarycorrespondenceway AS primarycorrespondenceway,
              E1.new_personincharge AS personincharge,
              E1.new_connectiontelephonenumber1 AS connectiontelephonenumber1,
              E1.new_ordercontents AS ordercontents,
              -- 自動差配設定情報
              E2.new_autoagentid AS autoagentid_guid,
              -- 差配担当窓口ユニット情報
              BMS.businessunitid AS agent_window_comprehensivecompany_guid,
              BMB.businessunitid AS agent_window_division_guid,
              BMBM.businessunitid AS agent_window_section_guid,
              E2.new_agent_window_unit AS agent_window_unit_guid
            FROM
              -- 取次E
              NTTEAST_MSCRM.dbo.new_commission E1
              -- 自動差配設定E
              INNER JOIN NTTEAST_MSCRM.dbo.new_autoagent E2
                ON E2.new_autoagent_unit = E1.new_unit_to
                AND E2.statecode = 0
              -- 部署E
              -- 差配担当窓口ユニットの部署情報
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BMU
                ON BMU.businessunitid = E2.new_agent_window_unit
              -- 差配担当窓口ユニット総合会社の部署情報
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BMS
                ON BMS.new_busho_code = LEFT(BMU.new_busho_code, 3) + '000000000'
              -- 差配担当窓口ユニット部の部署情報
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BMB
                ON BMB.new_busho_code = LEFT(BMU.new_busho_code, 6) + '000000'
              -- 差配担当窓口ユニット部門の部署情報
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BMBM
                ON BMBM.new_busho_code = LEFT(BMU.new_busho_code, 9) + '000'
            WHERE
              E1.statecode = 0
              AND E1.new_draft = 0
            ORDER BY
              E1.modifiedon ASC
            """

        return pd.read_sql_query(sql, con=self.__conn)

    def _select_handle_area(self, next_account_code: str, autoagentid_guid: str) -> pd.DataFrame:
        '''
        担当エリア情報を検索します。
        '''

        sql = f"""
            SELECT TOP(1)
              1
            FROM
              -- スルー取次・支店優先取次E
              NTTEAST_MSCRM.dbo.new_autoagent_thru_brnc_commission E1
              -- 担当エリアE
              INNER JOIN NTTEAST_MSCRM.dbo.new_autoagent_handle_area E2
                ON E2.new_autoagent_thru_brnc_commission = E1.new_autoagent_thru_brnc_commissionid
                AND E2.new_addresscode IN (
                  '{next_account_code}',
                  LEFT('{next_account_code}', 8) + '000',
                  LEFT('{next_account_code}', 5) + '000000',
                  LEFT('{next_account_code}', 2) + '000000000'
                )
                AND E2.statecode = 0
            WHERE
              E1.new_autoagent = '{autoagentid_guid}'
              AND E1.statecode = 0
            """

        return pd.read_sql_query(sql, con=self.__conn)

    def _select_autoagent_untargeted_keyword(self, autoagentid_guid: str) -> pd.DataFrame:
        '''
        自動差配対象外キーワードを取得します。
        '''

        sql = f"""
            SELECT
              E1.new_autoagent_untargeted_keyword
            FROM
              -- 自動差配対象外キーワード設定E
              NTTEAST_MSCRM.dbo.new_autoagent_untargeted_keyword E1
            WHERE
              E1.new_autoagent = '{autoagentid_guid}'
              AND E1.statecode = 0
            """

        return pd.read_sql_query(sql, con=self.__conn)

    def _check_required(self, row: dict, key: str, msg_arg: str) -> str:
        '''
        必須チェックを実施します。
        '''

        self.logger.debug('%s：%s', msg_arg, row[key])

        # 入力チェック
        if pd.isna(row[key]) or not row[key]:
            # 未入力時処理
            return message.MSG['MSG3002'] % (msg_arg)
        else:
            return None

    def _update_new_commission(self, row: dict) -> bool:
        '''
        取次Eを更新します。
        '''

        entity = Entity('new_commission')
        entity.Id = row['commissionid_guid']
        entity.Attributes['new_comprehensivecompany_to'] = EntityReference(entity.LogicalName, row['agent_window_comprehensivecompany_guid'])
        entity.Attributes['new_division_to'] = EntityReference(entity.LogicalName, row['agent_window_division_guid'])
        entity.Attributes['new_section_to'] = EntityReference(entity.LogicalName, row['agent_window_section_guid'])
        entity.Attributes['new_unit_to'] = EntityReference(entity.LogicalName, row['agent_window_unit_guid'])
        entity.Attributes['new_personincharge_to'] = None
        entity.Attributes['new_comprehensivecompany_from'] = EntityReference(entity.LogicalName, row['comprehensivecompany_guid'])
        entity.Attributes['new_division_from'] = EntityReference(entity.LogicalName, row['division_guid'])
        entity.Attributes['new_section_from'] = EntityReference(entity.LogicalName, row['section_guid'])
        entity.Attributes['new_unit_from'] = EntityReference(entity.LogicalName, row['unit_guid'])
        entity.Attributes['new_personincharge_from'] = None
        entity.Attributes['new_comprehensivecompany_next_to'] = None
        entity.Attributes['new_division_next_to'] = None
        entity.Attributes['new_section_next_to'] = None
        entity.Attributes['new_unit_next_to'] = None
        entity.Attributes['new_personincharge_next_to'] = None
        entity.Attributes['new_comprehensivecompany_next_from'] = None
        entity.Attributes['new_division_next_from'] = None
        entity.Attributes['new_section_next_from'] = None
        entity.Attributes['new_unit_next_from'] = None
        entity.Attributes['new_personincharge_next_from'] = None
        entity.Attributes['new_status_commission'] = OptionSetValue(C7013_01_task.STATUS_COMMISSION_SELECTION)

        self.__helper.UpdateEntity(entity)

        return True

    def _insert_annotation(self, row: dict, notetext: str) -> bool:
        '''
        メモEを更新します。
        '''

        entity = Entity('annotation')
        entity.Attributes['objectid'] = EntityReference('new_commission', row['commissionid_guid'])
        entity.Attributes['notetext'] = notetext

        self.__helper.CreateEntity(entity)

        return True
