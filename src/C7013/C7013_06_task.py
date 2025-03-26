# 標準ライブラリインポート
import os
import datetime
import csv

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
import inject
import pymssql

# プロジェクトライブラリインポート
from . import const, message
from .task import BaseTask, TaskResult
from .dao.crmdb_dao import CrmDBDao
from .dto.dcrm_sdk import Entity, EntityReference, OptionSetValue
from .dcrm_helper import DcrmHelper

class C7013_06_task(BaseTask):
    '''
    アカウント有無や担当エリア、ランク等により差配先を決定し取次を行う
    '''

    # 機能ID
    APP_ID: str = 'C7013_06'
    # 機能名
    APP_NAME: str = '自動差配'

    # 対応方法（訪問）
    PRIMARYCORRESPONDENCEWAY_APPOINT: int = 2

    # 第三者申込（第三者申込対象）
    THIRD_PERSON_APPLICATION_YES: int = 1

    # コラボ回線（コラボ回線対象）
    COLAB_LINE_YES: int = 1

    # 差配種類（スルー取次）
    AGENT_CATEGORY_THROUGH: int = 100
    # 差配種類（ノータッチ取次）
    AGENT_CATEGORY_NOTOUCH: int = 200
    # 差配種類（支店優先取次）
    AGENT_CATEGORY_PRIORITY: int = 300
    # 差配種類（通常差配）
    AGENT_CATEGORY_NORMAL: int = 400

    # 担当者選定状況（担当者選定中）
    STATUS_COMMISSION_SELECTION: int = 1

    # BCC取次状況（BCC未対応取次）
    BCC_STATUS_COMMISSION_NO: int = 100
    # BCC取次状況（BCC対応取次）
    BCC_STATUS_COMMISSION_YES: int = 200

    # BCC未対応取次理由（アカウント有）
    BCC_UNSUPPORTED_REASON_ACCOUNT: int = 100
    # BCC未対応取次理由（訪問希望）
    BCC_UNSUPPORTED_REASON_APPOINT: int = 200

    # 活動状況（活動中(今年度)）
    ACTIVITY_CONDITION_ACTIVE_THISYEAR: int = 200
    # 商品型紙情報要否（不要）
    COMMODITY_PAPER_FLG_UNNECESSARY: int = 100

    # 提案PJ名の接頭語
    OPPORTUNITY_NAME_PREFIX: str = 'チケット差配自動化_'
    # 提案PJ名の最大長
    OPPORTUNITY_NAME_LENGTH_MAX: int = 40

    # 提案プロジェクトEの作成有無（無）
    IS_CREATED_OPPORTUNITY_NO: str = '0'
    # 提案プロジェクトEの作成有無（有）
    IS_CREATED_OPPORTUNITY_YES: str = '1'

    # 空の差配先情報ディクショナリ
    AGENT_TO_DICT_EMPTY = {
        'agent_window_comprehensivecompany_guid': '',
        'agent_window_division_guid': '',
        'agent_window_section_guid': '',
        'agent_window_unit_guid': ''
    }

    # エラーCSVファイル名の接頭語
    ERROR_CSV_FILE_NAME_PREFIX: str = 'C7013_06_Err_'
    # エラーCSVファイル名の拡張子
    ERROR_CSV_FILE_NAME_EXT: str = '.csv'

    @inject.autoparams()
    def __init__(self, crmDbDao: CrmDBDao, dcrmHelper: DcrmHelper):
        '''
        初期化関数
        '''

        self.__dao: CrmDBDao = crmDbDao
        self.__conn: pymssql.Connection = self.__dao.conn()
        self.__helper: DcrmHelper = dcrmHelper
        self.__helper.Conn()

        self.__result: TaskResult = TaskResult()
        self.__output_data: pd.DataFrame = None
        self.__error_list: list = []

        # 親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self, input_data: pd.DataFrame) -> TaskResult:
        '''
        スルー取次、ノータッチ取次、支店優先取次、通常差配の優先順位で差配先を決定し、取次を行う。
        また、ノータッチ取次、支店優先取次、通常差配の場合、提案プロジェクトの作成を行う。
        '''

        self.logger.info(f'タスクを実行します。')

        # ↓↓ここにビジネスロジックを実装します。
        # ================================
        # 初期処理
        # ================================

        # 処理開始メッセージ出力
        self.logger.info(message.MSG['MSG0001'], C7013_06_task.APP_ID, C7013_06_task.APP_NAME)

        # プログラム引数解析
        # → C7013_06_batchで実施

        # 変数・リソース初期化
        sysdate = datetime.datetime.now()
        today_str = f'{sysdate:%Y/%m/%d}'

        # 全国公開データ取得
        nationwide_release_df = self._select_nationwide_release()
        nationwide_release_len = len(nationwide_release_df)
        self.logger.debug('全国公開データ取得件数：%d', nationwide_release_len)

        if nationwide_release_len != 1:
            # ログ出力
            self.logger.error(message.MSG['MSG2007'])
            # 終了処理
            self.logger.error(message.MSG['MSG2002'], C7013_06_task.APP_ID, C7013_06_task.APP_NAME)
            self.__result.resultCode = const.BATCH_ERROR
            return self.__result

        teamid = nationwide_release_df.iat[0, 0]
        self.logger.debug('チーム：%s', teamid)

        # 通常差配用日付情報取得
        sysdate_utc_df = self._select_sysdate_utc()
        sysdate_utc_dict = sysdate_utc_df.iloc[0].to_dict()

        # ================================
        # アカウント/施策キーワード付与済情報CSV読込
        # ================================
        # → C7013_06_batchで実施

        # ================================
        # アカウント/施策キーワード付与済情報ごとの繰返処理
        # ================================

        self.logger.debug('入力データ件数：%d', len(input_data))
        self.__output_data = input_data.copy()
        input_data_dict = input_data.to_dict(orient='records')

        for index, row in enumerate(input_data_dict):

            message_for_memo = None
            self.logger.debug('index = %d', index)

            # 該当ランク確認
            rank_system = row['rank_system']
            self.logger.debug('ランク(システム)：%s', rank_system)

            # 上記条件に一致した場合
            if pd.isna(rank_system):
                message_for_memo = message.MSG['MSG3006']
                # 取次保存
                if self._update_commission(row, row, None, None, None, message_for_memo):
                    self._set_commission_data(index, None, C7013_06_task.AGENT_TO_DICT_EMPTY, [], [], [], C7013_06_task.IS_CREATED_OPPORTUNITY_NO, message_for_memo)
                continue
            # 上記条件に一致しなかった場合

            # スルー取次判断

            # BI本部、支店BIアカウントによる差配
            bi_account_agent_df = self._select_bi_account_agent(row['accountperson_incharge_guid'], rank_system)
            bi_account_agent_len = len(bi_account_agent_df)
            self.logger.debug('BI本部、支店BIアカウントによる差配情報取得件数：%d', bi_account_agent_len)

            if 0 < bi_account_agent_len:
                # BI本部、支店BIアカウントによる差配の取次情報保持
                bi_account_agent_dict = bi_account_agent_df.iloc[0].to_dict()
                # 取次保存
                if self._update_commission(row, bi_account_agent_dict, C7013_06_task.BCC_STATUS_COMMISSION_NO, C7013_06_task.BCC_UNSUPPORTED_REASON_ACCOUNT, C7013_06_task.AGENT_CATEGORY_THROUGH, message_for_memo):
                    self._set_commission_data(index, C7013_06_task.AGENT_CATEGORY_THROUGH, bi_account_agent_dict, [], [], [], C7013_06_task.IS_CREATED_OPPORTUNITY_NO, message_for_memo)
                continue

            # 訪問希望差配
            primary_correspondence_way = row['primarycorrespondenceway']
            self.logger.debug('対応方法：%s', primary_correspondence_way)

            # 読込CSVの対応方法が"2"(訪問)の場合
            if not pd.isna(primary_correspondence_way) and primary_correspondence_way == C7013_06_task.PRIMARYCORRESPONDENCEWAY_APPOINT:
                appoint_agent_df = self._select_appoint_agent(row['autoagentid_guid'], row['next_account_code'], rank_system)
                appoint_agent_len = len(appoint_agent_df)
                self.logger.debug('訪問希望差配情報取得件数：%d', appoint_agent_len)

                if 0 < appoint_agent_len:
                    # 訪問希望差配の取次情報保持
                    appoint_agent_dict = appoint_agent_df.iloc[0].to_dict()
                    # 取次保存
                    if self._update_commission(row, appoint_agent_dict, C7013_06_task.BCC_STATUS_COMMISSION_NO, C7013_06_task.BCC_UNSUPPORTED_REASON_APPOINT, C7013_06_task.AGENT_CATEGORY_THROUGH, message_for_memo):
                        self._set_commission_data(index, C7013_06_task.AGENT_CATEGORY_THROUGH, appoint_agent_dict, [], [], [], C7013_06_task.IS_CREATED_OPPORTUNITY_NO, message_for_memo)
                    continue
            # 読込CSVの対応方法が"2"(訪問)以外の場合

            # ノータッチ取次判断

            # ノータッチ取次情報取得
            notouch_commission_df = self._select_notouch_commission(row['autoagentid_guid'], row['accountperson_incharge_guid'], rank_system)
            notouch_commission_len = len(notouch_commission_df)
            self.logger.debug('ノータッチ取次情報取得件数：%d', notouch_commission_len)

            if 0 < notouch_commission_len:
                # ノータッチ取次判断の取次情報保持
                notouch_commission_dict = notouch_commission_df.iloc[0].to_dict()
                # 提案プロジェクト作成
                if self._insert_opportunity(row, teamid, today_str):
                    # 取次保存
                    if self._update_commission(row, notouch_commission_dict, C7013_06_task.BCC_STATUS_COMMISSION_NO, None, C7013_06_task.AGENT_CATEGORY_NOTOUCH, message_for_memo):
                        self._set_commission_data(index, C7013_06_task.AGENT_CATEGORY_NOTOUCH, notouch_commission_dict, [], [], [], C7013_06_task.IS_CREATED_OPPORTUNITY_YES, message_for_memo)
                continue

            # 支店優先取次判断

            # 支店優先取次情報の取得
            priority_commission_df = self._select_priority_commission(row['autoagentid_guid'], row['next_account_code'], rank_system)
            priority_commission_len = len(priority_commission_df)
            self.logger.debug('支店優先取次情報取得件数：%d', priority_commission_len)

            if 0 < priority_commission_len:
                # 支店優先取次判断の取次情報保持
                priority_commission_dict = priority_commission_df.iloc[0].to_dict()
                # 提案プロジェクト作成
                if self._insert_opportunity(row, teamid, today_str):
                    # 取次保存
                    if self._update_commission(row, priority_commission_dict, C7013_06_task.BCC_STATUS_COMMISSION_NO, None, C7013_06_task.AGENT_CATEGORY_PRIORITY, message_for_memo):
                        self._set_commission_data(index, C7013_06_task.AGENT_CATEGORY_PRIORITY, priority_commission_dict, [], [], [], C7013_06_task.IS_CREATED_OPPORTUNITY_YES, message_for_memo)
                continue

            # 第三者申込判断
            third_person_application = row['third_person_application']
            self.logger.debug('第三者申込：%s', third_person_application)

            # 第三者申込対象判断
            if third_person_application == C7013_06_task.THIRD_PERSON_APPLICATION_YES:
                # 差配先ユニット特定
                is_third_person_application = True

                # 第三者申込判断の差配対象ユニット一覧取得
                third_person_application_agent_unit_df = self._select_third_person_application_agent_unit(row['autoagentid_guid'], rank_system)
                third_person_application_agent_unit_len = len(third_person_application_agent_unit_df)
                self.logger.debug('第三者申込差配先ユニット一覧取得件数：%d', third_person_application_agent_unit_len)

                if 0 < third_person_application_agent_unit_len:
                    # 第三者申込判断の差配先の決定
                    third_person_application_agent_unit_list = third_person_application_agent_unit_df.iloc[:, 0].values.tolist()
                    third_person_application_agent_df = self._select_agent(row['unit_guid'], row['agent_window_unit_guid'], third_person_application_agent_unit_list, sysdate_utc_dict, rank_system)
                    third_person_application_agent_len = len(third_person_application_agent_df)
                    self.logger.debug('差配先取得（第三者申込）件数：%d', third_person_application_agent_len)

                    if 0 < third_person_application_agent_len:
                        # 第三者申込判断の取次情報保持
                        third_person_application_agent_dict = third_person_application_agent_df.iloc[0].to_dict()
                        # 提案プロジェクト作成
                        if self._insert_opportunity(row, teamid, today_str):
                            # 取次保存
                            if self._update_commission(row, third_person_application_agent_dict, C7013_06_task.BCC_STATUS_COMMISSION_YES, None, C7013_06_task.AGENT_CATEGORY_NORMAL, message_for_memo):
                                third_person_application_agent_amount_list = third_person_application_agent_df['agent_amount'].values.tolist()
                                third_person_application_agent_object_list = third_person_application_agent_df['agent_window_unit_guid'].values.tolist()
                                third_person_application_agent_rate_list = third_person_application_agent_df['agentrate_instant'].values.tolist()
                                self._set_commission_data(index, C7013_06_task.AGENT_CATEGORY_NORMAL, third_person_application_agent_dict, third_person_application_agent_amount_list, third_person_application_agent_object_list, third_person_application_agent_rate_list, C7013_06_task.IS_CREATED_OPPORTUNITY_YES, message_for_memo)
                        continue
                    else:
                        is_third_person_application = False
                else:
                    is_third_person_application = False

                # 窓口担当へ取次(第三者申込判断)
                if not is_third_person_application:
                    message_for_memo = message.MSG['MSG3007']
                    # 取次保存
                    if self._update_commission(row, row, None, None, None, message_for_memo):
                        self._set_commission_data(index, None, C7013_06_task.AGENT_TO_DICT_EMPTY, [], [], [], C7013_06_task.IS_CREATED_OPPORTUNITY_NO, message_for_memo)
                    continue

            # コラボ回線判断
            colab_line = row['colab_line']
            self.logger.debug('コラボ回線：%s', colab_line)

            # コラボ回線対象判断
            if colab_line == C7013_06_task.COLAB_LINE_YES:
                # 差配先ユニット特定
                is_colab_line = True

                # コラボ回線判断の差配対象ユニット一覧取得
                colab_line_agent_unit_df = self._select_colab_line_agent_unit(row['autoagentid_guid'], rank_system)
                colab_line_agent_unit_len = len(colab_line_agent_unit_df)
                self.logger.debug('コラボ回線差配先ユニット一覧取得件数：%d', colab_line_agent_unit_len)

                if 0 < colab_line_agent_unit_len:
                    # コラボ回線判断の差配先の決定
                    colab_line_agent_unit_list = colab_line_agent_unit_df.iloc[:, 0].values.tolist()
                    colab_line_agent_df = self._select_agent(row['unit_guid'], row['agent_window_unit_guid'], colab_line_agent_unit_list, sysdate_utc_dict, rank_system)
                    colab_line_agent_len = len(colab_line_agent_df)
                    self.logger.debug('差配先取得（コラボ回線）件数：%d', colab_line_agent_len)

                    if 0 < colab_line_agent_len:
                        # コラボ回線判断の取次情報保持
                        colab_line_agent_dict = colab_line_agent_df.iloc[0].to_dict()
                        # 提案プロジェクト作成
                        if self._insert_opportunity(row, teamid, today_str):
                            # 取次保存
                            if self._update_commission(row, colab_line_agent_dict, C7013_06_task.BCC_STATUS_COMMISSION_YES, None, C7013_06_task.AGENT_CATEGORY_NORMAL, message_for_memo):
                                colab_line_agent_amount_list = colab_line_agent_df['agent_amount'].values.tolist()
                                colab_line_agent_object_list = colab_line_agent_df['agent_window_unit_guid'].values.tolist()
                                colab_line_agent_rate_list = colab_line_agent_df['agentrate_instant'].values.tolist()
                                self._set_commission_data(index, C7013_06_task.AGENT_CATEGORY_NORMAL, colab_line_agent_dict, colab_line_agent_amount_list, colab_line_agent_object_list, colab_line_agent_rate_list, C7013_06_task.IS_CREATED_OPPORTUNITY_YES, message_for_memo)
                        continue
                    else:
                        is_colab_line = False
                else:
                    is_colab_line = False

                # 窓口担当へ取次(コラボ回線判断)
                if not is_colab_line:
                    message_for_memo = message.MSG['MSG3007']
                    # 取次保存
                    if self._update_commission(row, row, None, None, None, message_for_memo):
                        self._set_commission_data(index, None, C7013_06_task.AGENT_TO_DICT_EMPTY, [], [], [], C7013_06_task.IS_CREATED_OPPORTUNITY_NO, message_for_memo)
                    continue

            # ランクによる取次判断

            # 差配先ユニット特定
            is_rank = True

            # 差配対象ユニット一覧の抽出
            rank_agent_unit_df = self._select_rank_agent_unit(row['autoagentid_guid'], rank_system)
            rank_agent_unit_len = len(rank_agent_unit_df)
            self.logger.debug('ランクによる取次差配先ユニット一覧取得件数：%d', rank_agent_unit_len)

            if 0 < rank_agent_unit_len:
                # ランクによる取次判断の差配先の決定
                rank_agent_unit_list = rank_agent_unit_df.iloc[:, 0].values.tolist()
                rank_agent_df = self._select_agent(row['unit_guid'], row['agent_window_unit_guid'], rank_agent_unit_list, sysdate_utc_dict, rank_system)
                rank_agent_len = len(rank_agent_df)
                self.logger.debug('差配先取得（ランク）件数：%d', rank_agent_len)

                if 0 < rank_agent_len:
                    # ランクによる取次判断の取次情報保持
                    rank_agent_dict = rank_agent_df.iloc[0].to_dict()
                    # 提案プロジェクト作成
                    if self._insert_opportunity(row, teamid, today_str):
                        # 取次保存
                        if self._update_commission(row, rank_agent_dict, C7013_06_task.BCC_STATUS_COMMISSION_YES, None, C7013_06_task.AGENT_CATEGORY_NORMAL, message_for_memo):
                            rank_agent_amount_list = rank_agent_df['agent_amount'].values.tolist()
                            rank_agent_object_list = rank_agent_df['agent_window_unit_guid'].values.tolist()
                            rank_agent_rate_list = rank_agent_df['agentrate_instant'].values.tolist()
                            self._set_commission_data(index, C7013_06_task.AGENT_CATEGORY_NORMAL, rank_agent_dict, rank_agent_amount_list, rank_agent_object_list, rank_agent_rate_list, C7013_06_task.IS_CREATED_OPPORTUNITY_YES, message_for_memo)
                    continue
                else:
                    is_rank = False
            else:
                is_rank = False

            # 窓口担当へ取次(ランクによる取次判断)
            if not is_rank:
                message_for_memo = message.MSG['MSG3007']
                # 取次保存
                if self._update_commission(row, row, None, None, None, message_for_memo):
                    self._set_commission_data(index, None, C7013_06_task.AGENT_TO_DICT_EMPTY, [], [], [], C7013_06_task.IS_CREATED_OPPORTUNITY_NO, message_for_memo)
                continue

        # ================================
        # 自動差配済情報CSV出力
        # ================================

        # 自動差配済情報CSV出力
        # → C7013_06_batchで実施

        # エラーCSV出力
        if self.__error_list:
            file_name = f'{C7013_06_task.ERROR_CSV_FILE_NAME_PREFIX}{sysdate:%Y%m%d%H%M%S}{C7013_06_task.ERROR_CSV_FILE_NAME_EXT}'
            self.logger.debug('エラーCSVファイル名：%s', file_name)
            error_df = pd.DataFrame.from_dict(self.__error_list)
            error_df.to_csv(const.APP_DATA_PATH / file_name, sep=',', header=True, index=False, mode='w', encoding='utf-8', quoting=csv.QUOTE_ALL, line_terminator='\r\n')
            self.__result.resultCode = const.BATCH_ERROR

        # ================================
        # 終了処理
        # ================================

        # 処理終了メッセージ出力
        if self.__result.resultCode == const.BATCH_SUCCESS:
            self.logger.info(message.MSG['MSG0002'], C7013_06_task.APP_ID, C7013_06_task.APP_NAME)
        elif self.__result.resultCode == const.BATCH_ERROR:
            self.logger.error(message.MSG['MSG2002'], C7013_06_task.APP_ID, C7013_06_task.APP_NAME)
        else:
            self.logger.warning(message.MSG['MSG1002'], C7013_06_task.APP_ID, C7013_06_task.APP_NAME)

        # 戻り値返却
        self.__result.resultData = self.__output_data

        return self.__result
        # ↑↑ここにビジネスロジックを実装します。

    def _select_nationwide_release(self) -> pd.DataFrame:
        '''
        チームEから全国公開レコードのGUIDを取得する。
        '''

        self.logger.debug('[全国公開取得]')

        sql = """
            SELECT
              E1.teamid AS teamid
            FROM
              -- チームE
              NTTEAST_MSCRM.dbo.team E1
            WHERE
              E1.name = '全国公開'
            """

        return pd.read_sql_query(sql, con=self.__conn)

    def _select_bi_account_agent(self, accountperson_incharge_guid: str, rank_system: int) -> pd.DataFrame:
        '''
        BI本部、支店BIアカウントによる差配情報を取得する。
        '''

        self.logger.debug('[BI本部、支店BIアカウントによる差配情報取得]')
        self.logger.debug('アカウント担当者のユーザ(GUID)：%s, ランク(システム)：%s', accountperson_incharge_guid, rank_system)

        accountperson_incharge_guid = str(const.EMPTY_UUID) if pd.isna(accountperson_incharge_guid) or not accountperson_incharge_guid else accountperson_incharge_guid

        sql = f"""
            SELECT TOP 1
              -- 総合会社の部署E
              BS.businessunitid AS agent_window_comprehensivecompany_guid,
              -- 部の部署E
              BB.businessunitid AS agent_window_division_guid,
              -- 部門の部署E
              BBM.businessunitid AS agent_window_section_guid,
              -- スルー取次・支店優先取次設定E
              E1.new_unit AS agent_window_unit_guid
            FROM
              -- スルー取次・支店優先取次設定E
              NTTEAST_MSCRM.dbo.new_autoagent_thru_brnc_commission E1
              -- 支店BIアカウント所属部門設定E
              INNER JOIN NTTEAST_MSCRM.dbo.new_autoagent_branch_account E2
                ON E2.new_autoagent_thru_brnc_commission = E1.new_autoagent_thru_brnc_commissionid
                AND E2.statecode = 0
              -- ユーザーE
              INNER JOIN NTTEAST_MSCRM.dbo.systemuser E3
                ON E3.new_section = E2.new_section
                AND E3.systemuserid = '{accountperson_incharge_guid}'
              -- 部署E
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BU
                ON BU.businessunitid = E1.new_unit
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BBM
                ON BBM.new_busho_code = LEFT(BU.new_busho_code, 9) + '000'
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BB
                ON BB.new_busho_code = LEFT(BU.new_busho_code, 6) + '000000'
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BS
                ON BS.new_busho_code = LEFT(BU.new_busho_code, 3) + '000000000'
            WHERE
              E1.statecode = 0
              AND (
                CASE {rank_system}
                  WHEN 100 THEN E1.new_rank_account_a
                  WHEN 200 THEN E1.new_rank_account_b
                  WHEN 300 THEN E1.new_rank_account_c
                  WHEN 400 THEN E1.new_rank_account_d
                  ELSE E1.new_rank_account_none
                END
              ) = 1
            ORDER BY
              E1.new_autoagent_thru_brnc_commissionid ASC,
              E2.new_autoagent_branch_accountid ASC,
              E3.systemuserid ASC,
              BU.businessunitid ASC
            """

        return pd.read_sql_query(sql, con=self.__conn)

    def _select_appoint_agent(self, autoagentid_guid: str, next_account_code: str, rank_system: int) -> pd.DataFrame:
        '''
        訪問希望差配情報を取得する。
        '''

        self.logger.debug('[訪問希望差配情報取得]')
        self.logger.debug('自動差配設定(GUID)：%s, 新設置場所(住所コード)：%s, ランク(システム)：%s', autoagentid_guid, next_account_code, rank_system)

        sql = f"""
            SELECT TOP 1
              -- 総合会社の部署E
              BS.businessunitid AS agent_window_comprehensivecompany_guid,
              -- 部の部署E
              BB.businessunitid AS agent_window_division_guid,
              -- 部門の部署E
              BBM.businessunitid AS agent_window_section_guid,
              -- スルー取次・支店優先取次設定E
              E1.new_unit AS agent_window_unit_guid
            FROM
              -- スルー取次・支店優先取次設定E
              NTTEAST_MSCRM.dbo.new_autoagent_thru_brnc_commission E1
              -- 担当エリア設定E
              INNER JOIN NTTEAST_MSCRM.dbo.new_autoagent_handle_area E2
                ON E2.new_autoagent_thru_brnc_commission = E1.new_autoagent_thru_brnc_commissionid
                AND E2.statecode = 0
                AND (
                     E2.new_addresscode = '{next_account_code}'
                  OR E2.new_addresscode = LEFT('{next_account_code}', 8) + '000'
                  OR E2.new_addresscode = LEFT('{next_account_code}', 5) + '000000'
                  OR E2.new_addresscode = LEFT('{next_account_code}', 2) + '000000000'
                )
              -- 部署E
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BU
                ON BU.businessunitid = E1.new_unit
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BBM
                ON BBM.new_busho_code = LEFT(BU.new_busho_code, 9) + '000'
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BB
                ON BB.new_busho_code = LEFT(BU.new_busho_code, 6) + '000000'
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BS
                ON BS.new_busho_code = LEFT(BU.new_busho_code, 3) + '000000000'
            WHERE
              E1.statecode = 0
              AND E1.new_autoagent = '{autoagentid_guid}'
              AND (
                CASE {rank_system}
                  WHEN 100 THEN E1.new_rank_appoint_a
                  WHEN 200 THEN E1.new_rank_appoint_b
                  WHEN 300 THEN E1.new_rank_appoint_c
                  WHEN 400 THEN E1.new_rank_appoint_d
                  ELSE E1.new_rank_appoint_none
                END
              ) = 1
            ORDER BY
              E2.new_addresscode DESC
            """

        return pd.read_sql_query(sql, con=self.__conn)

    def _select_notouch_commission(self, autoagentid_guid: str, accountperson_incharge_guid: str, rank_system: int) -> pd.DataFrame:
        '''
        ノータッチ取次情報を取得する。
        '''

        self.logger.debug('[ノータッチ取次情報取得]')
        self.logger.debug('自動差配設定(GUID)：%s, アカウント担当者のユーザ(GUID)：%s, ランク(システム)：%s', autoagentid_guid, accountperson_incharge_guid, rank_system)

        accountperson_incharge_guid = str(const.EMPTY_UUID) if pd.isna(accountperson_incharge_guid) or not accountperson_incharge_guid else accountperson_incharge_guid

        sql = f"""
            SELECT TOP 1
              -- 総合会社の部署E
              BS.businessunitid AS agent_window_comprehensivecompany_guid,
              -- 部の部署E
              BB.businessunitid AS agent_window_division_guid,
              -- 部門の部署E
              BBM.businessunitid AS agent_window_section_guid,
              -- ノータッチ取次設定E
              E1.new_commission_unit AS agent_window_unit_guid
            FROM
              -- ノータッチ取次設定E
              NTTEAST_MSCRM.dbo.new_autoagent_notouch_commission E1
              -- 部署E
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BU
                ON BU.businessunitid = E1.new_commission_unit
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BBM
                ON BBM.new_busho_code = LEFT(BU.new_busho_code, 9) + '000'
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BB
                ON BB.new_busho_code = LEFT(BU.new_busho_code, 6) + '000000'
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BS
                ON BS.new_busho_code = LEFT(BU.new_busho_code, 3) + '000000000'
            WHERE
              E1.statecode = 0
              AND E1.new_autoagent = '{autoagentid_guid}'
              AND E1.new_centeraccount = '{accountperson_incharge_guid}'
              AND (
                CASE {rank_system}
                  WHEN 100 THEN E1.new_rank_notouch_a
                  WHEN 200 THEN E1.new_rank_notouch_b
                  WHEN 300 THEN E1.new_rank_notouch_c
                  WHEN 400 THEN E1.new_rank_notouch_d
                  ELSE E1.new_rank_notouch_none
                END
              ) = 1
            ORDER BY
              E1.new_autoagent_notouch_commissionid ASC,
              BU.businessunitid ASC
            """

        return pd.read_sql_query(sql, con=self.__conn)

    def _select_priority_commission(self, autoagentid_guid: str, next_account_code: str, rank_system: int) -> pd.DataFrame:
        '''
        支店優先取次情報を取得する。
        '''

        self.logger.debug('[支店優先取次情報取得]')
        self.logger.debug('自動差配設定(GUID)：%s, 新設置場所(住所コード)：%s, ランク(システム)：%s', autoagentid_guid, next_account_code, rank_system)

        sql = f"""
            SELECT TOP 1
              -- 総合会社の部署E
              BS.businessunitid AS agent_window_comprehensivecompany_guid,
              -- 部の部署E
              BB.businessunitid AS agent_window_division_guid,
              -- 部門の部署E
              BBM.businessunitid AS agent_window_section_guid,
              -- スルー取次・支店優先取次設定E
              E1.new_unit AS agent_window_unit_guid
            FROM
              -- スルー取次・支店優先取次設定E
              NTTEAST_MSCRM.dbo.new_autoagent_thru_brnc_commission E1
              -- 担当エリア設定E
              INNER JOIN NTTEAST_MSCRM.dbo.new_autoagent_handle_area E2
                ON E2.new_autoagent_thru_brnc_commission = E1.new_autoagent_thru_brnc_commissionid
                AND E2.statecode = 0
                AND (
                     E2.new_addresscode = '{next_account_code}'
                  OR E2.new_addresscode = LEFT('{next_account_code}', 8) + '000'
                  OR E2.new_addresscode = LEFT('{next_account_code}', 5) + '000000'
                  OR E2.new_addresscode = LEFT('{next_account_code}', 2) + '000000000'
                )
              -- 部署E
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BU
                ON BU.businessunitid = E1.new_unit
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BBM
                ON BBM.new_busho_code = LEFT(BU.new_busho_code, 9) + '000'
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BB
                ON BB.new_busho_code = LEFT(BU.new_busho_code, 6) + '000000'
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BS
                ON BS.new_busho_code = LEFT(BU.new_busho_code, 3) + '000000000'
            WHERE
              E1.statecode = 0
              AND E1.new_autoagent = '{autoagentid_guid}'
              AND (
                CASE {rank_system}
                  WHEN 100 THEN E1.new_rank_priority_a
                  WHEN 200 THEN E1.new_rank_priority_b
                  WHEN 300 THEN E1.new_rank_priority_c
                  WHEN 400 THEN E1.new_rank_priority_d
                  ELSE E1.new_rank_priority_none
                END
              ) = 1
            ORDER BY
              E2.new_addresscode DESC
            """

        return pd.read_sql_query(sql, con=self.__conn)

    def _select_agent(self, unit_guid: str, agent_window_unit_guid: str, new_unit_to_list: list, sysdate_utc_dict: dict, rank_system: int) -> pd.DataFrame:
        '''
        差配先情報を取得する。
        '''

        self.logger.debug('[差配先取得]')
        self.logger.debug('自動差配元ユニット(GUID)：%s, 差配担当窓口ユニット(GUID)：%s, ランク(システム)：%s', unit_guid, agent_window_unit_guid, rank_system)
        self.logger.debug('差配先ユニット一覧：%s', new_unit_to_list)
        self.logger.debug('システム日時(UTC)：%s', sysdate_utc_dict)

        new_unit_to_str = '\'' + '\', \''.join(map(str, new_unit_to_list)) + '\''

        sql = f"""
            SELECT
              -- 総合会社の部署E
              BS.businessunitid AS agent_window_comprehensivecompany_guid,
              -- 部の部署E
              BB.businessunitid AS agent_window_division_guid,
              -- 部門の部署E
              BBM.businessunitid AS agent_window_section_guid,
              -- 通常差配E
              E1.new_unit AS agent_window_unit_guid,
              CASE {rank_system}
                WHEN 100 THEN (COALESCE(T2.agent_amount, 0) + 1) / E1.new_agentrate_instant_a
                ELSE (COALESCE(T2.agent_amount, 0) + 1) / E1.new_agentrate_instant
              END AS agent_priority_value,
              COALESCE(T2.agent_amount, 0) AS agent_amount, -- CSV出力用項目
              CASE {rank_system}
                WHEN 100 THEN E1.new_agentrate_instant_a
                ELSE E1.new_agentrate_instant
              END AS agentrate_instant                      -- CSV出力用項目
            FROM
              -- 通常差配設定E
              NTTEAST_MSCRM.dbo.new_autoagent_normal_agent E1
              LEFT OUTER JOIN
              (
                SELECT
                  COUNT(*) AS agent_amount,
                  COMMISSIONHISTORY_GROUPCOUNT_TBL.new_unit_to
                FROM
                  (
                    SELECT
                      E2.new_commission,
                      E2.new_unit_from,
                      E2.new_unit_to
                    FROM
                      -- 取次履歴E
                      NTTEAST_MSCRM.dbo.new_commissionhistory E2
                      -- 取次E
                      INNER JOIN NTTEAST_MSCRM.dbo.new_commission E3
                        ON E3.new_commissionid = E2.new_commission
                        AND (
                             (E3.new_rank IS NOT NULL AND E3.new_rank = {rank_system})
                          OR (E3.new_rank IS NULL AND E3.new_rank_system = {rank_system})
                        )
                    WHERE
                      E2.statecode = 0
                      AND E2.createdon >=
                        CASE {rank_system}
                          WHEN 100 THEN
                            CASE
                              WHEN CONVERT(DATETIME, '{sysdate_utc_dict['sysdate_utc']}') >= CONVERT(DATETIME, '{sysdate_utc_dict['current_month_last_day_1500_utc']}')
                              THEN
                                CONVERT(DATETIME, '{sysdate_utc_dict['current_month_last_day_1500_utc']}')
                              ELSE
                                CONVERT(DATETIME, '{sysdate_utc_dict['previous_month_last_day_1500_utc']}')
                            END
                          ELSE
                            CASE
                              WHEN CONVERT(DATETIME, '{sysdate_utc_dict['sysdate_utc']}') >= CONVERT(DATETIME, '{sysdate_utc_dict['current_day_1500_utc']}')
                              THEN
                                CONVERT(DATETIME, '{sysdate_utc_dict['current_day_1500_utc']}')
                              ELSE
                                CONVERT(DATETIME, '{sysdate_utc_dict['previous_day_1500_utc']}')
                            END
                        END
                      AND E2.new_unit_from IN ('{unit_guid}', '{agent_window_unit_guid}')
                      AND E2.new_unit_to IN ({new_unit_to_str})
                    GROUP BY
                      E2.new_commission,
                      E2.new_unit_from,
                      E2.new_unit_to
                  ) AS COMMISSIONHISTORY_GROUPCOUNT_TBL
                GROUP BY
                  COMMISSIONHISTORY_GROUPCOUNT_TBL.new_unit_to
              ) T2
                ON T2.new_unit_to = E1.new_unit
              -- 部署E
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BU
                ON BU.businessunitid = E1.new_unit
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BBM
                ON BBM.new_busho_code = LEFT(BU.new_busho_code, 9) + '000'
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BB
                ON BB.new_busho_code = LEFT(BU.new_busho_code, 6) + '000000'
              INNER JOIN NTTEAST_MSCRM.dbo.businessunit BS
                ON BS.new_busho_code = LEFT(BU.new_busho_code, 3) + '000000000'
            WHERE
              E1.new_unit IN ({new_unit_to_str})
              AND (
                CASE {rank_system}
                  WHEN 100 THEN E1.new_agentrate_instant_a
                  ELSE E1.new_agentrate_instant
                END
              ) > 0
            ORDER BY
              agent_priority_value ASC
            """

        return pd.read_sql_query(sql, con=self.__conn)

    def _select_third_person_application_agent_unit(self, autoagentid_guid: str, rank_system: int) -> pd.DataFrame:
        '''
        第三者申込差配対象ユニット一覧情報を取得する。
        '''

        self.logger.debug('[第三者申込差配先ユニット一覧取得]')
        self.logger.debug('自動差配設定(GUID)：%s, ランク(システム)：%s', autoagentid_guid, rank_system)

        sql = f"""
            SELECT
              E1.new_unit AS new_unit_to
            FROM
              -- 通常差配設定E
              NTTEAST_MSCRM.dbo.new_autoagent_normal_agent E1
            WHERE
              E1.statecode = 0
              AND E1.new_autoagent = '{autoagentid_guid}'
              AND E1.new_third_person_application = 1
              AND (
                CASE {rank_system}
                  WHEN 100 THEN E1.new_agentrate_instant_a
                  ELSE E1.new_agentrate_instant
                END
              ) > 0
            """

        return pd.read_sql_query(sql, con=self.__conn)

    def _select_colab_line_agent_unit(self, autoagentid_guid: str, rank_system: int) -> pd.DataFrame:
        '''
        コラボ回線差配対象ユニット一覧情報を取得する。
        '''

        self.logger.debug('[コラボ回線差配先ユニット一覧取得]')
        self.logger.debug('自動差配設定(GUID)：%s, ランク(システム)：%s', autoagentid_guid, rank_system)

        sql = f"""
            SELECT
              E1.new_unit AS new_unit_to
            FROM
              -- 通常差配設定E
              NTTEAST_MSCRM.dbo.new_autoagent_normal_agent E1
            WHERE
              E1.statecode = 0
              AND E1.new_autoagent = '{autoagentid_guid}'
              AND E1.new_colab_line = 1
              AND (
                CASE {rank_system}
                  WHEN 100 THEN E1.new_agentrate_instant_a
                  ELSE E1.new_agentrate_instant
                END
              ) > 0
            """

        return pd.read_sql_query(sql, con=self.__conn)

    def _select_rank_agent_unit(self, autoagentid_guid: str, rank_system: int) -> pd.DataFrame:
        '''
        ランクによる取次差配対象ユニット一覧情報を取得する。
        '''

        self.logger.debug('[ランクによる取次差配先ユニット一覧取得]')
        self.logger.debug('自動差配設定(GUID)：%s, ランク(システム)：%s', autoagentid_guid, rank_system)

        sql = f"""
            SELECT
              E1.new_unit AS new_unit_to
            FROM
              -- 通常差配設定E
              NTTEAST_MSCRM.dbo.new_autoagent_normal_agent E1
            WHERE 
              E1.statecode = 0
              AND E1.new_autoagent = '{autoagentid_guid}'
              AND (
                CASE {rank_system}
                  WHEN 100 THEN E1.new_agentrate_instant_a
                  ELSE E1.new_agentrate_instant
                END
              ) > 0
              AND (
                CASE {rank_system}
                  WHEN 100 THEN E1.new_rank_normal_a
                  WHEN 200 THEN E1.new_rank_normal_b
                  WHEN 300 THEN E1.new_rank_normal_c
                  WHEN 400 THEN E1.new_rank_normal_d
                  ELSE E1.new_rank_normal_none
                END
              ) = 1
            """

        return pd.read_sql_query(sql, con=self.__conn)

    def _select_sysdate_utc(self) -> pd.DataFrame:
        '''
        通常差配取得の際に使用する日付情報を取得する。
        '''

        self.logger.debug('[通常差配用日付情報取得]')

        sql = """
            SELECT
              CONVERT(DATETIME, CONVERT(VARCHAR, GETUTCDATE(), 20)) AS sysdate_utc,
              DATEADD(HOUR, 15, DATEADD(DAY, -1, LEFT(CONVERT(VARCHAR, GETUTCDATE(), 111), 8) + '01')) AS previous_month_last_day_1500_utc,
              DATEADD(HOUR, 15, DATEADD(DAY, -1, LEFT(CONVERT(VARCHAR, DATEADD(MONTH, 1, GETUTCDATE()), 111), 8) + '01')) AS current_month_last_day_1500_utc,
              DATEADD(HOUR, 15, DATEADD(DAY, -1, LEFT(CONVERT(VARCHAR, GETUTCDATE(), 111), 10))) AS previous_day_1500_utc,
              DATEADD(HOUR, 15, LEFT(CONVERT(VARCHAR, GETUTCDATE(), 111), 10)) AS current_day_1500_utc
            """

        return pd.read_sql_query(sql, con=self.__conn)

    def _insert_opportunity(self, row: dict, teamid: str, today_str: str) -> bool:
        '''
        提案プロジェクトEを登録する。
        '''

        self.logger.debug('[提案プロジェクトE登録]')

        contractorname = row['contractorname']
        commissionid_guid = row['commissionid_guid']
        self.logger.debug('契約者名：%s, 取次(GUID)：%s', contractorname, commissionid_guid)
        self.logger.debug('チーム：%s, システム日付：%s', teamid, today_str)

        try:
            contractorname = '' if pd.isna(contractorname) else contractorname
            name = C7013_06_task.OPPORTUNITY_NAME_PREFIX + today_str + '_' + contractorname
            if C7013_06_task.OPPORTUNITY_NAME_LENGTH_MAX < len(name):
                name = name[0:C7013_06_task.OPPORTUNITY_NAME_LENGTH_MAX]
            self.logger.debug('提案PJ名：%s', name)

            entity = Entity('opportunity')
            entity.Attributes['ownerid'] = EntityReference('team', teamid)
            entity.Attributes['name'] = name
            entity.Attributes['new_activity_condition'] = OptionSetValue(C7013_06_task.ACTIVITY_CONDITION_ACTIVE_THISYEAR)
            entity.Attributes['new_commodity_paper_flg'] = OptionSetValue(C7013_06_task.COMMODITY_PAPER_FLG_UNNECESSARY)
            entity.Attributes['new_commission'] = EntityReference('new_commission', commissionid_guid)

            self.__helper.CreateEntity(entity)

        except Exception:
            self.logger.error(message.MSG['MSG2008'], commissionid_guid)
            self.__error_list.append(row)
            return False

        return True

    def _update_commission(self, row: dict, agent_to_dict: dict, new_bcc_status_commission: int, new_bcc_unsupported_reason: int, new_agent_category: int, notetext: str) -> bool:
        '''
        取次Eを更新する。
        メモEを登録する。
        '''

        self.logger.debug('[取次E更新]')

        commissionid_guid = row['commissionid_guid']
        comprehensivecompany_guid = row['comprehensivecompany_guid']
        division_guid = row['division_guid']
        section_guid = row['section_guid']
        unit_guid = row['unit_guid']
        rank_system = row['rank_system']
        accountperson_incharge_name = row['accountperson_incharge_name']
        policy_keywords = row['policy_keywords']

        agent_window_comprehensivecompany_guid = agent_to_dict['agent_window_comprehensivecompany_guid']
        agent_window_division_guid = agent_to_dict['agent_window_division_guid']
        agent_window_section_guid = agent_to_dict['agent_window_section_guid']
        agent_window_unit_guid = agent_to_dict['agent_window_unit_guid']

        self.logger.debug('取次(GUID)：%s', commissionid_guid)
        self.logger.debug('差配先：%s, %s, %s, %s', agent_window_comprehensivecompany_guid, agent_window_division_guid, agent_window_section_guid, agent_window_unit_guid)
        self.logger.debug('依頼元：%s, %s, %s, %s', comprehensivecompany_guid, division_guid, section_guid, unit_guid)
        self.logger.debug('BCC取次状況：%s, BCC未対応取次理由：%s, 差配種類：%s', new_bcc_status_commission, new_bcc_unsupported_reason, new_agent_category)
        self.logger.debug('ランク(システム)：%s, アカウント名：%s, 施策キーワード：%s', rank_system, accountperson_incharge_name, policy_keywords)

        # 取次E更新
        try:
            entity = Entity('new_commission')
            entity.Id = commissionid_guid
            entity.Attributes['new_comprehensivecompany_to'] = EntityReference(entity.LogicalName, agent_window_comprehensivecompany_guid)
            entity.Attributes['new_division_to'] = EntityReference(entity.LogicalName, agent_window_division_guid)
            entity.Attributes['new_section_to'] = EntityReference(entity.LogicalName, agent_window_section_guid)
            entity.Attributes['new_unit_to'] = EntityReference(entity.LogicalName, agent_window_unit_guid)
            entity.Attributes['new_personincharge_to'] = None
            entity.Attributes['new_comprehensivecompany_from'] = EntityReference(entity.LogicalName, comprehensivecompany_guid)
            entity.Attributes['new_division_from'] = EntityReference(entity.LogicalName, division_guid)
            entity.Attributes['new_section_from'] = EntityReference(entity.LogicalName, section_guid)
            entity.Attributes['new_unit_from'] = EntityReference(entity.LogicalName, unit_guid)
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
            entity.Attributes['new_status_commission'] = OptionSetValue(C7013_06_task.STATUS_COMMISSION_SELECTION)
            entity.Attributes['new_bcc_status_commission'] = None if pd.isna(new_bcc_status_commission) else OptionSetValue(new_bcc_status_commission)
            entity.Attributes['new_bcc_unsupported_reason'] = None if pd.isna(new_bcc_unsupported_reason) else OptionSetValue(new_bcc_unsupported_reason)
            entity.Attributes['new_agent_category'] = None if pd.isna(new_agent_category) else OptionSetValue(new_agent_category)
            entity.Attributes['new_rank_system'] = None if pd.isna(rank_system) else OptionSetValue(rank_system)
            entity.Attributes['new_account_person'] = None if pd.isna(accountperson_incharge_name) else accountperson_incharge_name
            entity.Attributes['new_autoagent_policy_keyword'] = None if pd.isna(policy_keywords) else policy_keywords

            self.__helper.UpdateEntity(entity)

        except Exception:
            self.logger.error(message.MSG['MSG2003'], commissionid_guid)
            self.__error_list.append(row)
            return False

        # メモE登録
        if notetext:
            self.logger.debug('[メモE登録]')
            self.logger.debug('オブジェクトID：%s, メモ：%s', commissionid_guid, notetext)

            try:
                entity = Entity('annotation')
                entity.Attributes['objectid'] = EntityReference('new_commission', commissionid_guid)
                entity.Attributes['notetext'] = notetext

                self.__helper.CreateEntity(entity)

            except Exception:
                self.logger.error(message.MSG['MSG2004'], commissionid_guid, notetext)
                self.__error_list.append(row)
                return False

        return True

    def _set_commission_data(self, row_num: int, agent_category: int, agent_to_dict: dict, agent_amount_list: list, agent_object_unit_guid_list: list, new_agentrate_instant_list: list, is_created_opportunity: str, annotation_message: str):
        '''
        取次保存情報を設定する。
        '''

        self.logger.debug('[取次保存情報設定]')

        index = self.__output_data.index[row_num]

        agent_window_comprehensivecompany_guid = agent_to_dict['agent_window_comprehensivecompany_guid']
        agent_window_division_guid = agent_to_dict['agent_window_division_guid']
        agent_window_section_guid = agent_to_dict['agent_window_section_guid']
        agent_window_unit_guid = agent_to_dict['agent_window_unit_guid']

        agent_amount_str = ':'.join(map(str, agent_amount_list))
        agent_object_unit_guid_str = ':'.join(map(str, agent_object_unit_guid_list))
        new_agentrate_instant_str = ':'.join(map(str, new_agentrate_instant_list))

        self.logger.debug('行名：%s', index)
        self.logger.debug('差配種類：%s', agent_category)
        self.logger.debug('差配先：%s, %s, %s, %s', agent_window_comprehensivecompany_guid, agent_window_division_guid, agent_window_section_guid, agent_window_unit_guid)
        self.logger.debug('差配数一覧：%s', agent_amount_str)
        self.logger.debug('差配対象ユニット一覧(GUID)：%s', agent_object_unit_guid_str)
        self.logger.debug('差配比率一覧：%s', new_agentrate_instant_str)
        self.logger.debug('提案プロジェクトEの作成有無：%s', is_created_opportunity)
        self.logger.debug('メモEへのメッセージ：%s', annotation_message)

        # 差配種類
        self.__output_data.at[index, 'agent_category'] = '' if pd.isna(agent_category) else str(agent_category)
        # 差配先総合会社(GUID)
        self.__output_data.at[index, 'to_agent_comprehensivecompany_guid'] = agent_to_dict['agent_window_comprehensivecompany_guid']
        # 差配先部(GUID)
        self.__output_data.at[index, 'to_agent_division_guid'] = agent_to_dict['agent_window_division_guid']
        # 差配先部門(GUID)
        self.__output_data.at[index, 'to_agent_section_guid'] = agent_to_dict['agent_window_section_guid']
        # 差配先ユニット(GUID)
        self.__output_data.at[index, 'to_agent_unit_guid'] = agent_to_dict['agent_window_unit_guid']
        # 差配数一覧
        self.__output_data.at[index, 'agent_amount'] = agent_amount_str
        # 差配対象ユニット一覧(GUID)
        self.__output_data.at[index, 'agent_object_unit_guid_list'] = agent_object_unit_guid_str
        # 差配比率一覧
        self.__output_data.at[index, 'new_agentrate_instant_list'] = new_agentrate_instant_str
        # 提案プロジェクトEの作成有無
        self.__output_data.at[index, 'is_created_opportunity'] = '' if pd.isna(is_created_opportunity) else is_created_opportunity
        # メモEへのメッセージ
        self.__output_data.at[index, 'annotation_message'] = '' if pd.isna(annotation_message) else annotation_message
