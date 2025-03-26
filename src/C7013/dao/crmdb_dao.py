# 標準ライブラリインポート

# サードパーティライブラリインポート
import pymssql
import inject
import numpy as np
import pandas as pd

# プロジェクトライブラリインポート
from ..utils import get_crmdb_connection
from .dao import BaseDao
from ..dto.RGLT_INFO import RGLT_INFO

class CrmDBDao(BaseDao):
    '''
    CRMDBのData Access Object
    DB情報：Microsoft SQL Server 2008 R2 (SP3) - 10.50.6000.34 (X64)   Aug 19 2014 12:21:34
    '''

    @inject.autoparams()
    def __init__(self):
        '''
        '''
        self._conn: pymssql.Connection = None

    def conn(self) -> pymssql.Connection:
        """
        データベースに接続する
        """
        if self._conn == None:
            self._conn = get_crmdb_connection()
        return self._conn

    def cursor(self, as_dict=False):
        '''
        新しいカーソル生成する
        '''
        return self.conn().cursor(as_dict)

    def commit(self) -> None:
        '''
        トランザクションをCommitする
        '''
        self.conn().commit()

    def close(self) -> None:
        '''
        データベースを閉じる
        '''
        if self._conn != None:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        self.conn()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def try_get_locked_rglt_info(self, rglt_apid:str, rec_no:str) -> RGLT_INFO:
        '''
        規制情報を取得する

        Args:
            rglt_apid: 機能ID
            rec_no: レコード番号

        Returns:
            規制情報Dto
        '''

        with self.cursor(as_dict=True) as cursor:
            cursor.execute('UPDATE CrmCustomDB.dbo.RGLT_INFO SET SPR1=\'1\' WHERE RGLT_APID=%s AND REC_NO=%s AND SPR1=\'0\'', (rglt_apid, rec_no))
            if cursor.rowcount != 1:
                return None

            cursor.execute('SELECT * FROM CrmCustomDB.dbo.RGLT_INFO WHERE RGLT_APID=%s AND REC_NO=%s AND SPR1=\'1\'', (rglt_apid, rec_no))
            for row in cursor:
                rglt_info = RGLT_INFO()
                rglt_info.RGLT_APID = row['RGLT_APID']
                rglt_info.REC_NO = row['REC_NO']
                rglt_info.RGLT_KIKAN_STRT = row['RGLT_KIKAN_STRT']
                rglt_info.RGLT_KIKAN_END = row['RGLT_KIKAN_END']
                rglt_info.RGLT_TIME_STRT = row['RGLT_TIME_STRT']
                rglt_info.RGLT_TIME_END = row['RGLT_TIME_END']
                rglt_info.SPR1 = row['SPR1']
                rglt_info.SPR2 = row['SPR2']
                rglt_info.SPR3 = row['SPR3']
                rglt_info.SPR4 = row['SPR4']
                rglt_info.SPR5 = row['SPR5']
                rglt_info.SPR6 = row['SPR6']
                rglt_info.SPR7 = row['SPR7']
                rglt_info.SPR8 = row['SPR8']
                rglt_info.SPR9 = row['SPR9']
                rglt_info.SPR10 = row['SPR10']
                return rglt_info

    def update_rglt_info(self, rglt_info:RGLT_INFO) -> int:
        '''
        規制情報を更新する

        Args:
            rglt_info: 規制情報Dto

        Returns:
            更新できた場合1、できなかった場合0を返却する
        '''
        with self.cursor(as_dict=True) as cursor:
            cursor.execute('UPDATE CrmCustomDB.dbo.RGLT_INFO SET SPR1=%s,SPR2=%s,SPR3=%s,SPR4=%s,SPR5=%s,SPR6=%s,SPR7=%s,SPR8=%s,SPR9=%s,SPR10=%s WHERE RGLT_APID=%s AND REC_NO=%s', (rglt_info.SPR1, rglt_info.SPR2, rglt_info.SPR3, rglt_info.SPR4, rglt_info.SPR5, rglt_info.SPR6, rglt_info.SPR7, rglt_info.SPR8, rglt_info.SPR9, rglt_info.SPR10, rglt_info.RGLT_APID, rglt_info.REC_NO))
            return cursor.rowcount

    def retrive_all_addresscode(self) -> pd.DataFrame:
        '''
        全ての住所コードを取得する

        Returns:
            住所コードDataFrame
        '''

        sql = """
SELECT
     t1.new_new_addressname                  AS addr_cd       --住所コード
    ,SUBSTRING(t1.new_new_addressname, 1, 2) AS tdfkn_cd      --都道府県コード
    ,SUBSTRING(t1.new_new_addressname, 3, 3) AS scyosn_cd     --市区町村コード
    ,SUBSTRING(t1.new_new_addressname, 6, 3) AS oaza_tshum_cd --大字通称コード
    ,SUBSTRING(t1.new_new_addressname, 9, 3) AS azchm_cd      --字丁目コード
    ,t1.new_addresscode                      AS addr_nm       --住所
    ,t1.new_prefecturename                   AS tdfkn_nm      --都道府県
    ,t1.new_municipalityname                 AS scyosn_nm     --市区町村
    ,t1.new_largersectionalias               AS oaza_tshum_nm --大字通称
    ,t1.new_sectioncityblock                 AS azchm_nm      --字丁目
    ,t1.new_zipcode                          AS zip_cd        --郵便番号
FROM NTTEAST_MSCRM.dbo.new_addresscode t1 WITH(NOLOCK)
WHERE t1.new_new_addressname <> 'JTD00000000' --（未登録）
AND LEN(t1.new_new_addressname) = 11
ORDER BY t1.new_new_addressname ASC
            """

        return pd.read_sql_query(sql, con=self.conn())

    def retrive_new_autoagent_policy_keyword(self, new_autoagent) -> pd.DataFrame:
        '''
        施策キーワードを取得する

        Args:
            new_autoagent: 自動差配設定GUID

        '''

        sql = f"""
SELECT
     t1.new_autoagent_policy_keyword         AS new_autoagent_policy_keyword --キーワード
    ,t1.new_policy_word                      AS new_policy_word              --登録値
FROM NTTEAST_MSCRM.dbo.new_autoagent_policy_keyword t1 WITH(NOLOCK)
WHERE t1.new_autoagent = '{new_autoagent}'
AND t1.statecode = 0
ORDER BY t1.new_policy_word ASC
            """
        return pd.read_sql_query(sql, con=self.conn())

    def retrive_accountperson_incharge_from_telephonenumber(self, telephonenumber):
        '''
        電話番号を利用しアカウント担当者を取得する

        Args:
            telephonenumber: 電話番号

        '''

        sql = f"""
SELECT
     t2.new_accountpersonincharge        AS accountpersonincharge_guid
    ,t2.new_accountpersoninchargeName    AS accountperson_incharge_name
FROM NTTEAST_MSCRM.dbo.new_telephonenumber T1 WITH(NOLOCK)
INNER JOIN NTTEAST_MSCRM.dbo.Account T2 WITH(NOLOCK) ON
        T1.new_account = T2.AccountId
WHERE
        T1.new_telephonenumberhynophenate = REPLACE('{telephonenumber}', '-', '')
    AND T1.statecode = 0
    AND T2.StateCode = 0
            """
        return pd.read_sql_query(sql, con=self.conn())

    def retrive_accountperson_incharge_from_account(self, customerid, accountaddresscode):
        '''
        顧客事業所からアカウント担当者を取得する

        Args:
            customerid: 企業ID
            accountaddresscode: 住所コード

        '''

        sql = f"""
SELECT
     t1.new_accountpersonincharge        AS accountpersonincharge_guid
    ,t1.new_accountpersoninchargeName    AS accountperson_incharge_name
FROM NTTEAST_MSCRM.dbo.Account T1 WITH(NOLOCK)
WHERE
    T1.new_customerid_accountaddresscode = '{customerid}{accountaddresscode}'
            """
        return pd.read_sql_query(sql, con=self.conn())

    def retrive_new_autoagent_specific_vendor(self, new_autoagent):
        '''
        特定ベンダから自動差配設定と特定ベンダ名を取得する

        Args:
            new_autoagent: 自動差配設定GUID
        '''

        sql = f"""
SELECT
     t1.new_autoagent
    ,t1.new_autoagent_specific_vendor
FROM NTTEAST_MSCRM.dbo.new_autoagent_specific_vendor T1 WITH(NOLOCK)
WHERE
    T1.new_autoagent = '{new_autoagent}'
AND T1.statecode = 0
            """
        return pd.read_sql_query(sql, con=self.conn())

    def retrive_new_commission_from_new_commissionid(self, new_commissionid):
        '''
        取次からランクと取次(GUID)を取得する

        Args:
            new_commissionid: 取次(GUID)
        '''
        sql = f"""
SELECT
     t1.new_commissionId as commissionid_guid
    ,t1.new_rank as rank
FROM NTTEAST_MSCRM.dbo.new_commission T1 WITH(NOLOCK)
WHERE
    T1.new_commissionId = '{new_commissionid}'
            """
        return pd.read_sql_query(sql, con=self.conn())
