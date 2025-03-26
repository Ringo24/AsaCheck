# 標準ライブラリインポート
import sqlite3

# サードパーティライブラリインポート
import inject
import numpy as np
import pandas as pd

# プロジェクトライブラリインポート
from ..utils import get_sqlite_connection
from .dao import BaseDao

class SqliteDao(BaseDao):
    '''
    LocalDBのData Access Object
    DB情報：Sqlite3
    '''

    @inject.autoparams()
    def __init__(self):
        '''
        '''
        self._conn: sqlite3.Connection = None
        super().__init__()

    def conn(self) -> sqlite3.Connection:
        '''
        データベースに接続する
        '''
        if self._conn == None:
            self._conn = get_sqlite_connection()
        return self._conn

    def cursor(self):
        '''
        新しいカーソル生成する
        '''
        return self.conn().cursor()

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

    def delete_custom_table_by_commissionid_guid(self, df) -> None:
        '''
        取次(GUID)をキーにcustom_tableのレコードを削除する
            
        Args:
            df: 自動差配済情報DataFrame
        '''
        cursor = self.cursor()
        first_sql_flag = True
        sql = ""
        for index, row in df.iterrows():
            if first_sql_flag:
                first_sql_flag = False
                sql = f"""
DELETE FROM custom_table WHERE """
            else:
                sql += 'OR'
            sql += f"""
commissionid_guid = '{row.commissionid_guid}'
"""
            # 1000件ずつ処理する
            if (index + 1) % 1000 == 0:
                cursor.execute(sql)
                first_sql_flag = True

        if not first_sql_flag:
            cursor.execute(sql)

    def select_custom_table(self, jidou_sahai_rev, target_period_from, target_period_to) -> pd.DataFrame:
        '''
        自動差配リビジョン、対象期間(開始)、対象期間(終了)を検索キーにcustom_tableのレコードを取得する

        Args:
            jidou_sahai_rev: 自動差配リビジョン
            target_period_from: 対象期間(開始)
            target_period_to: 対象期間(終了)
        Returns:
            df:custom_tableのDataFrame
        '''
        cursor = self.cursor()

        sql = f"""
SELECT * FROM custom_table WHERE
update_date BETWEEN '{target_period_from}' AND '{target_period_to}'
"""
        if jidou_sahai_rev:
            sql += f"""
AND jidou_sahai_rev = '{jidou_sahai_rev}'
"""
        sql += f"""
ORDER BY jidou_sahai_rev ASC, update_date ASC        
"""

        return pd.read_sql_query(sql=sql,con=self.conn())

    def update_custom_table_rank(self, jidou_sahai_rev, commissionid_guid, rank) -> None:
        '''
        自動差配リビジョン、取次(GUID)、をキーにcustom_tableのrankを更新する

        Args:
            jidou_sahai_rev: 自動差配リビジョン
            commissionid_guid: 取次(GUID)
            rank: ランク
        '''
        cursor = self.cursor()

        sql = f"""
UPDATE custom_table SET rank = {rank if rank is not None else 'null'}
WHERE commissionid_guid = '{commissionid_guid}'
"""
        if jidou_sahai_rev:
            sql += f"""
AND jidou_sahai_rev = '{jidou_sahai_rev}'
"""
        cursor.execute(sql)

    def correct_answer_rate(self, jidou_sahai_rev, target_period_from, target_period_to) -> pd.DataFrame:
        '''
        正答率データを取得する

        Args:
            jidou_sahai_rev: 自動差配リビジョン
            target_period_from: 対象期間(開始)
            target_period_to: 対象期間(終了)
        Returns:
            df:データ蓄積_正答率データのDataFrame
        '''
        cursor = self.cursor()

        sql = f"""
SELECT
rank_answer,
SUM(A_rank_ticket) as rank_a_sys_amt,
SUM(B_rank_ticket) as rank_b_sys_amt,
SUM(C_rank_ticket) as rank_c_sys_amt,
SUM(D_rank_ticket) as rank_d_sys_amt,
SUM(Bar_rank_ticket) as rank_bar_sys_amt,
SUM(Non_rank_ticket) as rank_non_sys_amt,
CASE rank_answer
WHEN 'A' THEN ROUND( CAST(SUM(A_rank_ticket)AS REAL) / NULLIF( ( SUM(A_rank_ticket)+SUM(B_rank_ticket)+SUM(C_rank_ticket)+SUM(D_rank_ticket)+SUM(Bar_rank_ticket)  ),0  ) ,2 )
WHEN 'B' THEN ROUND( CAST(SUM(B_rank_ticket)AS REAL) / NULLIF( ( SUM(A_rank_ticket)+SUM(B_rank_ticket)+SUM(C_rank_ticket)+SUM(D_rank_ticket)+SUM(Bar_rank_ticket)  ),0  ) ,2 )
WHEN 'C' THEN ROUND( CAST(SUM(C_rank_ticket)AS REAL) / NULLIF( ( SUM(A_rank_ticket)+SUM(B_rank_ticket)+SUM(C_rank_ticket)+SUM(D_rank_ticket)+SUM(Bar_rank_ticket)  ),0  ) ,2 )
WHEN 'D' THEN ROUND( CAST(SUM(D_rank_ticket)AS REAL) / NULLIF( ( SUM(A_rank_ticket)+SUM(B_rank_ticket)+SUM(C_rank_ticket)+SUM(D_rank_ticket)+SUM(Bar_rank_ticket)  ),0  ) ,2 )
WHEN '－' THEN ROUND( CAST(SUM(Bar_rank_ticket)AS REAL) / NULLIF( ( SUM(A_rank_ticket)+SUM(B_rank_ticket)+SUM(C_rank_ticket)+SUM(D_rank_ticket)+SUM(Bar_rank_ticket)  ),0  ) ,2 )
ELSE null
END AS correct_ans_rate
FROM
(
SELECT
CASE IFNULL(rank, rank_system)
WHEN 100 THEN 'A'
WHEN 200 THEN 'B'
WHEN 300 THEN 'C'
WHEN 400 THEN 'D'
WHEN 500 THEN '－'
ELSE 'ランクなし'
END AS rank_answer,
CASE IFNULL(rank, rank_system)
WHEN 100 THEN 1
WHEN 200 THEN 2
WHEN 300 THEN 3
WHEN 400 THEN 4
WHEN 500 THEN 5
ELSE 6
END AS rank_order,
CASE rank_system
WHEN 100 THEN 1
ELSE 0
END AS A_rank_ticket,
CASE rank_system
WHEN 200 THEN 1
ELSE 0
END AS B_rank_ticket,
CASE rank_system
WHEN 300 THEN 1
ELSE 0
END AS C_rank_ticket,
CASE rank_system
WHEN 400 THEN 1
ELSE 0
END AS D_rank_ticket,
CASE rank_system
WHEN 500 THEN 1
ELSE 0
END AS Bar_rank_ticket,
CASE WHEN rank_system ISNULL
THEN 1
ELSE 0
END AS Non_rank_ticket
FROM custom_table
WHERE"""

        if jidou_sahai_rev:
            sql += f"""
jidou_sahai_rev = '{jidou_sahai_rev}'
AND"""
        sql += f"""
update_date BETWEEN '{target_period_from}' AND '{target_period_to}'
)
GROUP BY rank_answer
ORDER BY rank_order ASC"""

        return pd.read_sql_query(sql=sql,con=self.conn())

    def init_custom_tables(self) -> None:
        cursor = self.cursor()
        cursor.execute('''
create table if not exists custom_table(
    jidou_sahai_rev text,
    commissionid_guid text collate nocase,
    comprehensivecompany_guid text,
    division_guid text,
    section_guid text,
    unit_guid text,
    commissionclassification text,
    sourcecompany text,
    contractorname text,
    next_account text,
    third_person_application text,
    ordertelephonenumber text,
    contract_id text,
    colab_line text,
    contents_commission text,
    primarycorrespondenceway text,
    personincharge text,
    connectiontelephonenumber1 text,
    ordercontents text,
    autoagentid_guid text,
    agent_window_comprehensivecompany_guid text,
    agent_window_division_guid text,
    agent_window_section_guid text,
    agent_window_unit_guid text,
    next_account_code text,
    contractorname_cleansing text,
    contents_commission_cleansing text,
    sourcecompany_cleansing text,
    personincharge_cleansing text,
    rank_flag01 integer,
    rank_flag02 integer,
    rank_flag03 integer,
    rank_flag04 integer,
    rank_flag05 integer,
    rank_flag06 integer,
    rank_flag07 integer,
    rank_flag08 integer,
    rank_flag09 integer,
    rank_flag10 integer,
    rank_flag11 integer,
    rank_flag12 integer,
    rank_flag13 integer,
    rank_flag14 integer,
    rank_flag15 integer,
    rank_flag16 integer,
    rank_flag17 integer,
    rank_flag18 integer,
    rank_flag19 integer,
    rank_flag20 integer,
    rank_flag21 integer,
    rank_flag22 integer,
    rank_flag23 integer,
    rank_flag24 integer,
    rank_flag25 integer,
    rank_flag26 integer,
    rank_flag27 integer,
    rank_flag28 integer,
    rank_flag29 integer,
    rank_flag30 integer,
    rank_flag31 integer,
    rank_flag32 integer,
    rank_flag33 integer,
    rank_flag34 integer,
    rank_flag35 integer,
    rank_flag36 integer,
    rank_flag37 integer,
    rank_flag38 integer,
    rank_flag39 integer,
    rank_flag40 integer,
    rank_flag41 integer,
    rank_flag42 integer,
    rank_flag43 integer,
    rank_flag44 integer,
    rank_flag45 integer,
    rank_flag46 integer,
    rank_flag47 integer,
    rank_flag48 integer,
    rank_flag49 integer,
    rank_flag50 integer,
    rank_flag51 integer,
    rank_flag52 integer,
    rank_flag53 integer,
    rank_flag54 integer,
    rank_flag55 integer,
    rank_flag56 integer,
    rank_flag57 integer,
    rank_flag58 integer,
    rank_flag59 integer,
    rank_flag60 integer,
    rank_flag61 integer,
    rank_flag62 integer,
    rank_flag63 integer,
    rank_flag64 integer,
    rank_flag65 integer,
    rank_flag66 integer,
    rank_flag67 integer,
    rank_flag68 integer,
    rank_flag69 integer,
    rank_flag70 integer,
    rank_flag71 integer,
    rank integer default null,
    rank_system integer default null,
    accountperson_incharge_guid text,
    accountperson_incharge_name text,
    policy_keywords text,
    agent_category text,
    to_agent_comprehensivecompany_guid text,
    to_agent_division_guid text,
    to_agent_section_guid text,
    to_agent_unit_guid text,
    agent_amount text,
    agent_object_unit_guid_list text,
    new_agentrate_instant_list text,
    is_created_opportunity text,
    annotation_message text,
    update_date text,
    PRIMARY KEY(commissionid_guid)
)
''')
