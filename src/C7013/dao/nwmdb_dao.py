# 標準ライブラリインポート

# サードパーティライブラリインポート
import cx_Oracle
import inject

# プロジェクトライブラリインポート
from ..utils import get_nwmdb_connection
from .dao import BaseDao

# エラー発生時の最大再検索回数
MAX_RETRY = 2

class NwmDBDao(BaseDao):
    '''
    NWMDBのData Access Object
    DB情報：Oracle Database 12c Enterprise Edition Release 12.1.0.2.0 - 64bit Production
    '''

    @inject.autoparams()
    def __init__(self):
        '''
        '''
        self._conn: cx_Oracle.Connection = None
        super().__init__()

    def conn(self) -> cx_Oracle.Connection:
        '''
        データベースに接続する
        '''
        if self._conn == None:
            self._conn = get_nwmdb_connection()
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

    def find_account_by_contract_id(self, contract_id) -> dict:
        '''
           リスト検索用テーブルから契約IDを元に企業ID、住所コードを取得する
           ※検索中エラーが発生した場合、２回まで再検索を行う。

        Args:
            contract_id: 契約ID

        Returns:
            企業IDと住所コードのDict、存在しない場合None
            CUST_ID: 企業ID
            SETLOC_ADDR_CD: 住所コード
        '''

        if not contract_id:
            return

        search_key = contract_id.replace(" ", "").replace("　", "")

        if len(search_key) <= 3:
            return
        if len(search_key) > 3 and len(search_key) < 16:
            search_key = search_key[0:3] + search_key[3:].rjust(13, '0')

        sql = f"""
SELECT CUST_ID, SETLOC_ADDR_CD
FROM LIST_SRCH_NO
WHERE SRCH_KEY_KBN = 3
AND NO_CLAS_CD IN (4, 5)
AND VARI_NO = '{search_key}'
"""
        for num in range(0, MAX_RETRY+1):
            try:
                with self.cursor() as cur:
                    for row in cur.execute(sql):
                        return {'CUST_ID': row[0], 'SETLOC_ADDR_CD': row[1]}
            except Exception as ex:
                self.logger.debug(f'{num+1}/{MAX_RETRY+1}：検索エラー・・・{"再検索します。" if num != MAX_RETRY else "処理を終了します。"}契約ID：{contract_id}')
                if num == MAX_RETRY:
                    raise Exception(ex) from ex

