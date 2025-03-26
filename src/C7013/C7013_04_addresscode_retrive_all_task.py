# 標準ライブラリインポート
import pathlib

# サードパーティライブラリインポート
import inject

# プロジェクトライブラリインポート
from . import const
from . import message
from .task import BaseTask
from .dao.crmdb_dao import CrmDBDao

class C7013_04_addresscode_retrive_all_task(BaseTask):
    '''
    全ての住所コードを取得する
    '''

    @inject.autoparams()
    def __init__(self, dao: CrmDBDao):
        '''
        初期化関数

        Args:
            dao: CrmDBDao
        '''
        self._dao: CrmDBDao = dao
        # 親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self, output_file_path: pathlib.PurePath) -> int:
        '''
        全ての住所コードを取得してデータフレーム形式で返却します

        Args:
            output_file_path: 住所コード出力ファイルのパス

        Returns:
            タスク実行結果（0:正常、1:異常、2:警告）
        '''
        self.logger.info(f'タスクを実行します。')

        addresscodes = self._dao.retrive_all_addresscode()
        addresscodes.to_csv(output_file_path, sep=',', encoding='utf-8', index=False)
        # 出力ファイルを書込みました。ファイル名＝{0}
        self.logger.info(message.MSG['MSG0011'], output_file_path)
        return const.BATCH_SUCCESS
