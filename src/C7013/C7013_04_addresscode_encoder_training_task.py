# 標準ライブラリインポート
import pathlib

# サードパーティライブラリインポート
import pandas as pd
import inject

# プロジェクトライブラリインポート
from . import const
from . import message
from .task import BaseTask
from .onehot_utils import AddresscodeOneHotEncoder

class C7013_04_addresscode_encoder_training_task(BaseTask):
    '''
    住所コード変換用のOne-Hot Encoder及びWord2Vec用モデルを作成します。
    '''

    @inject.autoparams()
    def __init__(self):
        '''
        '''
        # 親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self, addresscode_file_path: pathlib.PurePath, dict_location_path: pathlib.PurePath = None, save_path: pathlib.PurePath = None) -> int:
        '''
        Args:
            addresscode_file_path: 住所コードファイルのパス
            dict_location_path: ユーザ辞書ファイルが格納されているパス
            save_path: 保存フォルダ、指定しない場合はデータフォルダに保存します。

        Returns:
            タスク実行結果（0:正常、1:異常、2:警告）
        '''
        self.logger.info(f'タスクを実行します。')

        one_hot_encoder = AddresscodeOneHotEncoder()
        one_hot_encoder.fit_all_from_file(addresscode_file_path)
        one_hot_encoder.save_all(save_path)

        self.logger.debug(f'住所コードCSVファイルを読み込みます。 {addresscode_file_path}')
        input_data = pd.read_csv(addresscode_file_path, sep=',', dtype=str, encoding='utf-8', usecols=['addr_nm'])
        input_data.dropna(inplace=True)

        if input_data.empty:
            # 入力ファイルにデータが存在しません。ファイル名＝{0}
            self.logger.error(message.MSG['MSG2010'], addresscode_file_path)
            return const.BATCH_ERROR

        return const.BATCH_SUCCESS
