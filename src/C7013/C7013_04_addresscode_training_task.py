# 標準ライブラリインポート
import pathlib

# サードパーティライブラリインポート
import inject

# プロジェクトライブラリインポート
from . import const
from .task import BaseTask
from .onehot_utils import AddresscodeOneHotEncoder
from .scyosn_cd_model_helper import ScyosnCdModelHelper
from .oaza_tshum_cd_model_helper import OazaTshumCdModelHelper
from .azchm_cd_model_helper import AzchmCdModelHelper
from .addresscode_utils import JapaneseSentenceVectorizer

class C7013_04_addresscode_training_task(BaseTask):
    '''
    市区町村コード変換モデル、大字通称＋字丁目コード変換モデルを学習する
    '''

    @inject.autoparams()
    def __init__(self):
        '''
        初期化関数
        '''
        # 親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self, training_option: str, training_file_path: pathlib.PurePath, validation_file_path: pathlib.PurePath, save_path: pathlib.PurePath = None) -> int:
        '''
            市区町村コード変換モデル、大字通称コード変換モデル、字丁目コード変換モデルを構築し、学習する。

        Args:
            training_option: 学習オプション
            training_file_path: 学習ファイルのパス
            validation_file_path: 検証ファイルのパス
            save_path: 保存フォルダ、指定しない場合はデータフォルダに保存します。

        Returns:
            タスク実行結果（0:正常、1:異常、2:警告）
        '''
        self.logger.info(f'タスクを実行します。')

        if not training_option:
            training_option = "all"

        one_hot_encoder = AddresscodeOneHotEncoder()
        one_hot_encoder.load_all()

        vectorizer = JapaneseSentenceVectorizer.load_from_file()
        vectorizer.max_tokens = const.APP_CONFIG['addresscode_config']['max_vocab_size']
        vectorizer.output_sequence_length = const.APP_CONFIG['addresscode_config']['max_sequence_length']

        train_file_path = training_file_path
        val_file_path = validation_file_path
        batch_size = const.APP_CONFIG['addresscode_config']['batch_size']
        epochs = const.APP_CONFIG['addresscode_config']['epochs']
        workers = const.APP_CONFIG['addresscode_config']['workers']

        self.logger.debug(f'Training AddressCode Convert Model training_option={training_option}, batch_size={batch_size}, epochs={epochs}, workers={workers}')

        # 市区町村コード変換モデル
        if training_option in ("all", "scyosn_cd"):
            scyosn_cd_helper = ScyosnCdModelHelper(one_hot_encoder, vectorizer)
            scyosn_cd_helper.assembly_model()
            scyosn_cd_helper.fit_model_from_file(train_file_path, val_file_path, batch_size=batch_size, epochs=epochs, workers=workers)
            scyosn_cd_helper.save_model(save_path)
            scyosn_cd_helper.save_training_accuracy_and_loss()
            scyosn_cd_helper.save_training_and_validation_loss()

        # 大字通称コード変換モデル
        if training_option in ("all", "oaza_tshum_cd"):
            oaza_tshum_cd_helper = OazaTshumCdModelHelper(one_hot_encoder, vectorizer)
            oaza_tshum_cd_helper.assembly_model()
            oaza_tshum_cd_helper.fit_model_from_file(train_file_path, val_file_path, batch_size=batch_size, epochs=epochs, workers=workers)
            oaza_tshum_cd_helper.save_model(save_path)
            oaza_tshum_cd_helper.save_training_accuracy_and_loss()
            oaza_tshum_cd_helper.save_training_and_validation_loss()

        # 字丁目コード変換モデル
        if training_option in ("all", "azchm_cd"):
            azchm_cd_helper = AzchmCdModelHelper(one_hot_encoder, vectorizer)
            azchm_cd_helper.assembly_model()
            azchm_cd_helper.fit_model_from_file(train_file_path, val_file_path, batch_size=batch_size, epochs=epochs, workers=workers)
            azchm_cd_helper.save_model(save_path)
            azchm_cd_helper.save_training_accuracy_and_loss()
            azchm_cd_helper.save_training_and_validation_loss()

        return const.BATCH_SUCCESS

