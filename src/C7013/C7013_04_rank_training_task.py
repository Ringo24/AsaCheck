# 標準ライブラリインポート
import pathlib

# サードパーティライブラリインポート
import inject

# プロジェクトライブラリインポート
from . import const
from .task import BaseTask
from .rank_model_helper import RankModelHelper

class C7013_04_rank_training_task(BaseTask):
    '''
    ランクモデル学習タスククラス
    '''

    @inject.autoparams()
    def __init__(self):
        '''
        初期化関数
        '''

        # 親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self, training_file_path: pathlib.PurePath, validation_file_path: pathlib.PurePath, save_path: pathlib.PurePath = None) -> int:
        '''
            ランクモデルを構築し、学習する。

        Args:
            training_file_path: 学習ファイルのパス
            validation_file_path: 検証ファイルのパス
            save_path: 保存フォルダ、指定しない場合はデータフォルダに保存します。

        Returns:
            タスク実行結果（0:正常、1:異常、2:警告）
        '''
        self.logger.info(f'タスクを実行します。')

        # ランク判定用モデルを構築する
        rank_model_helper: RankModelHelper = RankModelHelper()
        rank_model_helper.assembly_model()
        
        # ランク判定用モデルを学習する
        epochs = const.APP_CONFIG['rank_config']['epochs']
        batch_size = const.APP_CONFIG['rank_config']['batch_size']
        workers = const.APP_CONFIG['rank_config']['workers']

        self.logger.debug(f'Training Rank Model batch_size={batch_size}, epochs={epochs}, workers={workers}')

        rank_model_helper.fit_model_from_file(training_file_path, validation_file_path, epochs=epochs, batch_size=batch_size, workers=workers)

        # ランク判定用モデルを保存する
        rank_model_helper.save_models(save_path)
        # 学習結果をグラフに出力する
        rank_model_helper.save_training_accuracies_and_losses()
        rank_model_helper.save_training_and_validation_losses()

        return const.BATCH_SUCCESS