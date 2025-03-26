# 標準ライブラリインポート
import os

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
import inject
import pathlib

# プロジェクトライブラリインポート
from . import const
from . import message
from .task import BaseTask
from .task import TaskResult
from . import onehot_utils
from . import rank_utils
from .rank_model_helper import RankModelHelper

class C7013_04_rank_prediction_task(BaseTask):
    '''
    ランクモデルを読み込み、ランクを付与します。
    '''

    @inject.autoparams()
    def __init__(self):
        '''
        初期化関数
        '''
        self.__rank_model_helper: RankModelHelper = None
        # 親クラスの初期化関数を呼び出す
        super().__init__()

    @property
    def _rank_model_helper(self) -> RankModelHelper:
        '''
        ランクモデルヘルパー
        '''

        if not self.__rank_model_helper:
            self.__rank_model_helper: RankModelHelper = RankModelHelper()
            self.__rank_model_helper.load_models()

        return self.__rank_model_helper

    def execute(self, input_data: pd.DataFrame) -> pd.DataFrame:
        '''
            ランク判定用モデルを読込、ランクを付与します。

        Args:
            input_data: 入力DataFrame
        Returns:
            ランク付与済みDataFrame
        '''
        self.logger.info(f'ランク付与タスクを実行します。')

        input_data_one_hot = rank_utils.input_data_transform(input_data)
        batch_size = const.APP_CONFIG['rank_config']['batch_size']
        result = self._rank_model_helper.predict(input_data_one_hot, batch_size)

        # 0:該当なしをNoneに置換する
        result = result.astype(np.object)
        result[result == 0] = None

        input_data['rank_system'] = result
        return input_data
