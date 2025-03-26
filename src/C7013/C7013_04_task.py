# 標準ライブラリインポート

# サードパーティライブラリインポート
import pandas as pd
import inject

# プロジェクトライブラリインポート
from . import const
from .task import BaseTask
from .task import TaskResult
from .C7013_04_rank_flag_forced_correction_task import C7013_04_rank_flag_forced_correction_task
from .C7013_04_rank_prediction_task import C7013_04_rank_prediction_task


class C7013_04_task(BaseTask):
    '''
        ランク判定を行うタスククラス
    '''

    @inject.autoparams()
    def __init__(
        self,
        rank_flag_forced_correction_task: C7013_04_rank_flag_forced_correction_task,
        rank_prediction_task: C7013_04_rank_prediction_task
    ):
        '''
        初期化関数

        Args:
            rankflag_forced_correction_task: フラグ強制補正タスク
            rank_prediction_task: フラグ付与タスク
        '''
        self._rank_flag_forced_correction_task = rank_flag_forced_correction_task
        self._rank_prediction_task = rank_prediction_task

        # 親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self, input_data: pd.DataFrame) -> TaskResult:
        '''
        取次情報とランク判定用フラグを元にランク判定を行う。

        Args:
            input_data: 入力DataFrame
        Returns:
            ランク付与済みDataFrame
        '''
        self.logger.info(f'ランク判定タスクを実行します。')

        # 入力データが空の場合、カラムを追加して返却する
        if input_data.empty:
            if 'rank_system' not in input_data:
                input_data['rank_system'] = None
            return TaskResult(resultCode=const.BATCH_SUCCESS, resultData=input_data)

        output_data = input_data.copy()
        # フラグ強制補正
        input_data = self._rank_flag_forced_correction_task.execute(input_data)
        # ランク判定
        input_data = self._rank_prediction_task.execute(input_data)
        
        output_data['rank_system'] = input_data['rank_system']

        return TaskResult(resultCode=const.BATCH_SUCCESS, resultData=output_data)
