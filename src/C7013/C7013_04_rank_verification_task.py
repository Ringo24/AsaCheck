# 標準ライブラリインポート
import pathlib

# サードパーティライブラリインポート
import pandas as pd
import inject

# プロジェクトライブラリインポート
from . import const
from . import message
from .task import BaseTask
from . import rank_utils
from .rank_model_helper import RankModelHelper
from .C7013_04_rank_flag_forced_correction_task import C7013_04_rank_flag_forced_correction_task

class C7013_04_rank_verification_task(BaseTask):
    '''
    ランクモデル検証タスククラス
    '''

    @inject.autoparams()
    def __init__(self, rank_flag_forced_correction_task: C7013_04_rank_flag_forced_correction_task):
        '''
        初期化関数

        Args:
            rankflag_forced_correction_task: フラグ強制補正タスク
        '''
        self.__rank_model_helper: RankModelHelper = None
        self._rank_flag_forced_correction_task = rank_flag_forced_correction_task
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

    def execute(self, input_file_path: pathlib.PurePath, output_file_path: pathlib.PurePath) -> int:
        '''
        ランク判定モデルを検証します。

        Args:
            input_file_path: 検証データファイルのパス
            output_file_path: 検証データ出力ファイルのパス

        Returns:
            タスク実行結果（0:正常、1:異常、2:警告）
        '''
        self.logger.info(f'ランク判定モデルの検証を実行します。')

        # 学習データを読込
        df = pd.read_csv(input_file_path, sep=',', encoding='utf-8')
        # 入力ファイルを読込みました。ファイル名＝{0}
        self.logger.info(message.MSG['MSG0012'], input_file_path)

        if df.empty:
            # 入力ファイルにデータが存在しません。ファイル名＝{0}
            self.logger.error(message.MSG['MSG2010'], input_file_path)
            return const.BATCH_ERROR

        # ランク判定用モデルを読込
        self._rank_model_helper.load_models()

        # フラグ強制補正
        input_data = self._rank_flag_forced_correction_task.execute(df)

        input_data = rank_utils.input_data_transform(input_data)

        target_data = rank_utils.target_data_transform(df)
        a_rank_target_data = target_data[:, 0:1]
        b_rank_target_data = target_data[:, 1:2]
        c_rank_target_data = target_data[:, 2:3]
        d_rank_target_data = target_data[:, 3:4]
        bar_rank_target_data = target_data[:, 4:5]
        batch_size = const.APP_CONFIG['rank_config']['batch_size']

        test_loss, test_acc = self._rank_model_helper.evaluate_a(input_data, a_rank_target_data, batch_size)
        self.logger.info(f'evaluate a rank model test_loss={test_loss}, test_acc={test_acc}')

        test_loss, test_acc = self._rank_model_helper.evaluate_b(input_data, b_rank_target_data, batch_size)
        self.logger.info(f'evaluate b rank model test_loss={test_loss}, test_acc={test_acc}')

        test_loss, test_acc = self._rank_model_helper.evaluate_c(input_data, c_rank_target_data, batch_size)
        self.logger.info(f'evaluate c rank model test_loss={test_loss}, test_acc={test_acc}')

        test_loss, test_acc = self._rank_model_helper.evaluate_d(input_data, d_rank_target_data, batch_size)
        self.logger.info(f'evaluate d rank model test_loss={test_loss}, test_acc={test_acc}')

        test_loss, test_acc = self._rank_model_helper.evaluate_bar(input_data, bar_rank_target_data, batch_size)
        self.logger.info(f'evaluate bar rank model test_loss={test_loss}, test_acc={test_acc}')

        rank_a_predict = self._rank_model_helper.predict_a(input_data, batch_size)
        rank_b_predict = self._rank_model_helper.predict_b(input_data, batch_size)
        rank_c_predict = self._rank_model_helper.predict_c(input_data, batch_size)
        rank_d_predict = self._rank_model_helper.predict_d(input_data, batch_size)
        rank_bar_predict = self._rank_model_helper.predict_bar(input_data, batch_size)
        df['rank_a_predict'] = rank_a_predict.reshape(-1)
        df['rank_b_predict'] = rank_b_predict.reshape(-1)
        df['rank_c_predict'] = rank_c_predict.reshape(-1)
        df['rank_d_predict'] = rank_d_predict.reshape(-1)
        df['rank_bar_predict'] = rank_bar_predict.reshape(-1)
        df['rank_prediction'] = rank_utils.decide_rank(rank_a_predict, rank_b_predict, rank_c_predict, rank_d_predict, rank_bar_predict)
        # ランク予測結果（*1はint型に変換するためのコード）
        df['rank_predict_result'] = (df['rank_system'] == df['rank_prediction']) * 1

        df.to_csv(output_file_path, sep=',', encoding='utf-8', index=False)
        summary_file_path = output_file_path.with_name(output_file_path.stem + '_summary' + output_file_path.suffix)
        self.save_verification_result(df, summary_file_path)
        
        return const.BATCH_SUCCESS

    def save_verification_result(self, result: pd.DataFrame, output_file_path: pathlib.PurePath) -> None:
        '''
        ランク判定検証結果を出力します。

        Args:
            result: 検証結果データフレーム。
            output_file_path: 検証結果出力ファイルのパス
        '''
        # ランクグループ別
        rank_group = result.groupby('rank_system')
        # 正解件数
        number_of_correct = rank_group['rank_predict_result'].sum()
        number_of_correct.rename('number_of_correct', inplace = True)

        total = rank_group['rank_predict_result'].count()
        total.rename('total', inplace = True)

        total_result = pd.concat([total, number_of_correct], axis='columns')
        # 識別率
        total_result['rate'] = total_result['number_of_correct'] / total_result['total']
        # 件数割合
        total_result['percentage of cases'] = total_result['total'] / total_result['total'].sum()
        # 正解件数割合
        total_result['percentage of correct'] = total_result['number_of_correct'] / total_result['number_of_correct'].sum()
        # 合計
        total_result.loc['summary'] = total_result.sum(axis='index')

        # 小数点4桁以下を丸める
        total_result = total_result.round(4)

        total_result.to_csv(output_file_path, sep=',', encoding='utf-8', index=True)