# 標準ライブラリインポート

# サードパーティライブラリインポート
import pandas as pd
import inject

# プロジェクトライブラリインポート
from . import rank_utils
from .task import BaseTask

class C7013_04_rank_flag_forced_correction_task(BaseTask):
    '''
    フラグ強制補正を行うタスククラス
    '''

    @inject.autoparams()
    def __init__(self):
        '''
        初期化関数
        '''
        #親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self, input_data: pd.DataFrame) -> pd.DataFrame:
        '''
        フラグを強制補正を行う
        Args:
            input_data: 入力DataFrame
        Returns:
            強制補正済みDataFrame
        '''
        self.logger.info(f'フラグ強制補正タスクを実行します。')

        output_data = input_data.apply(self.set_force_flag, axis=1)

        return output_data

    def set_force_flag(self, row: pd.Series) -> pd.Series:
        '''
        フラグを強制敵に設定する

        Args:
            row: ランク判定用データフレームの１行
        '''

        # No32 Ｂフレ マイグレ（アウトバウンド）
        if self.is_rule_no32(row):
            rank_utils.clear_all_rank_flag(row)
            self.set_rule_no32(row)
        # No33 ADSL マイグレ（アウトバウンド）
        elif self.is_rule_no33(row):
            rank_utils.clear_all_rank_flag(row)
            self.set_rule_no33(row)
        # No35 パートナーセンタからの取次
        elif self.is_rule_no35(row):
            rank_utils.clear_all_rank_flag(row)
            self.set_rule_no35(row)
        # No40 アップセルNGベンダ名義・申込
        elif self.is_rule_no40(row):
            rank_utils.clear_all_rank_flag(row)
            self.set_rule_no40(row)
        # No16 現場事務所系・イベント系(ブースモデルルーム等)への注文
        elif self.is_rule_no16(row):
            rank_utils.clear_all_rank_flag(row)
            self.set_rule_no16(row)

        return row

    def is_rule_no32(self, row: pd.Series) -> int:
        '''
        NO32：Ｂフレ マイグレ（アウトバウンド）
        '''
        if row['ordercontents'] in [
                rank_utils.ordercontents_dict['新設'],
                rank_utils.ordercontents_dict['変更'],
                rank_utils.ordercontents_dict['問い合せ'],
            ]:
            if row['rank_flag33'] == 1 and row['rank_flag24'] == 1 and row['rank_flag07'] == 0:
                return 1
        return 0

    def set_rule_no32(self, row: pd.Series) -> None:
        '''
        NO32：Ｂフレ マイグレ（アウトバウンド）
        '''
        row['rank_flag33'] = 1
        row['rank_flag24'] = 1
        row['rank_flag07'] = 0

    def is_rule_no33(self, row: pd.Series) -> int:
        '''
        NO33：ADSL マイグレ（アウトバウンド）
        '''
        if row['ordercontents'] in [
                rank_utils.ordercontents_dict['新設'],
                rank_utils.ordercontents_dict['変更'],
                rank_utils.ordercontents_dict['問い合せ'],
            ]:
            if row['rank_flag33'] == 1 and row['rank_flag06'] == 1 and row['rank_flag07'] == 0:
                return 1
        return 0

    def set_rule_no33(self, row: pd.Series) -> None:
        '''
        NO33：ADSL マイグレ（アウトバウンド）
        '''
        row['rank_flag33'] = 1
        row['rank_flag06'] = 1
        row['rank_flag07'] = 0

    def is_rule_no35(self, row: pd.Series) -> int:
        '''
        NO35：パートナーセンタからの取次
        '''
        if row['rank_flag35'] == 1:
            return 1
        return 0

    def set_rule_no35(self, row: pd.Series) -> None:
        '''
        NO35：パートナーセンタからの取次
        '''
        row['rank_flag35'] = 1

    def is_rule_no40(self, row: pd.Series) -> int:
        '''
        NO40：アップセルNGベンダ名義・申込
        '''
        if row['rank_flag41'] == 1:
            return 1
        return 0

    def set_rule_no40(self, row: pd.Series) -> None:
        '''
        NO40：アップセルNGベンダ名義・申込
        '''
        row['rank_flag41'] = 1

    def is_rule_no16(self, row: pd.Series) -> int:
        '''
        NO16：現場事務所系・イベント系(ブースモデルルーム等)への注文
        '''
        if row['ordercontents'] in [
                rank_utils.ordercontents_dict['新設'],
                rank_utils.ordercontents_dict['増設'],
                rank_utils.ordercontents_dict['変更'],
                rank_utils.ordercontents_dict['問い合せ'],
            ]:
            if row['rank_flag21'] == 0 and row['rank_flag39'] == 1:
                return 1
        return 0

    def set_rule_no16(self, row: pd.Series) -> None:
        '''
        NO16：現場事務所系・イベント系(ブースモデルルーム等)への注文
        '''
        row['rank_flag21'] = 0
        row['rank_flag39'] = 1
