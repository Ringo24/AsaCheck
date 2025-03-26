# 標準ライブラリインポート
import pathlib

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
import inject

# プロジェクトライブラリインポート
from . import const
from . import message
from .task import BaseTask
from .addresscode_utils import JapaneseSentenceVectorizer
from .onehot_utils import AddresscodeOneHotEncoder
from .scyosn_cd_model_helper import ScyosnCdModelHelper
from .oaza_tshum_cd_model_helper import OazaTshumCdModelHelper
from .azchm_cd_model_helper import AzchmCdModelHelper
from .addresscode_utils import extract_tdfkn_from_address
from .addresscode_utils import address_cleansing
from .addresscode_utils import unification_text
from .addresscode_utils import azchm_hypen_inverse_convert
from .addresscode_utils import azchm_after_address_truncate

class C7013_04_addresscode_verification_task(BaseTask):
    '''
    住所コード変換モデルを検証します。
    '''

    @inject.autoparams()
    def __init__(self):
        '''
        初期化関数
        '''

        # 親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self, input_file_path: pathlib.PurePath, output_file_path: pathlib.PurePath) -> int:
        '''
            市区町村コード変換モデル、大字通称コード変換モデル、字丁目コード変換モデルを検証する。

        Args:
            input_file_path: 住所コード変換検証用ファイルのパス
            output_file_path: 住所コード変換検証結果用ファイルのパス

        Returns:
            タスク実行結果（0:正常、1:異常、2:警告）
        '''
        self.logger.info(f'住所コード変換検証タスクを実行します。')

        input_data = pd.read_csv(input_file_path, sep=',', encoding='utf-8', dtype=object)
        # 入力ファイルを読込みました。ファイル名＝{0}
        self.logger.info(message.MSG['MSG0012'], input_file_path)

        if input_data.empty:
            # 入力ファイルにデータが存在しません。ファイル名＝{0}
            self.logger.error(message.MSG['MSG2010'], input_file_path)
            return const.BATCH_ERROR

        one_hot_encoder: AddresscodeOneHotEncoder = AddresscodeOneHotEncoder()
        one_hot_encoder.load_all()

        vectorizer = JapaneseSentenceVectorizer.load_from_file()
        vectorizer.max_tokens = const.APP_CONFIG['addresscode_config']['max_vocab_size']
        vectorizer.output_sequence_length = const.APP_CONFIG['addresscode_config']['max_sequence_length']

        # 市区町村コード変換モデル
        scyosn_cd_helper: ScyosnCdModelHelper = ScyosnCdModelHelper(one_hot_encoder, vectorizer)
        scyosn_cd_helper.load_model()

        # 大字通称コード変換モデル
        oaza_tshum_cd_helper: OazaTshumCdModelHelper = OazaTshumCdModelHelper(one_hot_encoder, vectorizer)
        oaza_tshum_cd_helper.load_model()

        # 字丁目コード変換モデル
        azchm_cd_helper: AzchmCdModelHelper = AzchmCdModelHelper(one_hot_encoder, vectorizer)
        azchm_cd_helper.load_model()

        # 都道府県コードと住所（市区町村以下）を抽出
        self.logger.debug(f'都道府県コードと住所（市区町村以下）を抽出します。')
        output_data = input_data.apply(self._extract_tdfkn_from_df, axis='columns')

        batch_size = const.APP_CONFIG['addresscode_config']['batch_size']

        # 市区町村コード変換モデルを評価
        test_loss, test_acc = scyosn_cd_helper.evaluate(
            output_data[['tdfkn_cd']].values,
            output_data['nomalized_addr_nm'].values,
            output_data[['scyosn_cd']].values,
            batch_size)
        self.logger.info(f'evaluate a scyosn_cd model test_loss={test_loss}, test_acc={test_acc}')

        # 市区町村コードを予測
        predict = scyosn_cd_helper.predict(
            output_data[['tdfkn_cd']].values,
            output_data['nomalized_addr_nm'].values,
            batch_size)
        output_data['scyosn_cd_pred'] = predict

        # 大字通称コード変換モデルを評価
        test_loss, test_acc = oaza_tshum_cd_helper.evaluate(
            output_data[['tdfkn_cd']].values,
            output_data[['scyosn_cd']].values,
            output_data['nomalized_addr_nm'].values,
            output_data[['oaza_tshum_cd']].values,
            batch_size)
        self.logger.info(f'evaluate a oaza_tshum_cd model test_loss={test_loss}, test_acc={test_acc}')

        # 大字通称コードを予測
        predict = oaza_tshum_cd_helper.predict(
            output_data[['tdfkn_cd']].values,
            output_data[['scyosn_cd']].values,
            output_data['nomalized_addr_nm'].values,
            batch_size)
        output_data['oaza_tshum_cd_pred'] = predict

        # 字丁目コード変換モデルを評価
        test_loss, test_acc = azchm_cd_helper.evaluate(
            output_data[['tdfkn_cd']].values,
            output_data[['scyosn_cd']].values,
            output_data[['oaza_tshum_cd']].values,
            output_data['nomalized_addr_nm'].values,
            output_data[['azchm_cd']].values,
            batch_size)
        self.logger.info(f'evaluate a azchm_cd model test_loss={test_loss}, test_acc={test_acc}')

        # 字丁目コードを予測
        predict = azchm_cd_helper.predict(
            output_data[['tdfkn_cd']].values,
            output_data[['scyosn_cd']].values,
            output_data[['oaza_tshum_cd']].values,
            output_data['nomalized_addr_nm'].values,
            batch_size)
        output_data['azchm_cd_pred'] = predict

        # 市区町村コード予測結果（*1はint型に変換するためのコード）
        output_data['scyosn_cd_rslt'] = (output_data['scyosn_cd'] == output_data['scyosn_cd_pred']) * 1
        # 大字通称コード予測結果（*1はint型に変換するためのコード）
        output_data['oaza_tshum_cd_rslt'] = (output_data['oaza_tshum_cd'] == output_data['oaza_tshum_cd_pred']) * 1
        # 字丁目コード予測結果（*1はint型に変換するためのコード）
        output_data['azchm_cd_rslt'] = (output_data['azchm_cd'] == output_data['azchm_cd_pred']) * 1

        # 機械が解析したワードを分析するため
        # 住所（市区町村以下）をシーケンス化に変換してテキストに戻した値を保存する
        tokenized_texts = vectorizer.sequences_to_texts(vectorizer.texts_to_sequences(output_data['nomalized_addr_nm']))
        output_data['tokenized_addr_nm'] = [" ".join(x).strip() for x in tokenized_texts]

        output_data.to_csv(output_file_path, sep=',', encoding='utf-8', index=False, header=True)
        summary_file_path = output_file_path.with_name(output_file_path.stem + '_summary' + output_file_path.suffix)
        self.save_verification_result(output_data, summary_file_path)

        return const.BATCH_SUCCESS

    def _extract_tdfkn_from_df(self, row: pd.Series) -> pd.Series:
        '''
        都道府県コードを抽出する

        Args:
            row: 住所DataFrameの1行

        Returns:
            tdfkn_nm, tdfkn_cd, next_account_street_addrが追加されたSeries
        '''
        address = row["addr_nm"]
        # NaN対応
        if not address or not isinstance(address, str):
            row["nomalized_addr_nm"] = ''
            row["tdfkn_nm_dtd"] = ''
            row["tdfkn_cd_dtd"] = ''
            row["street_addr_dtd"] = ''
            return row

        # 住所のクレンジング処理を実施する
        cleansed_address = address_cleansing(address)
        # 文字列を統一化する
        nomalized_address = unification_text(cleansed_address)
        nomalized_address = azchm_hypen_inverse_convert(nomalized_address)
        nomalized_address = azchm_after_address_truncate(nomalized_address)
        val = extract_tdfkn_from_address(nomalized_address)

        row["nomalized_addr_nm"] = nomalized_address # 住所（正規化後）
        row["tdfkn_nm_dtd"] = val[0]                 # 都道府県名
        row["tdfkn_cd_dtd"] = val[1]                 # 都道府県コード
        row["street_addr_dtd"] = val[2]              # 住所（市区町村以下）

        return row

    def save_verification_result(self, result: pd.DataFrame, output_file_path: pathlib.PurePath) -> None:
        '''
        住所コード変換検証結果を出力する。

        Args:
            result: 検証結果データフレーム。
            output_file_path: 検証結果出力ファイルのパス
        '''
        # 都道府県コードグループ別
        tdfkn_cd_group = result.groupby('tdfkn_cd')
        scyosn_cd_correct = tdfkn_cd_group['scyosn_cd_rslt'].sum()
        scyosn_cd_correct.rename('scyosn_cd_correct', inplace = True)

        oaza_tshum_cd_correct = tdfkn_cd_group['oaza_tshum_cd_rslt'].sum()
        oaza_tshum_cd_correct.rename('oaza_tshum_cd_correct', inplace = True)

        azchm_cd_correct = tdfkn_cd_group['azchm_cd_rslt'].sum()
        azchm_cd_correct.rename('azchm_cd_correct', inplace = True)

        total = tdfkn_cd_group['scyosn_cd_rslt'].count()
        total.rename('total', inplace = True)

        total_result = pd.concat([total, scyosn_cd_correct, oaza_tshum_cd_correct, azchm_cd_correct], axis='columns')
        # 合計
        total_result.loc['summary'] = total_result.sum(axis='index')

        # 小数点4桁以下を丸める
        total_result = total_result.round(4)

        total_result.to_csv(output_file_path, sep=',', encoding='utf-8', index=True)       
