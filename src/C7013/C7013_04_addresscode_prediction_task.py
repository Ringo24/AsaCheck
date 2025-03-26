# 標準ライブラリインポート

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
import inject

# プロジェクトライブラリインポート
from . import const
from .task import BaseTask
from .onehot_utils import AddresscodeOneHotEncoder
from .scyosn_cd_model_helper import ScyosnCdModelHelper
from .oaza_tshum_cd_model_helper import OazaTshumCdModelHelper
from .azchm_cd_model_helper import AzchmCdModelHelper
from .addresscode_utils import extract_tdfkn_from_address
from .addresscode_utils import address_cleansing
from .addresscode_utils import unification_text
from .addresscode_utils import azchm_hypen_inverse_convert
from .addresscode_utils import azchm_after_address_truncate
from .addresscode_utils import JapaneseSentenceVectorizer

class C7013_04_addresscode_prediction_task(BaseTask):
    '''
    住所コード変換モデルを読み込み、住所コードを付与します。
    '''

    @inject.autoparams()
    def __init__(self):
        '''
        初期化関数
        '''

        one_hot_encoder: AddresscodeOneHotEncoder = AddresscodeOneHotEncoder()
        one_hot_encoder.load_all()


        vectorizer = JapaneseSentenceVectorizer.load_from_file()
        vectorizer.max_tokens = const.APP_CONFIG['addresscode_config']['max_vocab_size']
        vectorizer.output_sequence_length = const.APP_CONFIG['addresscode_config']['max_sequence_length']

        self._scyosn_cd_helper: ScyosnCdModelHelper = ScyosnCdModelHelper(one_hot_encoder, vectorizer)
        self._scyosn_cd_helper.load_model()

        self._oaza_tshum_cd_helper: OazaTshumCdModelHelper = OazaTshumCdModelHelper(one_hot_encoder, vectorizer)
        self._oaza_tshum_cd_helper.load_model()

        self._azchm_cd_helper: AzchmCdModelHelper = AzchmCdModelHelper(one_hot_encoder, vectorizer)
        self._azchm_cd_helper.load_model()

        # 親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self, input_data: pd.DataFrame) -> pd.DataFrame:
        '''
        データフレームの新設置場所(next_account)から住所コードを抽出し、
        新設置場所(住所コード)(next_account_code)を設定して返却します。
        変換できなかった場合は、NaNが設定されます。

        Args:
            input_data: データフレーム、このデータフレームには新設置場所が設定されていることを想定します。

        Returns:
            住所コード（11桁）が付与されたデータフレーム

        '''
        self.logger.info(f'住所コード変換タスクを実行します。')

        df = input_data.apply(self._extract_tdfkn_from_df, axis='columns')
        predict = self._scyosn_cd_helper.predict(df[['tdfkn_cd']].values, df['nomalized_next_account'].values)
        df['scyosn_cd'] = predict

        predict = self._oaza_tshum_cd_helper.predict(df[['tdfkn_cd']].values, df[['scyosn_cd']].values, df['nomalized_next_account'].values)
        df['oaza_tshum_cd'] = predict


        predict = self._azchm_cd_helper.predict(df[['tdfkn_cd']].values, df[['scyosn_cd']].values, df[['oaza_tshum_cd']].values, df['nomalized_next_account'].values)
        df['azchm_cd'] = predict

        input_data['next_account_code'] = df.apply(self._join_addresscode, axis='columns')
        return input_data

    def address2addresscode(self, address: str) -> str:
        '''
        住所を住所コードに変換します。
        変換できなかった場合、Noneを返却します

        Args:
            address: 住所の文字列

        Returns:
            住所コード（11桁）
        '''

        if not address:
            return None

        # 住所のクレンジング処理を実施する
        cleansed_address = address_cleansing(address)
        # 文字列を統一化する
        nomalized_address = unification_text(cleansed_address)
        nomalized_address = azchm_hypen_inverse_convert(nomalized_address)
        nomalized_address = azchm_after_address_truncate(nomalized_address)
        self.logger.debug(f'address:{address} nomalized_address:{nomalized_address}')

        tdfkn_cd = extract_tdfkn_from_address(nomalized_address)[1]
        self.logger.debug(f'tdfkn_cd:{tdfkn_cd}')
        if tdfkn_cd == 'ZZ':
            return None
        predict = self._scyosn_cd_helper.predict(tdfkn_cd_list=np.array([[tdfkn_cd]]), addr_nm_list=np.array([nomalized_address]))
        scyosn_cd = predict[0][0]
        self.logger.debug(f'scyosn_cd:{scyosn_cd}')
        if scyosn_cd == 'ZZZ':
            return None

        predict = self._oaza_tshum_cd_helper.predict(
            tdfkn_cd_list=np.array([[tdfkn_cd]]),
            scyosn_cd_list=np.array([[scyosn_cd]]),
            addr_nm_list=np.array([nomalized_address]))
        oaza_tshum_cd = predict[0][0]
        self.logger.debug(f'oaza_tshum_cd:{oaza_tshum_cd}')
        if oaza_tshum_cd == 'ZZZ':
            return None

        predict = self._azchm_cd_helper.predict(
            tdfkn_cd_list=np.array([[tdfkn_cd]]),
            scyosn_cd_list=np.array([[scyosn_cd]]),
            oaza_tshum_cd_list=np.array([[oaza_tshum_cd]]),
            addr_nm_list=np.array([nomalized_address]))
        azchm_cd = predict[0][0]
        self.logger.debug(f'azchm_cd:{azchm_cd}')
        if azchm_cd == 'ZZZ':
            return None

        addresscode = f'{tdfkn_cd}{scyosn_cd}{oaza_tshum_cd}{azchm_cd}'
        return addresscode

    def _join_addresscode(self, row: pd.Series) -> str:
        '''
        住所コード生成します。

        Args:
            row: 住所DataFrameの1行

        Returns:
            住所コード（11桁）
        '''

        tdfkn_cd = row['tdfkn_cd']
        scyosn_cd = row['scyosn_cd']
        oaza_tshum_cd = row['oaza_tshum_cd']
        azchm_cd = row['azchm_cd']
        if tdfkn_cd == 'ZZ' or scyosn_cd == 'ZZZ' or oaza_tshum_cd == 'ZZZ' or azchm_cd == 'ZZZ':
            self.logger.debug(f'addr_cd:{tdfkn_cd}{scyosn_cd}{oaza_tshum_cd}{azchm_cd}')
            next_account_code = np.NaN
        else:
            next_account_code = f'{tdfkn_cd}{scyosn_cd}{oaza_tshum_cd}{azchm_cd}'
        return next_account_code

    def _extract_tdfkn_from_df(self, row: pd.Series) -> pd.Series:
        '''
        都道府県コードを抽出する

        Args:
            row: 住所DataFrameの1行

        Returns:
            tdfkn_nm, tdfkn_cd, next_account_street_addrが追加されたSeries
        '''
        address = row["next_account"]
        # NaN対応
        if not address or not isinstance(address, str):
            row["nomalized_next_account"] = ''
            row["tdfkn_nm"] = ''
            row["tdfkn_cd"] = ''
            row["next_account_street_addr"] = ''
            return row

        # 住所のクレンジング処理を実施する
        cleansed_address = address_cleansing(address)
        # 文字列を統一化する
        nomalized_address = unification_text(cleansed_address)
        nomalized_address = azchm_hypen_inverse_convert(nomalized_address)
        nomalized_address = azchm_after_address_truncate(nomalized_address)
        val = extract_tdfkn_from_address(nomalized_address)

        row["nomalized_next_account"] = nomalized_address  # 住所（正規化後）
        row["tdfkn_nm"] = val[0]                           # 都道府県名
        row["tdfkn_cd"] = val[1]                           # 都道府県コード
        row["next_account_street_addr"] = val[2]           # 住所（市区町村以下）

        return row

