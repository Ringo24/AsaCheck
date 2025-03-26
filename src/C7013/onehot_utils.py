# 標準ライブラリインポート
import logging
import pathlib
import pickle
from typing import Dict, List, Tuple

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
from sklearn.preprocessing import OneHotEncoder

# プロジェクトライブラリインポート
from . import const
from . import utils

ordercontents_onehot_dict: Dict[int, np.array] = {
    1: np.array([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], dtype=np.int32),  # 新設
    2: np.array([0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], dtype=np.int32),  # 移転
    4: np.array([0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0], dtype=np.int32),  # 変更
    7: np.array([0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0], dtype=np.int32),  # その他１
    8: np.array([0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0], dtype=np.int32),  # その他２
    9: np.array([0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0], dtype=np.int32),  # 増設
    10: np.array([0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0], dtype=np.int32),  # ch増
    11: np.array([0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0], dtype=np.int32),  # 番号増
    12: np.array([0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0], dtype=np.int32),  # 休止廃止
    13: np.array([0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0], dtype=np.int32),  # ch減
    14: np.array([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0], dtype=np.int32),  # 番号減
    15: np.array([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], dtype=np.int32),  # 問い合せ
}
ordercontents_onehot_zero: np.ndarray = np.array(
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], dtype=np.int32)  # 該当なし


def ordercontents_one_hot_encode(ordercontents: int) -> np.ndarray:
    '''
    当初注文内容をOne-Hotエンコードする
    存在しない当初注文内容の場合、該当なしを返却する

    Args:
        ordercontents: 当初注文内容
    Returns:
        One-Hotエンコードされた当初注文内容
    '''

    if ordercontents not in ordercontents_onehot_dict.keys():
        return ordercontents_onehot_zero
    return ordercontents_onehot_dict[ordercontents]


def ordercontents_one_hot_decode(ordercontents_onehot: np.ndarray) -> int:
    '''
    One-Hotされた当初注文内容をデコードする
    存在しない当初注文内容の場合、0を返却する

    Args:
        ordercontents: One-Hotされた当初注文内容
    Returns:
        当初注文内容
    '''

    for key, value in ordercontents_onehot_dict.items():
        if np.all(value == ordercontents_onehot):
            return key
    return 0


rank_onehot_dict: Dict[int, np.array] = {
    100: np.array([1, 0, 0, 0, 0], dtype=np.int32),  # A
    200: np.array([0, 1, 0, 0, 0], dtype=np.int32),  # B
    300: np.array([0, 0, 1, 0, 0], dtype=np.int32),  # C
    400: np.array([0, 0, 0, 1, 0], dtype=np.int32),  # D
    500: np.array([0, 0, 0, 0, 1], dtype=np.int32),  # -
}
rank_onehot_zero: np.ndarray = np.array([0, 0, 0, 0, 0], dtype=np.int32)  # 該当なし


def rank_one_hot_encode(rank: int) -> np.ndarray:
    '''
    ランクをOne-Hotエンコードする
    存在しないランクの場合、該当なしを返却する

    Args:
        rank: ランク
    Returns:
        One-Hotエンコードされたランク
    '''
    if rank not in rank_onehot_dict.keys():
        return rank_onehot_zero
    return rank_onehot_dict[rank]


def rank_one_hot_decode(rank_onehot: np.ndarray) -> int:
    '''
    One-Hotされたランクをデコードする
    存在しないランクの場合、0を返却する

    Args:
        rank_onehot: One-Hotされたランク
    Returns:
        ランク
    '''

    for key, value in rank_onehot_dict.items():
        if np.all(value == rank_onehot):
            return key
    return 0


class AddresscodeOneHotEncoder(object):
    '''
    住所コードをOne-Hotエンコードする。
    '''

    # 既定のロードパス
    default_load_path: pathlib.PurePath = const.APP_MODEL_PATH
    # 既定の保存パス
    default_save_path: pathlib.PurePath = const.APP_DATA_PATH

    def __init__(self):
        '''
        初期化関数

        '''
        # ロガー
        self._logger: logging.Logger = utils.getLogger()
        # 都道府県コード
        self._tdfkn_cd_encoder: OneHotEncoder = None
        # 市区町村コード
        self._scyosn_cd_encoder: OneHotEncoder = None
        # 大字通称コード
        self._oaza_tshum_cd_encoder: OneHotEncoder = None
        # 字丁目コード
        self._azchm_cd_encoder: OneHotEncoder = None
        # 都道府県＋市区町村コード
        self._tdfkn_scyosn_cd_encoder: OneHotEncoder = None
        # 都道府県＋市区町村コード＋大字通称コード
        self._tdfkn_scyosn_oaza_tshum_cd_encoder: OneHotEncoder = None

    @property
    def logger(self) -> logging.Logger:
        '''
        ロガー
        '''

        return self._logger

    def load_all(self, load_path: pathlib.PurePath = None) -> None:
        '''
        One-Hot Encoderをロードする。

        Args:
            load_path: ロードするフォルダ
        '''

        if not load_path:
            load_path = AddresscodeOneHotEncoder.default_load_path

        self.logger.info(f'住所コードOne Hot Encoderをロードします load_path={load_path}')

        with open(load_path / 'tdfkn_cd_one_hot_encoder.pickle', 'rb') as f:
            self._tdfkn_cd_encoder = pickle.load(f)
        with open(load_path / 'scyosn_cd_one_hot_encoder.pickle', 'rb') as f:
            self._scyosn_cd_encoder = pickle.load(f)
        with open(load_path / 'oaza_tshum_cd_one_hot_encoder.pickle', 'rb') as f:
            self._oaza_tshum_cd_encoder = pickle.load(f)
        with open(load_path / 'azchm_cd_one_hot_encoder.pickle', 'rb') as f:
            self._azchm_cd_encoder = pickle.load(f)

        with open(load_path / 'tdfkn_scyosn_cd_one_hot_encoder.pickle', 'rb') as f:
            self._tdfkn_scyosn_cd_encoder = pickle.load(f)
        with open(load_path / 'tdfkn_scyosn_oaza_tshum_cd_one_hot_encoder.pickle', 'rb') as f:
            self._tdfkn_scyosn_oaza_tshum_cd_encoder = pickle.load(f)

    def save_all(self, save_path: pathlib.PurePath = None) -> None:
        '''
        One-Hot Encoderを保存する。

        Args:
            save_path: 保存するフォルダ
        '''

        if not save_path:
            save_path = AddresscodeOneHotEncoder.default_save_path

        self.logger.info(f'住所コードOne Hot Encoderを保存します save_path={save_path}')

        with open(save_path / 'tdfkn_cd_one_hot_encoder.pickle', 'wb') as f:
            pickle.dump(self._tdfkn_cd_encoder, f)
        with open(save_path / 'scyosn_cd_one_hot_encoder.pickle', 'wb') as f:
            pickle.dump(self._scyosn_cd_encoder, f)
        with open(save_path / 'oaza_tshum_cd_one_hot_encoder.pickle', 'wb') as f:
            pickle.dump(self._oaza_tshum_cd_encoder, f)
        with open(save_path / 'azchm_cd_one_hot_encoder.pickle', 'wb') as f:
            pickle.dump(self._azchm_cd_encoder, f)

        with open(save_path / 'tdfkn_scyosn_cd_one_hot_encoder.pickle', 'wb') as f:
            pickle.dump(self._tdfkn_scyosn_cd_encoder, f)
        with open(save_path / 'tdfkn_scyosn_oaza_tshum_cd_one_hot_encoder.pickle', 'wb') as f:
            pickle.dump(self._tdfkn_scyosn_oaza_tshum_cd_encoder, f)

    def fit_all_from_file(self, addresscode_mstr_file_path: pathlib.PurePath) -> None:
        '''
        住所コードマスタファイルを元に訓練する。

        Args:
            addresscode_mstr_file_path: 住所コードマスタファイルのパス
        '''
        self.fit_tdfkn_cd_from_file(addresscode_mstr_file_path)
        self.fit_scyosn_cd_from_file(addresscode_mstr_file_path)
        self.fit_oaza_tshum_cd_from_file(addresscode_mstr_file_path)
        self.fit_azchm_cd_from_file(addresscode_mstr_file_path)

        self.fit_tdfkn_scyosn_cd_from_file(addresscode_mstr_file_path)
        self.fit_tdfkn_scyosn_oaza_tshum_cd_from_file(addresscode_mstr_file_path)

    def fit_tdfkn_cd_from_file(self, addresscode_mstr_file_path: pathlib.PurePath) -> None:
        '''
        都道府県コードを訓練する。

        Args:
            addresscode_mstr_file_path: 住所コードマスタファイルのパス
        '''
        self.logger.debug(f'住所コードCSVファイルを読み込みます。 {addresscode_mstr_file_path}')
        df = pd.read_csv(addresscode_mstr_file_path, sep=',', encoding='utf-8', dtype=object, usecols=['tdfkn_cd'])
        df.dropna(inplace=True)
        df = df.append({'tdfkn_cd': 'ZZ'}, ignore_index=True)
        encoder = OneHotEncoder(sparse=False, dtype=np.int32, handle_unknown='ignore')
        encoder.fit(df[['tdfkn_cd']])
        self._tdfkn_cd_encoder = encoder

    def fit_scyosn_cd_from_file(self, addresscode_mstr_file_path: pathlib.PurePath) -> None:
        '''
        市区町村コードを訓練する。

        Args:
            addresscode_mstr_file_path: 住所コードマスタファイルのパス
        '''
        self.logger.debug(f'住所コードCSVファイルを読み込みます。 {addresscode_mstr_file_path}')
        df = pd.read_csv(addresscode_mstr_file_path, sep=',',
                         encoding='utf-8', dtype=object, usecols=['scyosn_cd'])
        df.dropna(inplace=True)
        df = df.append({'scyosn_cd': 'ZZZ'}, ignore_index=True)
        encoder = OneHotEncoder(sparse=False, dtype=np.int32, handle_unknown='ignore')
        encoder.fit(df[['scyosn_cd']])
        self._scyosn_cd_encoder = encoder

    def fit_oaza_tshum_cd_from_file(self, addresscode_mstr_file_path: pathlib.PurePath) -> None:
        '''
        大字通称コードを訓練する。

        Args:
            addresscode_mstr_file_path: 住所コードマスタファイルのパス
        '''
        self.logger.debug(f'住所コードCSVファイルを読み込みます。 {addresscode_mstr_file_path}')
        df = pd.read_csv(addresscode_mstr_file_path, sep=',',
                         encoding='utf-8', dtype=object, usecols=['oaza_tshum_cd'])
        df.dropna(inplace=True)
        df = df.append({'oaza_tshum_cd': 'ZZZ'}, ignore_index=True)
        encoder = OneHotEncoder(sparse=False, dtype=np.int32, handle_unknown='ignore')
        encoder.fit(df[['oaza_tshum_cd']])
        self._oaza_tshum_cd_encoder = encoder

    def fit_azchm_cd_from_file(self, addresscode_mstr_file_path: pathlib.PurePath) -> None:
        '''
        字丁目コードを訓練する。

        Args:
            addresscode_mstr_file_path: 住所コードマスタファイルのパス
        '''
        self.logger.debug(f'住所コードCSVファイルを読み込みます。 {addresscode_mstr_file_path}')
        df = pd.read_csv(addresscode_mstr_file_path, sep=',',
                         encoding='utf-8', dtype=object, usecols=['azchm_cd'])
        df.dropna(inplace=True)
        df = df.append({'azchm_cd': 'ZZZ'}, ignore_index=True)
        encoder = OneHotEncoder(sparse=False, dtype=np.int32, handle_unknown='ignore')
        encoder.fit(df[['azchm_cd']])
        self._azchm_cd_encoder = encoder

    def fit_tdfkn_scyosn_cd_from_file(self, addresscode_mstr_file_path: pathlib.PurePath) -> None:
        '''
        都道府県＋市区町村コードを訓練する。

        Args:
            addresscode_mstr_file_path: 住所コードマスタファイルのパス
        '''
        self.logger.debug(f'住所コードCSVファイルを読み込みます。 {addresscode_mstr_file_path}')
        df = pd.read_csv(addresscode_mstr_file_path, sep=',', encoding='utf-8',
                         dtype=object, usecols=['tdfkn_cd', 'scyosn_cd'])
        df.dropna(inplace=True)
        df = df.append({'tdfkn_cd': 'ZZ', 'scyosn_cd': 'ZZZ'}, ignore_index=True)
        encoder = OneHotEncoder(sparse=False, dtype=np.int32, handle_unknown='ignore')
        encoder.fit(df[['tdfkn_cd', 'scyosn_cd']])
        self._tdfkn_scyosn_cd_encoder = encoder

    def fit_tdfkn_scyosn_oaza_tshum_cd_from_file(self, addresscode_mstr_file_path: pathlib.PurePath) -> None:
        '''
        都道府県＋市区町村＋大字通称コードを訓練する。

        Args:
            addresscode_mstr_file_path: 住所コードマスタファイルのパス
        '''
        self.logger.debug(f'住所コードCSVファイルを読み込みます。 {addresscode_mstr_file_path}')
        df = pd.read_csv(addresscode_mstr_file_path, sep=',', encoding='utf-8',
                         dtype=object, usecols=['tdfkn_cd', 'scyosn_cd', 'oaza_tshum_cd'])
        df.dropna(inplace=True)
        df = df.append({'tdfkn_cd': 'ZZ', 'scyosn_cd': 'ZZZ', 'oaza_tshum_cd': 'ZZZ'}, ignore_index=True)
        encoder = OneHotEncoder(sparse=False, dtype=np.int32, handle_unknown='ignore')
        encoder.fit(df[['tdfkn_cd', 'scyosn_cd', 'oaza_tshum_cd']])
        self._tdfkn_scyosn_oaza_tshum_cd_encoder = encoder

    @property
    def tdfkn_cd_categories(self) -> list:
        '''
        都道府県コード一覧を返却する

        Returns:
            都道府県コード一覧の２次元配列
        '''
        return self._tdfkn_cd_encoder.categories_

    @property
    def tdfkn_cd_length(self) -> int:
        '''
        都道府県コードの数
        '''
        return len(self.tdfkn_cd_categories[0])

    @property
    def scyosn_cd_categories(self) -> list:
        '''
        市区町村コード一覧を返却する

        Returns:
            市区町村コード一覧の２次元配列
        '''
        return self._scyosn_cd_encoder.categories_

    @property
    def scyosn_cd_length(self) -> int:
        '''
        市区町村コードの数
        '''
        return len(self.scyosn_cd_categories[0])

    @property
    def oaza_tshum_cd_categories(self) -> list:
        '''
        大字通称コード一覧を返却する

        Returns:
            大字通称コード一覧の２次元配列
        '''
        return self._oaza_tshum_cd_encoder.categories_

    @property
    def oaza_tshum_cd_length(self) -> int:
        '''
        大字通称コードの数
        '''
        return len(self.oaza_tshum_cd_categories[0])

    @property
    def azchm_cd_categories(self) -> list:
        '''
        字丁目コード一覧を返却する

        Returns:
            字丁目コード一覧の２次元配列
        '''
        return self._azchm_cd_encoder.categories_

    @property
    def azchm_cd_length(self) -> int:
        '''
        字丁目コードの数
        '''
        return len(self.azchm_cd_categories[0])

    @property
    def tdfkn_scyosn_cd_categories(self) -> list:
        '''
        都道府県＋市区町村コード一覧を返却する

        Returns:
            都道府県＋市区町村コード一覧の２次元配列
        '''
        return self._tdfkn_scyosn_cd_encoder.categories_

    @property
    def tdfkn_scyosn_cd_length(self) -> int:
        '''
        都道府県コード＋市区町村コードの数
        '''
        return len(self.tdfkn_scyosn_cd_categories[0]) + len(self.tdfkn_scyosn_cd_categories[1])

    @property
    def tdfkn_scyosn_oaza_tshum_cd_categories(self) -> list:
        '''
        都道府県コード＋市区町村コード＋大字通称コード一覧を返却する

        Returns:
            都道府県コード＋市区町村コード＋大字通称コード一覧の２次元配列
        '''
        return self._tdfkn_scyosn_oaza_tshum_cd_encoder.categories_

    @property
    def tdfkn_scyosn_oaza_tshum_cd_length(self) -> int:
        '''
        都道府県コード＋市区町村コード＋大字通称コードの長さ
        '''
        return len(self.tdfkn_scyosn_oaza_tshum_cd_categories[0]) + len(self.tdfkn_scyosn_oaza_tshum_cd_categories[1]) + len(self.tdfkn_scyosn_oaza_tshum_cd_categories[2])

    def tdfkn_cd_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        都道府県コードをOne-Hot エンコードする。
        注意：この関数は引数として2次元Numpy配列を受け取る。
        以下のように引数を渡すこと
        encoder.tdfkn_cd_transform(df[['tdfkn_cd']].values)

        Args:
            values: 都道府県コードの２次元配列
        Returns:
            One-Hotエンコードされた都道府県コードの２次元配列
        '''

        return self._tdfkn_cd_encoder.transform(values)

    def tdfkn_cd_inverse_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        One-Hot データを都道府県コードにデコードする。
        注意：この関数は引数として2次元Numpy配列を受け取る。

        Args:
            values: One-Hotエンコードされた都道府県コードの２次元配列
        Returns:
            デコードされた都道府県コードの２次元配列
        '''

        return self._tdfkn_cd_encoder.inverse_transform(values)

    def tdfkn_cd_one_hot_encode(self, tdfkn_cd: str) -> np.array:
        '''
        都道府県コードをOne-Hot エンコードする。

        Args:
            tdfkn_cd: 都道府県コード
        Returns:
            One-Hotエンコードされた都道府県コード
        '''

        values = self._tdfkn_cd_encoder.transform([[tdfkn_cd]])
        return values[0]

    def tdfkn_cd_one_hot_decode(self, one_hot: np.array) -> str:
        '''
        One-Hot データを都道府県コードにデコードする。

        Args:
            one_hot: One-Hotエンコードされた都道府県コード
        Returns:
            デコードされた都道府県コード
        '''

        values = self._tdfkn_cd_encoder.inverse_transform([one_hot])
        return values[0][0]

    def tdfkn_cd_to_sequence(self, tdfkn_cd: str) -> int:
        '''
        都道府県コードを数字に変換する。
        都道府県コード数字に変換出来ない場合、0を返却する

        Args:
            tdfkn_cd: 都道府県コード
        Returns:
            数字化された都道府県コード
        '''
        f = np.where(self.tdfkn_cd_categories[0] == tdfkn_cd)
        if len(f[0]) > 0:
            return f[0][0] + 1
        return 0

    def tdfkn_cd_from_sequence(self, tdfkn_cd_int: int) -> str:
        '''
        数字を都道府県コードに変換する。
        数字を都道府県コードに変換出来ない場合、Noneを返却する

        Args:
            tdfkn_cd_int: 数字化された都道府県コード
        Returns:
            変換された都道府県コード
        '''

        if tdfkn_cd_int < 1 or tdfkn_cd_int > len(self.tdfkn_cd_categories[0]):
            return None
        else:
            return self.tdfkn_cd_categories[0][tdfkn_cd_int-1]

    def tdfkn_cd_sequence_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        都道府県コードを数字に変換する。
        注意：この関数は引数として2次元Numpy配列を受け取る。
        以下のように引数を渡すこと
        encoder.tdfkn_cd_sequence_transform(df[['tdfkn_cd']].values)
        数字は1から始まり、都道府県コード一覧のサイズが最大値になる。
        存在しない都道府県コードの場合、0が設定される

        Args:
            values: 都道府県コードの２次元配列
        Returns:
            数字化された都道府県コードのNumpy配列
            ex) 
            array([ 1,  3,  0, 48], dtype=int32)
        '''
        result = np.zeros(len(values), dtype='int32')
        for idx, item in enumerate(values):
            f = np.where(self.tdfkn_cd_categories[0] == item[0])
            if len(f[0]) > 0:
                result[idx] = f[0][0] + 1

        return result

    def tdfkn_cd_sequence_inverse_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        数字化されたデータを都道府県コードに変換する。
        注意：この関数は引数としてNumpy配列を受け取る。
        数字は1から始まり、都道府県コード一覧のサイズが最大値になる。
        数字を都道府県コードに変換出来ない場合、Noneが設定される

        Args:
            values: 数字化された都道府県コードのNumpy配列
        Returns:
            変換された都道府県コードの２次元配列
            ex) 
            array([['01'],
                   ['03'],
                   [None],
                   ['ZZ']], dtype=object)
        '''
        result = np.empty((len(values), 1), dtype='object')
        tdfkn_cd_len = len(self.tdfkn_cd_categories[0])
        for idx, item in enumerate(values):
            if item < 1 or item > tdfkn_cd_len:
                result[idx] = None
            else:
                result[idx] = self.tdfkn_cd_categories[0][item-1]

        return result

    def scyosn_cd_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        市区町村コードをOne-Hot エンコードする。
        注意：この関数は引数として2次元Numpy配列を受け取る。
        以下のように引数を渡すこと
        encoder.scyosn_cd_transform(df[['scyosn_cd']].values)

        Args:
            values: 市区町村コードの２次元配列
        Returns:
            One-Hotエンコードされた市区町村コードの２次元配列
        '''

        return self._scyosn_cd_encoder.transform(values)

    def scyosn_cd_inverse_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        One-Hot データを市区町村コードにデコードする。
        注意：この関数は引数として2次元Numpy配列を受け取る。
        Returns:
            デコードされた市区町村コードの２次元配列
        '''

        return self._scyosn_cd_encoder.inverse_transform(values)

    def scyosn_cd_one_hot_encode(self, scyosn_cd: str) -> np.array:
        '''
        市区町村コードをOne-Hot エンコードする。

        Args:
            scyosn_cd: 市区町村コード
        Returns:
            One-Hotエンコードされた市区町村コード
        '''

        values = self._scyosn_cd_encoder.transform([[scyosn_cd]])
        return values[0]

    def scyosn_cd_one_hot_decode(self, one_hot: np.array) -> str:
        '''
        One-Hot データを市区町村コードにデコードする。

        Args:
            one_hot: One-Hotエンコードされた市区町村コード
        Returns:
            デコードされた市区町村コード
        '''

        values = self._scyosn_cd_encoder.inverse_transform([one_hot])
        return values[0][0]

    def scyosn_cd_to_sequence(self, scyosn_cd: str) -> int:
        '''
        市区町村コードを数字に変換する。
        市区町村コード数字に変換出来ない場合、0を返却する

        Args:
            scyosn_cd: 市区町村コード
        Returns:
            数字化された市区町村コード
        '''
        f = np.where(self.scyosn_cd_categories[0] == scyosn_cd)
        if len(f[0]) > 0:
            return f[0][0] + 1
        return 0

    def scyosn_cd_from_sequence(self, scyosn_cd_int: int) -> str:
        '''
        数字を市区町村コードに変換する。
        数字を市区町村コードに変換出来ない場合、Noneを返却する

        Args:
            scyosn_cd_int: 数字化された市区町村コード
        Returns:
            変換された市区町村コード
        '''

        if scyosn_cd_int < 1 or scyosn_cd_int > len(self.scyosn_cd_categories[0]):
            return None
        else:
            return self.scyosn_cd_categories[0][scyosn_cd_int-1]

    def scyosn_cd_sequence_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        市区町村コードを数字に変換する。
        注意：この関数は引数として2次元Numpy配列を受け取る。
        以下のように引数を渡すこと
        encoder.scyosn_cd_sequence_transform(df[['scyosn_cd']].values)
        数字は1から始まり、市区町村コード一覧のサイズが最大値になる。
        存在しない市区町村コードの場合、0が設定される

        Args:
            values: 市区町村コードの２次元配列
        Returns:
            数字化された市区町村コードのNumpy配列
            ex) 
            array([ 1,  19,  0, 888], dtype=int32)
        '''
        result = np.zeros(len(values), dtype='int32')
        for idx, item in enumerate(values):
            f = np.where(self.scyosn_cd_categories[0] == item[0])
            if len(f[0]) > 0:
                result[idx] = f[0][0] + 1

        return result

    def scyosn_cd_sequence_inverse_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        数字化されたデータを市区町村コードに変換する。
        注意：この関数は引数としてNumpy配列を受け取る。
        数字は1から始まり、市区町村コード一覧のサイズが最大値になる。
        数字を市区町村コードに変換出来ない場合、Noneが設定される

        Args:
            values: 数字化された市区町村コードのNumpy配列
        Returns:
            変換された市区町村コードの２次元配列
            ex) 
            array([['000'],
                   ['155'],
                   [None],
                   ['ZZZ']], dtype=object)
        '''
        result = np.empty((len(values), 1), dtype='object')
        scyosn_cd_len = len(self.scyosn_cd_categories[0])
        for idx, item in enumerate(values):
            if item < 1 or item > scyosn_cd_len:
                result[idx] = None
            else:
                result[idx] = self.scyosn_cd_categories[0][item-1]

        return result

    def oaza_tshum_cd_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        大字通称コードをOne-Hot エンコードする。
        注意：この関数は引数として2次元Numpy配列を受け取る。
        以下のように引数を渡すこと
        encoder.oaza_tshum_cd_transform(df[['oaza_tshum_cd']].values)

        Args:
            values: 大字通称コードの２次元配列
        Returns:
            One-Hotエンコードされた大字通称コードの２次元配列
        '''

        return self._oaza_tshum_cd_encoder.transform(values)

    def oaza_tshum_cd_inverse_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        One-Hot データを大字通称コードにデコードする。
        注意：この関数は引数として2次元Numpy配列を受け取る。
        Returns:
            デコードされた大字通称コードの２次元配列
        '''

        return self._oaza_tshum_cd_encoder.inverse_transform(values)

    def oaza_tshum_cd_one_hot_encode(self, oaza_tshum_cd: str) -> np.array:
        '''
        大字通称コードをOne-Hot エンコードする。

        Args:
            oaza_tshum_cd: 大字通称コード
        Returns:
            One-Hotエンコードされた大字通称コード
        '''

        values = self._oaza_tshum_cd_encoder.transform([[oaza_tshum_cd]])
        return values[0]

    def oaza_tshum_cd_one_hot_decode(self, one_hot: np.array) -> str:
        '''
        One-Hot データを大字通称コードにデコードする。

        Args:
            one_hot: One-Hotエンコードされた大字通称コード
        Returns:
            デコードされた大字通称コード
        '''

        values = self._oaza_tshum_cd_encoder.inverse_transform([one_hot])
        return values[0][0]

    def oaza_tshum_cd_to_sequence(self, oaza_tshum_cd: str) -> int:
        '''
        大字通称コードを数字に変換する。
        大字通称コード数字に変換出来ない場合、0を返却する

        Args:
            oaza_tshum_cd: 大字通称コード
        Returns:
            数字化された大字通称コード
        '''
        f = np.where(self.oaza_tshum_cd_categories[0] == oaza_tshum_cd)
        if len(f[0]) > 0:
            return f[0][0] + 1
        return 0

    def oaza_tshum_cd_from_sequence(self, oaza_tshum_cd_int: int) -> str:
        '''
        数字を大字通称コードに変換する。
        数字を大字通称コードに変換出来ない場合、Noneを返却する

        Args:
            oaza_tshum_cd_int: 数字化された大字通称コード
        Returns:
            変換された大字通称コード
        '''

        if oaza_tshum_cd_int < 1 or oaza_tshum_cd_int > len(self.oaza_tshum_cd_categories[0]):
            return None
        else:
            return self.oaza_tshum_cd_categories[0][oaza_tshum_cd_int-1]

    def oaza_tshum_cd_sequence_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        大字通称コードを数字に変換する。
        注意：この関数は引数として2次元Numpy配列を受け取る。
        以下のように引数を渡すこと
        encoder.oaza_tshum_cd_sequence_transform(df[['oaza_tshum_cd']].values)
        数字は1から始まり、大字通称コード一覧のサイズが最大値になる。
        存在しない大字通称コードの場合、0が設定される

        Args:
            values: 大字通称コードの２次元配列
        Returns:
            数字化された大字通称コードのNumpy配列
            ex) 
            array([ 1,  19,  0, 888], dtype=int32)
        '''
        result = np.zeros(len(values), dtype='int32')
        for idx, item in enumerate(values):
            f = np.where(self.oaza_tshum_cd_categories[0] == item[0])
            if len(f[0]) > 0:
                result[idx] = f[0][0] + 1

        return result

    def oaza_tshum_cd_sequence_inverse_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        数字化されたデータを大字通称コードに変換する。
        注意：この関数は引数としてNumpy配列を受け取る。
        数字は1から始まり、大字通称コード一覧のサイズが最大値になる。
        数字を大字通称コードに変換出来ない場合、Noneが設定される

        Args:
            values: 数字化された大字通称コードのNumpy配列
        Returns:
            変換された大字通称コードの２次元配列
            ex) 
            array([['000'],
                   ['155'],
                   [None],
                   ['ZZZ']], dtype=object)
        '''
        result = np.empty((len(values), 1), dtype='object')
        oaza_tshum_cd_len = len(self.oaza_tshum_cd_categories[0])
        for idx, item in enumerate(values):
            if item < 1 or item > oaza_tshum_cd_len:
                result[idx] = None
            else:
                result[idx] = self.oaza_tshum_cd_categories[0][item-1]

        return result

    def azchm_cd_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        字丁目コードをOne-Hot エンコードする。
        注意：この関数は引数として2次元Numpy配列を受け取る。
        以下のように引数を渡すこと
        encoder.azchm_cd_transform(df[['azchm_cd']].values)

        Args:
            values: 字丁目コードの２次元配列
        Returns:
            One-Hotエンコードされた字丁目コードの２次元配列
        '''

        return self._azchm_cd_encoder.transform(values)

    def azchm_cd_inverse_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        One-Hot データを字丁目コードにデコードする。
        注意：この関数は引数として2次元Numpy配列を受け取る。
        Returns:
            デコードされた字丁目コードの２次元配列
        '''

        return self._azchm_cd_encoder.inverse_transform(values)

    def azchm_cd_one_hot_encode(self, azchm_cd: str) -> np.array:
        '''
        字丁目コードをOne-Hot エンコードする。

        Args:
            azchm_cd: 字丁目コード
        Returns:
            One-Hotエンコードされた字丁目コード
        '''

        values = self._azchm_cd_encoder.transform([[azchm_cd]])
        return values[0]

    def azchm_cd_one_hot_decode(self, one_hot: np.array) -> str:
        '''
        One-Hot データを字丁目コードにデコードする。

        Args:
            one_hot: One-Hotエンコードされた字丁目コード
        Returns:
            デコードされた字丁目コード
        '''

        values = self._azchm_cd_encoder.inverse_transform([one_hot])
        return values[0][0]

    def azchm_cd_to_sequence(self, azchm_cd: str) -> int:
        '''
        字丁目コードを数字に変換する。
        字丁目コード数字に変換出来ない場合、0を返却する

        Args:
            azchm_cd: 字丁目コード
        Returns:
            数字化された字丁目コード
        '''
        f = np.where(self.azchm_cd_categories[0] == azchm_cd)
        if len(f[0]) > 0:
            return f[0][0] + 1
        return 0

    def azchm_cd_from_sequence(self, azchm_cd_int: int) -> str:
        '''
        数字を字丁目コードに変換する。
        数字を字丁目コードに変換出来ない場合、Noneを返却する

        Args:
            azchm_cd_int: 数字化された字丁目コード
        Returns:
            変換された字丁目コード
        '''

        if azchm_cd_int < 1 or azchm_cd_int > len(self.azchm_cd_categories[0]):
            return None
        else:
            return self.azchm_cd_categories[0][azchm_cd_int-1]

    def azchm_cd_sequence_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        字丁目コードを数字に変換する。
        注意：この関数は引数として2次元Numpy配列を受け取る。
        以下のように引数を渡すこと
        encoder.azchm_cd_sequence_transform(df[['azchm_cd']].values)
        数字は1から始まり、字丁目コード一覧のサイズが最大値になる。
        存在しない字丁目コードの場合、0が設定される

        Args:
            values: 字丁目コードの２次元配列
        Returns:
            数字化された字丁目コードのNumpy配列
            ex) 
            array([ 1,  19,  0, 888], dtype=int32)
        '''
        result = np.zeros(len(values), dtype='int32')
        for idx, item in enumerate(values):
            f = np.where(self.azchm_cd_categories[0] == item[0])
            if len(f[0]) > 0:
                result[idx] = f[0][0] + 1

        return result

    def azchm_cd_sequence_inverse_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        数字化されたデータを字丁目コードに変換する。
        注意：この関数は引数としてNumpy配列を受け取る。
        数字は1から始まり、字丁目コード一覧のサイズが最大値になる。
        数字を字丁目コードに変換出来ない場合、Noneが設定される

        Args:
            values: 数字化された字丁目コードのNumpy配列
        Returns:
            変換された字丁目コードの２次元配列
            ex) 
            array([['000'],
                   ['155'],
                   [None],
                   ['ZZZ']], dtype=object)
        '''
        result = np.empty((len(values), 1), dtype='object')
        azchm_cd_len = len(self.azchm_cd_categories[0])
        for idx, item in enumerate(values):
            if item < 1 or item > azchm_cd_len:
                result[idx] = None
            else:
                result[idx] = self.azchm_cd_categories[0][item-1]

        return result

    def tdfkn_scyosn_cd_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        都道府県コード、市区町村コードをOne-Hot エンコードする。
        注意：この関数は引数として2次元Numpy配列を受け取る。
        以下のように引数を渡すこと
        encoder.tdfkn_scyosn_cd_transform(df[['tdfkn_cd', 'scyosn_cd']].values)

        Args:
            values: 都道府県コード、市区町村コードの２次元配列
        Returns:
            One-Hotエンコードされた都道府県コード、市区町村コードの２次元配列
        '''

        return self._tdfkn_scyosn_cd_encoder.transform(values)

    def tdfkn_scyosn_cd_inverse_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        One-Hot データを都道府県コード、市区町村コードにデコードする。
        注意：この関数は引数として2次元Numpy配列を受け取る。

        Args:
            values: One-Hotエンコードされた都道府県コード、市区町村コードの２次元配列
        Returns:
            デコードされた都道府県コード、市区町村コードの２次元配列
        '''

        return self._tdfkn_scyosn_cd_encoder.inverse_transform(values)

    def tdfkn_scyosn_cd_one_hot_encode(self, tdfkn_cd: str, scyosn_cd: str) -> np.array:
        '''
        都道府県コード、市区町村コードをOne-Hot エンコードする。

        Args:
            tdfkn_cd: 都道府県コード
            scyosn_cd: 市区町村コード
        Returns:
            One-Hotエンコードされた都道府県コード、市区町村コード
        '''

        values = self._tdfkn_scyosn_cd_encoder.transform([[tdfkn_cd, scyosn_cd]])
        return values[0]

    def tdfkn_scyosn_cd_one_hot_decode(self, one_hot: np.array) -> str:
        '''
        One-Hot データを都道府県コード、市区町村コードにデコードする。

        Args:
            one_hot: One-Hotエンコードされた都道府県コード、市区町村コード
        Returns:
            デコードされた都道府県コード、市区町村コードの配列。0番目：都道府県コード、1番目：市区町村コード
        '''

        values = self._tdfkn_scyosn_cd_encoder.inverse_transform([one_hot])
        return values[0]

    def tdfkn_scyosn_oaza_tshum_cd_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        都道府県コード、市区町村コード、大字通称コードをOne-Hot エンコードする。
        注意：この関数は引数として2次元Numpy配列を受け取る。
        以下のように引数を渡すこと
        encoder.tdfkn_scyosn_cd_transform(df[['tdfkn_cd', 'scyosn_cd', 'oaza_tshum_cd']].values)

        Args:
            values: 都道府県コード、市区町村コード、大字通称コードの２次元配列
        Returns:
            One-Hotエンコードされた都道府県コード、市区町村コード、大字通称コードの２次元配列
        '''

        return self._tdfkn_scyosn_oaza_tshum_cd_encoder.transform(values)

    def tdfkn_scyosn_oaza_tshum_cd_inverse_transform(self, values: np.ndarray) -> np.ndarray:
        '''
        One-Hot データを都道府県コード、市区町村コード、大字通称コードにデコードする。
        注意：この関数は引数として2次元Numpy配列を受け取る。

        Args:
            values: One-Hotエンコードされた都道府県コード、市区町村コードの２次元配列
        Returns:
            デコードされた都道府県コード、市区町村コード、大字通称コードの２次元配列
        '''

        return self._tdfkn_scyosn_oaza_tshum_cd_encoder.inverse_transform(values)

    def tdfkn_scyosn_oaza_tshum_cd_one_hot_encode(self, tdfkn_cd: str, scyosn_cd: str, oaza_tshum_cd: str) -> np.array:
        '''
        都道府県コード、市区町村コード、大字通称コードをOne-Hot エンコードする。

        Args:
            tdfkn_cd: 都道府県コード
            scyosn_cd: 市区町村コード
            oaza_tshum_cd: 大字通称コード
        Returns:
            One-Hotエンコードされた都道府県コード、市区町村コード、大字通称コード
        '''

        values = self._tdfkn_scyosn_oaza_tshum_cd_encoder.transform([[tdfkn_cd, scyosn_cd, oaza_tshum_cd]])
        return values[0]

    def tdfkn_scyosn_oaza_tshum_cd_one_hot_decode(self, one_hot: np.array) -> str:
        '''
        One-Hot データを都道府県コード、市区町村コード、大字通称コードにデコードする。

        Args:
            one_hot: One-Hotエンコードされた都道府県コード、市区町村コード、大字通称コード
        Returns:
            デコードされた都道府県コード、市区町村コード、大字通称コードの配列。0番目：都道府県コード、1番目：市区町村コード、2番目：大字通称コード
        '''

        values = self._tdfkn_scyosn_oaza_tshum_cd_encoder.inverse_transform([one_hot])
        return values[0]

