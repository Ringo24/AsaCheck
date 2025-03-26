# 標準ライブラリインポート
import logging
import pathlib

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
from tensorflow.keras import layers
from tensorflow.keras import models
from tensorflow.keras import utils
from tensorflow.keras import callbacks
from tensorflow.keras.utils import Sequence
from tensorflow.python.keras.utils.vis_utils import plot_model
from tensorflow.keras.backend import cast

# プロジェクトライブラリインポート
from . import const
from . import message
from . import utils
from .onehot_utils import AddresscodeOneHotEncoder
from .addresscode_utils import extract_tdfkn_from_address
from .addresscode_utils import unification_text
from .addresscode_utils import azchm_hypen_inverse_convert
from .addresscode_utils import azchm_after_address_truncate
from .addresscode_utils import JapaneseSentenceVectorizer

class ScyosnCdModelHelper(object):
    '''
    市区町村コード変換用モデルを構築するためのHelperクラスです
    '''

    def __init__(self, one_hot_encoder: AddresscodeOneHotEncoder = None, vectorizer: JapaneseSentenceVectorizer = None):
        '''
        初期化関数

        Args:
            one_hot_encoder: 住所コードのOne Hot Encoder、指定しない場合は新しいAddresscodeOneHotEncoderを利用します。
            vectorizer: JapaneseSentenceVectorizer
        '''
        self._logger: logging.Logger = utils.getLogger()
        if not one_hot_encoder:
            one_hot_encoder = AddresscodeOneHotEncoder()
            one_hot_encoder.load_all()
        self._one_hot_encoder: AddresscodeOneHotEncoder = one_hot_encoder
        if not vectorizer:
            vectorizer = JapaneseSentenceVectorizer.load_from_file()
        self._vectorizer: JapaneseSentenceVectorizer = vectorizer
        self._model: models.Model = None
        self._training_history: callbacks.History = None

    @property
    def logger(self) -> logging.Logger:
        '''
        ロガー
        '''

        return self._logger

    @property
    def metrics_names(self):
        return self._model.metrics_names

    def assembly_model(self) -> None:
        '''
            市区町村コード変換用モデルを構築します。
        '''

        address_input_layer = layers.Input(shape=(self._vectorizer.output_sequence_length, ), name='addr_nm_input', dtype=np.int32)
        embedded_address_layer = layers.Embedding(input_dim=self._vectorizer.max_tokens, output_dim=64, name='addr_nm_embedding')(address_input_layer)
        # 双方向LSTM
        encoded_address_layer = layers.Bidirectional(
            layer=layers.LSTM(units=64, return_sequences=True, dropout=0.1, recurrent_dropout=0.1),
            name="addr_nm_bidirectional_lstm")(embedded_address_layer)

        flatten_address_layer = layers.Flatten(name='flatten_address_layer')(encoded_address_layer)

        tdfkn_cd_input_layer = layers.Input(shape=(self._one_hot_encoder.tdfkn_cd_length, ), name='tdfkn_cd_input', dtype=np.int32)
        tdfkn_cd_float_layer = layers.Lambda(lambda x:cast(x, 'float32'), name='tdfkn_cd_float_converter')(tdfkn_cd_input_layer)

        # 結合
        concatenated_layer = layers.Concatenate(
            axis=-1,
            name='concatenated')([
                tdfkn_cd_float_layer,
                flatten_address_layer])

        dropout_layer = layers.Dropout(name='concatenated_dropout', rate=0.1)(concatenated_layer)

        dense_layer = layers.Dense(name='concatenated_dense', units=self._one_hot_encoder.scyosn_cd_length*2, activation='relu')(dropout_layer)

        # 出力を定義
        scyosn_cd_output_layer = layers.Dense(units=self._one_hot_encoder.scyosn_cd_length, activation='softmax', name='scyosn_cd_output')(dense_layer)

        # モデル定義とコンパイル
        self._model = models.Model(
            name='scyosn_cd_classification_model',
            inputs={
                'tdfkn_cd_input': tdfkn_cd_input_layer,
                'addr_nm_input': address_input_layer,
            },
            outputs={
                'scyosn_cd_output': scyosn_cd_output_layer
            })
        self._model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])
        self._model.summary()

    def training_callbacks(self) -> list:
        '''
        学習時のコールバック関数
        '''

        # val_lossが5回以上改善されない場合、学習を終了する
        early_stop = callbacks.EarlyStopping(monitor='val_loss', patience=5)

        return [early_stop]

    def fit_model_from_file(self, train_file_path: pathlib.PurePath, val_file_path: pathlib.PurePath, batch_size: int, epochs: int, workers: int = 0) -> None:
        '''
        市区町村コード変換用モデルを訓練します。

        Args:
            train_file_path: 学習用ファイルのパス
            val_file_path: 検証用ファイルのパス
            batch_size: batch_size
            epochs: 学習回数
            workers:workers
        '''
        self.logger.info(f'市区町村コード分類モデルを訓練します batch_size={batch_size}, epochs={epochs}')
        train_gen = ScyosnCdTrainingGenerator(self._one_hot_encoder, self._vectorizer, train_file_path, batch_size=batch_size)
        val_gen = ScyosnCdTrainingGenerator(self._one_hot_encoder, self._vectorizer, val_file_path, batch_size=batch_size)
        self._training_history = self._model.fit(
            train_gen,
            steps_per_epoch=train_gen.num_batches_per_epoch,
            validation_data=val_gen,
            validation_steps=val_gen.num_batches_per_epoch,
            epochs=epochs,
            callbacks=self.training_callbacks(),
            workers=workers)

    def load_model(self, load_path: pathlib.PurePath = None) -> None:
        '''
            市区町村コード変換用モデルをロードします。

        Args:
            load_path: ロードするフォルダのパス、指定しない場合はモデルパスからロードします。
        '''
        if not load_path:
            load_path = const.APP_MODEL_PATH

        model_path = load_path / 'scyosn_cd_model.h5'
        self.logger.info(f'市区町村コード変換用モデルを読み込みます load_path={model_path}')
        self._model = models.load_model(model_path)

    def save_model(self, save_path: pathlib.PurePath = None) -> None:
        '''
            市区町村コード変換用モデルを保存します。

        Args:
            save_path: 保存するフォルダのパス、指定しない場合はデータパスに保存します。
        '''
        if not save_path:
            save_path = const.APP_DATA_PATH

        model_path = save_path / 'scyosn_cd_model.h5'
        self.logger.debug(f'市区町村コード変換用モデルを保存します save_path={save_path}')
        # include_optimizer=Falseの場合、モデルを再学習することはできないが、出力サイズを抑制できる
        self._model.save(model_path, include_optimizer=False)
        plot_model(self._model, to_file=save_path / 'scyosn_cd_model.png', show_shapes=True, show_layer_names=True)

        # モデルファイルを保存しました。保存先＝%s
        self.logger.info(message.MSG['MSG0010'], model_path)

    def save_training_accuracy_and_loss(self) -> None:
        '''
            訓練と検証正確度をイメージで保存
        '''

        utils.save_training_accuracy_and_loss(self._training_history, 'scyosn_cd_model_training_accuracy_and_loss.png')

    def save_training_and_validation_loss(self) -> None:
        '''
            訓練と検証損失をイメージで保存
        '''

        utils.save_training_and_validation_loss(self._training_history, 'scyosn_cd_model_training_and_validation_loss.png')

    def predict(self, tdfkn_cd_list: np.ndarray, addr_nm_list: np.ndarray, batch_size: int = 1, verbose: int = 1) -> np.ndarray:
        '''
        市区町村コード予測

        Args:
            tdfkn_cd_list: 都道府県コードのNumpy配列
            addr_nm_list: 住所のNumpy配列
        Returns:
            市区町村コードのNumpy配列
        '''
        self.logger.debug('市区町村コードを予測します')

        tdfkn_cdV = self._one_hot_encoder.tdfkn_cd_transform(tdfkn_cd_list)
        addr_nmV = self._vectorizer.texts_to_sequences(addr_nm_list)
        predV = self._model.predict(x={'tdfkn_cd_input': tdfkn_cdV, 'addr_nm_input': addr_nmV}, batch_size=batch_size, verbose=verbose)
        return self._one_hot_encoder.scyosn_cd_inverse_transform(predV['scyosn_cd_output'])

    def evaluate(self, tdfkn_cd_list: np.ndarray, addr_nm_list: np.ndarray, scyosn_cd_list: np.ndarray, batch_size: int = 1, verbose: int = 1) -> tuple:
        '''
        市区町村コード予測の精度を測定します。

        Args:
            tdfkn_cd_list: 都道府県コードのNumpy配列
            addr_nm_list: 住所のNumpy配列
            scyosn_cd_list: 市区町村コードのNumpy配列
            batch_size: バッチサイズ
            verbose: 詳細ログを出力する場合、1
        
        Returns:
            loss, accuracyのTuple
        '''
        self._model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])

        tdfkn_cdV = self._one_hot_encoder.tdfkn_cd_transform(tdfkn_cd_list)
        addr_nmV = self._vectorizer.texts_to_sequences(addr_nm_list)
        scyosn_cdV = self._one_hot_encoder.scyosn_cd_transform(scyosn_cd_list)
        test_loss, test_acc = self._model.evaluate(
            x={
                'tdfkn_cd_input': tdfkn_cdV,
                'addr_nm_input': addr_nmV
            },
            y={
                'scyosn_cd_output': scyosn_cdV
            },
            batch_size=batch_size,
            verbose=verbose
        )
        return (test_loss, test_acc)

class ScyosnCdTrainingGenerator(Sequence):
    '''
    市区町村コード training generatorクラスV2
    '''

    def __init__(self, one_hot_encoder: AddresscodeOneHotEncoder, vectorizer: JapaneseSentenceVectorizer, file_path: pathlib.PurePath, batch_size: int = 1):
        '''
        初期化関数

        Args:
            one_hot_encoder: AddresscodeOneHotEncoder
            vectorizer: JapaneseSentenceVectorizer
            file_path: addresscode filepath
            batch_size: Batch size
        '''

        # ロガー
        self._logger: logging.Logger = utils.getLogger()
        self._one_hot_encoder: AddresscodeOneHotEncoder = one_hot_encoder
        self._vectorizer: JapaneseSentenceVectorizer = vectorizer
        self._file_path: pathlib.PurePath = file_path
        self._batch_size: int = batch_size
        self._input_data: pd.DataFrame = pd.read_csv(file_path, sep=',', encoding='utf-8', dtype=object, usecols=['addr_nm', 'tdfkn_cd', 'scyosn_cd'])
        if self._input_data.empty:
            raise RuntimeError(f"データが存在しません。file_path={file_path}")
        self._input_data = self._input_data.apply(self._pre_process, axis='columns')

        self._addr_nm_input = self._vectorizer.texts_to_sequences(self._input_data['nomalized_addr_nm'].values)
        self._tdfkn_cd_input = self._one_hot_encoder.tdfkn_cd_transform(self._input_data[['tdfkn_cd']].values)
        self._scyosn_cd_output = self._one_hot_encoder.scyosn_cd_transform(self._input_data[['scyosn_cd']].values)

        self._length: int = len(self._input_data)

        self._num_batches_per_epoch: int = int((self._length - 1) / batch_size) + 1

    def _pre_process(self, row: pd.Series) -> pd.Series:
        address = row["addr_nm"]
        nomalized_address = unification_text(address)
        nomalized_address = azchm_hypen_inverse_convert(nomalized_address)
        nomalized_address = azchm_after_address_truncate(nomalized_address)
        val = extract_tdfkn_from_address(nomalized_address)
        row["nomalized_addr_nm"] = nomalized_address
        row["addr_nm_street_addr"] = val[2]

        return row

    @property
    def logger(self) -> logging.Logger:
        '''
        ロガー
        '''
        return self._logger

    @property
    def num_batches_per_epoch(self) -> int:
        return self._num_batches_per_epoch

    def __getitem__(self, idx: int) -> dict:
        '''
        Get batch data
        Args:
            idx: 取得するデータのインデックス
        :return train_住所V_list: numpy array of address
        :return train_都道府県コードV_list: numpy array of 都道府県コード
        '''
        #if idx == 0:
        #    self.logger.info('on_epoch_start batch_size:%d,length:%d,num_batches_per_epoch:%d' % (self._batch_size, self._length, self._num_batches_per_epoch))

        start_pos = self._batch_size * idx
        end_pos = start_pos + self._batch_size
        if end_pos > self._length:
            end_pos = self._length

        #self.logger.info('idx:%07d start_pos:%07d end_pos:%07d' % (idx, start_pos, end_pos))

        data = ({
            'addr_nm_input': self._addr_nm_input[start_pos:end_pos],
            'tdfkn_cd_input': self._tdfkn_cd_input[start_pos:end_pos],
            }, {
            'scyosn_cd_output': self._scyosn_cd_output[start_pos:end_pos],
            })
        return data

    def __len__(self) -> int:
        '''
        Batch length
        '''
        return self._num_batches_per_epoch

    def on_epoch_end(self) -> None:
        '''
        Task when end of epoch
        '''
        #self.logger.info('on_epoch_end batch_size:%d,length:%d,num_batches_per_epoch:%d' % (self._batch_size, self._length, self._num_batches_per_epoch))
