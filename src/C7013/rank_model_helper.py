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

# プロジェクトライブラリインポート
from . import const
from . import message
from . import utils
from . import rank_utils
from .C7013_04_rank_flag_forced_correction_task import C7013_04_rank_flag_forced_correction_task 

class RankModelHelper(object):
    '''
    ランク判定モデルを構築するためのHelperクラスです。
    '''

    def __init__(self):
        '''
        初期化関数
        '''

        self._logger: logging.Logger = utils.getLogger()
        self._rank_a_model: models.Model = None
        self._rank_b_model: models.Model = None
        self._rank_c_model: models.Model = None
        self._rank_d_model: models.Model = None
        self._rank_bar_model: models.Model = None
        self._history_a: callbacks.History = None
        self._history_b: callbacks.History = None
        self._history_c: callbacks.History = None
        self._history_d: callbacks.History = None
        self._history_bar: callbacks.History = None

    @property
    def logger(self) -> logging.Logger:
        '''
        ロガー
        '''

        return self._logger

    def _create_binary_crossentropy_model(self, model_name: str) -> models.Model:
        '''
        ランク判定用2分類モデルを構築します。

        Args:
            model_name: モデル名称
        Return:
            2分類モデル
        '''

        rank_input_layer = layers.Input(name='rank_input', shape=(83, ), dtype='int32')
        middle_layer = layers.Dense(name='middle_layer1', units=55, activation='relu')(rank_input_layer)
        #middle_layer = layers.Dropout(name='middle_layer2', rate=0.01)(middle_layer)
        output_layer = layers.Dense(name='rank_output', units=1, activation="sigmoid")(middle_layer)
        return models.Model(name = model_name, inputs = rank_input_layer, outputs = output_layer)

    def assembly_model(self) -> None:
        '''
        ランク判定用モデルを構築します。

        '''

        self._rank_a_model = self._create_binary_crossentropy_model('rank_a_model')
        self._rank_a_model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
        self._rank_a_model.summary()

        self._rank_b_model = self._create_binary_crossentropy_model('rank_b_model')
        self._rank_b_model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
        self._rank_b_model.summary()

        self._rank_c_model = self._create_binary_crossentropy_model('rank_c_model')
        self._rank_c_model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
        self._rank_c_model.summary()

        self._rank_d_model = self._create_binary_crossentropy_model('rank_d_model')
        self._rank_d_model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
        self._rank_d_model.summary()

        self._rank_bar_model = self._create_binary_crossentropy_model('rank_bar_model')
        self._rank_bar_model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])        
        self._rank_bar_model.summary()

    def training_callbacks(self) -> list:
        '''
        学習時のコールバック関数
        '''

        # val_lossが4回以上改善されない場合、学習を終了する
        early_stop = callbacks.EarlyStopping(monitor='val_loss', patience=4)

        return [early_stop]

    def fit_model_from_file(self, train_file_path: pathlib.PurePath, val_file_path: pathlib.PurePath, batch_size: int, epochs: int, workers: int = 0) -> None:
        '''
        ファイルからランク判定モデルを訓練します。

        Args:
            train_file_path: 学習用ファイルのパス
            val_file_path: 検証用ファイルのパス
            batch_size: batch_size
            epochs: 学習回数
            workers: workers
        '''

        self._fit_a_model_from_file(train_file_path, val_file_path, batch_size, epochs, workers)
        self._fit_b_model_from_file(train_file_path, val_file_path, batch_size, epochs, workers)
        self._fit_c_model_from_file(train_file_path, val_file_path, batch_size, epochs, workers)
        self._fit_d_model_from_file(train_file_path, val_file_path, batch_size, epochs, workers)
        self._fit_bar_model_from_file(train_file_path, val_file_path, batch_size, epochs, workers)

    def _fit_a_model_from_file(self, train_file_path: pathlib.PurePath, val_file_path: pathlib.PurePath, batch_size: int, epochs: int, workers: int = 0) -> None:
        '''
        ファイルからAランク判定モデルを訓練します。

        Args:
            train_file_path: 学習用ファイルのパス
            val_file_path: 検証用ファイルのパス
            batch_size: batch_size
            epochs: 学習回数
            workers: workers
        '''
        self.logger.debug(f'Aランク判定用モデルを学習します。')
        train_gen_a = RankTrainingGenerator('A', train_file_path, batch_size=batch_size)
        val_gen_a = RankTrainingGenerator('A', val_file_path, batch_size=batch_size)
        self._history_a = self._rank_a_model.fit(
            train_gen_a,
            steps_per_epoch=train_gen_a.num_batches_per_epoch,
            validation_data=val_gen_a,
            validation_steps=val_gen_a.num_batches_per_epoch,
            epochs=epochs,
            callbacks=self.training_callbacks(),
            workers=workers)

    def _fit_b_model_from_file(self, train_file_path: pathlib.PurePath, val_file_path: pathlib.PurePath, batch_size: int, epochs: int, workers: int = 0) -> None:
        '''
        ファイルからBランク判定モデルを訓練します。

        Args:
            train_file_path: 学習用ファイルのパス
            val_file_path: 検証用ファイルのパス
            batch_size: batch_size
            epochs: 学習回数
            workers: workers
        '''
        self.logger.debug(f'Bランク判定用モデルを学習します。')
        train_gen_b = RankTrainingGenerator('B', train_file_path, batch_size=batch_size)
        val_gen_b = RankTrainingGenerator('B', val_file_path, batch_size=batch_size)
        self._history_b = self._rank_b_model.fit(
            train_gen_b,
            steps_per_epoch=train_gen_b.num_batches_per_epoch,
            validation_data=val_gen_b,
            validation_steps=val_gen_b.num_batches_per_epoch,
            epochs=epochs,
            callbacks=self.training_callbacks(),
            workers=workers)

    def _fit_c_model_from_file(self, train_file_path: pathlib.PurePath, val_file_path: pathlib.PurePath, batch_size: int, epochs: int, workers: int = 0) -> None:
        '''
        ファイルからCランク判定モデルを訓練します。

        Args:
            train_file_path: 学習用ファイルのパス
            val_file_path: 検証用ファイルのパス
            batch_size: batch_size
            epochs: 学習回数
            workers: workers
        '''
        self.logger.debug(f'Cランク判定用モデルを学習します。')
        train_gen_c = RankTrainingGenerator('C', train_file_path, batch_size=batch_size)
        val_gen_c = RankTrainingGenerator('C', val_file_path, batch_size=batch_size)
        self._history_c = self._rank_c_model.fit(
            train_gen_c,
            steps_per_epoch=train_gen_c.num_batches_per_epoch,
            validation_data=val_gen_c,
            validation_steps=val_gen_c.num_batches_per_epoch,
            epochs=epochs,
            callbacks=self.training_callbacks(),
            workers=workers)

    def _fit_d_model_from_file(self, train_file_path: pathlib.PurePath, val_file_path: pathlib.PurePath, batch_size: int, epochs: int, workers: int = 0) -> None:
        '''
        ファイルからDランク判定モデルを訓練します。

        Args:
            train_file_path: 学習用ファイルのパス
            val_file_path: 検証用ファイルのパス
            batch_size: batch_size
            epochs: 学習回数
            workers: workers
        '''
        self.logger.debug(f'Dランク判定用モデルを学習します。')
        train_gen_d = RankTrainingGenerator('D', train_file_path, batch_size=batch_size)
        val_gen_d = RankTrainingGenerator('D', val_file_path, batch_size=batch_size)
        self._history_d = self._rank_d_model.fit(
            train_gen_d,
            steps_per_epoch=train_gen_d.num_batches_per_epoch,
            validation_data=val_gen_d,
            validation_steps=val_gen_d.num_batches_per_epoch,
            epochs=epochs,
            callbacks=self.training_callbacks(),
            workers=workers)

    def _fit_bar_model_from_file(self, train_file_path: pathlib.PurePath, val_file_path: pathlib.PurePath, batch_size: int, epochs: int, workers: int = 0) -> None:
        '''
        ファイルから-ランク判定モデルを訓練します。

        Args:
            train_file_path: 学習用ファイルのパス
            val_file_path: 検証用ファイルのパス
            batch_size: batch_size
            epochs: 学習回数
            workers: workers
        '''
        self.logger.debug(f'-ランク判定用モデルを学習します。')
        train_gen_bar = RankTrainingGenerator('-', train_file_path, batch_size=batch_size)
        val_gen_bar = RankTrainingGenerator('-', val_file_path, batch_size=batch_size)
        self._history_bar = self._rank_bar_model.fit(
            train_gen_bar,
            steps_per_epoch=train_gen_bar.num_batches_per_epoch,
            validation_data=val_gen_bar,
            validation_steps=val_gen_bar.num_batches_per_epoch,
            epochs=epochs,
            callbacks=self.training_callbacks(),
            workers=workers)

    def load_models(self, load_path: pathlib.PurePath = None) -> None:
        '''
        ランク判定用モデルをロードします。

        Args:
            load_path: ロードするフォルダのパス、指定しない場合はモデルパスからロードします。
        '''
        if not load_path:
            load_path = const.APP_MODEL_PATH

        self.logger.info(f'ランク判定用モデルを読み込みます load_path={load_path}')

        self._rank_a_model = models.load_model(load_path / "rank_a_model.h5", compile=True)

        self._rank_b_model = models.load_model(load_path / "rank_b_model.h5", compile=True)

        self._rank_c_model = models.load_model(load_path / "rank_c_model.h5", compile=True)

        self._rank_d_model = models.load_model(load_path / "rank_d_model.h5", compile=True)

        self._rank_bar_model = models.load_model(load_path / "rank_bar_model.h5", compile=True)

    def save_models(self, save_path: pathlib.PurePath = None) -> None:
        '''
        ランク判定用モデルを保存します。

        Args:
            save_path: 保存するフォルダのパス、指定しない場合はデータパスに保存します。
        '''

        if not save_path:
            save_path = const.APP_DATA_PATH

        self.logger.debug(f'ランク判定用モデルを保存します save_path={save_path}')

        self._rank_a_model.save(save_path / "rank_a_model.h5", include_optimizer=True)
        plot_model(self._rank_a_model, to_file=save_path / 'rank_a_model.png', show_shapes=True, show_layer_names=True)

        self._rank_b_model.save(save_path / "rank_b_model.h5", include_optimizer=True)
        plot_model(self._rank_b_model, to_file=save_path / 'rank_b_model.png', show_shapes=True, show_layer_names=True)

        self._rank_c_model.save(save_path / "rank_c_model.h5", include_optimizer=True)
        plot_model(self._rank_c_model, to_file=save_path / 'rank_c_model.png', show_shapes=True, show_layer_names=True)

        self._rank_d_model.save(save_path / "rank_d_model.h5", include_optimizer=True)
        plot_model(self._rank_d_model, to_file=save_path / 'rank_d_model.png', show_shapes=True, show_layer_names=True)

        self._rank_bar_model.save(save_path / "rank_bar_model.h5", include_optimizer=True)
        plot_model(self._rank_bar_model, to_file=save_path / 'rank_bar_model.png', show_shapes=True, show_layer_names=True)

        # モデルファイルを保存しました。保存先＝%s
        self.logger.info(message.MSG['MSG0010'], save_path)

    def save_training_accuracies_and_losses(self) -> None:
        '''
        訓練と検証正確度をイメージで保存します。
        '''
        utils.save_training_accuracy_and_loss(self._history_a, 'a_rankmodel_training_accuracy_and_loss.png')
        utils.save_training_accuracy_and_loss(self._history_b, 'b_rankmodel_training_accuracy_and_loss.png')
        utils.save_training_accuracy_and_loss(self._history_c, 'c_rankmodel_training_accuracy_and_loss.png')
        utils.save_training_accuracy_and_loss(self._history_d, 'd_rankmodel_training_accuracy_and_loss.png')
        utils.save_training_accuracy_and_loss(self._history_bar, 'bar_rankmodel_training_accuracy_and_loss.png')

    def save_training_and_validation_losses(self) -> None:
        '''
        訓練と検証損失をイメージで保存します。
        '''
        utils.save_training_and_validation_loss(self._history_a, 'a_rankmodel_training_and_validation_loss.png')
        utils.save_training_and_validation_loss(self._history_b, 'b_rankmodel_training_and_validation_loss.png')
        utils.save_training_and_validation_loss(self._history_c, 'c_rankmodel_training_and_validation_loss.png')
        utils.save_training_and_validation_loss(self._history_d, 'd_rankmodel_training_and_validation_loss.png')
        utils.save_training_and_validation_loss(self._history_bar, 'bar_rankmodel_training_and_validation_loss.png')

    def predict(self, input_data: np.array, batch_size: int = 1, verbose: int = 1) -> np.array:
        '''
        ランクを予測します。

        Args:
            input_data: 検証データ
            batch_size: バッチサイズ
            verbose: 詳細ログを出力する場合、1

        Returns:
            ランク予測データ
        '''

        rank_a_predict = self.predict_a(input_data, batch_size, verbose)
        rank_b_predict = self.predict_b(input_data, batch_size, verbose)
        rank_c_predict = self.predict_c(input_data, batch_size, verbose)
        rank_d_predict = self.predict_d(input_data, batch_size, verbose)
        rank_bar_predict = self.predict_bar(input_data, batch_size, verbose)

        result = rank_utils.decide_rank(rank_a_predict, rank_b_predict, rank_c_predict, rank_d_predict, rank_bar_predict)
        
        return result

    def predict_a(self, input_data: np.array, batch_size: int = 1, verbose: int = 1) -> np.array:
        '''
        Aランク判定モデルの検証を実行します。

        Args:
            input_data: 検証データ
            batch_size: バッチサイズ
            verbose: 詳細ログを出力する場合、1

        Returns:
            Aランク予測結果
        '''
        return np.round(self._rank_a_model.predict(input_data, batch_size=batch_size, verbose=verbose)).astype(np.int32)

    def evaluate_a(self, input_data: np.array, target_data: np.array, batch_size: int = 1, verbose: int = 1) -> tuple:
        '''
        Bランク判定モデルの精度を測定します。

        Args:
            input_data: 入力データ
            target_data: 対象データ
            batch_size: バッチサイズ
            verbose: 詳細ログを出力する場合、1

        Returns:
            loss, acc
        '''

        test_loss, test_acc = self._rank_a_model.evaluate(input_data, target_data, batch_size=batch_size, verbose=verbose)
        return (test_loss, test_acc)

    def predict_b(self, input_data: np.array, batch_size: int = 1, verbose: int = 1) -> np.array:
        '''
        Bランク判定モデルの検証を実行します。

        Args:
            input_data: 検証データ
            batch_size: バッチサイズ
            verbose: 詳細ログを出力する場合、1

        Returns:
            Bランク予測結果
        '''
        return np.round(self._rank_b_model.predict(input_data, batch_size=batch_size, verbose=verbose)).astype(np.int32)

    def evaluate_b(self, input_data: np.array, target_data: np.array, batch_size: int = 1, verbose: int = 1) -> tuple:
        '''
        Bランク判定モデルの精度を測定します。

        Args:
            input_data: 入力データ
            target_data: 対象データ
            batch_size: バッチサイズ
            verbose: 詳細ログを出力する場合、1

        Returns:
            loss, acc
        '''

        test_loss, test_acc = self._rank_b_model.evaluate(input_data, target_data, batch_size=batch_size, verbose=verbose)
        return (test_loss, test_acc)

    def predict_c(self, input_data: np.array, batch_size: int = 1, verbose: int = 1) -> np.array:
        '''
        Cランク判定モデルの検証を実行します。

        Args:
            input_data: 検証データ
            batch_size: バッチサイズ
            verbose: 詳細ログを出力する場合、1

        Returns:
            Cランク予測結果
        '''
        return np.round(self._rank_c_model.predict(input_data, batch_size=batch_size, verbose=verbose)).astype(np.int32)

    def evaluate_c(self, input_data: np.array, target_data: np.array, batch_size: int = 1, verbose: int = 1) -> tuple:
        '''
        Cランク判定モデルの精度を測定します。

        Args:
            input_data: 入力データ
            target_data: 対象データ
            batch_size: バッチサイズ
            verbose: 詳細ログを出力する場合、1

        Returns:
            loss, acc
        '''

        test_loss, test_acc = self._rank_c_model.evaluate(input_data, target_data, batch_size=batch_size, verbose=verbose)
        return (test_loss, test_acc)

    def predict_d(self, input_data: np.array, batch_size: int = 1, verbose: int = 1) -> np.array:
        '''
        Dランク判定モデルの検証を実行します。

        Args:
            input_data: 検証データ
            batch_size: バッチサイズ
            verbose: 詳細ログを出力する場合、1
        
        Returns:
            Dランク予測結果
        '''
        return np.round(self._rank_d_model.predict(input_data, batch_size=batch_size, verbose=verbose)).astype(np.int32)

    def evaluate_d(self, input_data: np.array, target_data: np.array, batch_size: int = 1, verbose: int = 1) -> tuple:
        '''
        Dランク判定モデルの精度を測定します。

        Args:
            input_data: 入力データ
            target_data: 対象データ
            batch_size: バッチサイズ
            verbose: 詳細ログを出力する場合、1

        Returns:
            loss, acc
        '''

        test_loss, test_acc = self._rank_d_model.evaluate(input_data, target_data, batch_size=batch_size, verbose=verbose)
        return (test_loss, test_acc)

    def predict_bar(self, input_data: np.array, batch_size: int = 1, verbose: int = 1) -> np.array:
        '''
        -ランク判定モデルの検証を実行します。

        Args:
            input_data: 検証データ
            batch_size: バッチサイズ
            verbose: 詳細ログを出力する場合、1

        Returns:
            -ランク予測結果
        '''
        return np.round(self._rank_bar_model.predict(input_data, batch_size=batch_size, verbose=verbose)).astype(np.int32)

    def evaluate_bar(self, input_data: np.array, target_data: np.array, batch_size: int = 1, verbose: int = 1) -> tuple:
        '''
        -ランク判定モデルの精度を測定します。

        Args:
            input_data: 入力データ
            target_data: 対象データ
            batch_size: バッチサイズ
            verbose: 詳細ログを出力する場合、1
        
        Returns:
            loss, acc
        '''

        test_loss, test_acc = self._rank_bar_model.evaluate(input_data, target_data, batch_size=batch_size, verbose=verbose)
        return (test_loss, test_acc)


class RankTrainingGenerator(Sequence):
    '''
    ランク training generatorクラスV2
    '''

    def __init__(self, rank_type: str, file_path: str, batch_size: int = 1):
        '''
        初期化関数

        Args:
            rank_type: rank_type
            file_path: rank training filepath
            batch_size: Batch size
        '''

        # ロガー
        self._logger: logging.Logger = utils.getLogger()
        self._rank_type: str = rank_type
        self._file_path: str = file_path
        self._batch_size: int = batch_size
        input_data = pd.read_csv(file_path, sep=',', encoding='utf-8')
        if input_data.empty:
            raise RuntimeError(f"データが存在しません。file_path={file_path}")
        flag_forced_correction = C7013_04_rank_flag_forced_correction_task()
        self._input_data: pd.DataFrame = flag_forced_correction.execute(input_data)

        self._length: int = len(self._input_data)

        self._num_batches_per_epoch: int = int((self._length - 1) / batch_size) + 1

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
        :return rank_input: numpy array of rank_flag_data
        :return rank_output: numpy array of rank_data
        '''
        #if idx == 0:
        #    self.logger.info('on_epoch_start batch_size:%d,length:%d,num_batches_per_epoch:%d' % (self._batch_size, self._length, self._num_batches_per_epoch))

        start_pos = self._batch_size * idx
        end_pos = start_pos + self._batch_size
        if end_pos > self._length:
            end_pos = self._length

        #self.logger.info('idx:%07d start_pos:%07d end_pos:%07d' % (idx, start_pos, end_pos))

        rows = self._input_data[start_pos:end_pos]

        input_one_hot = rank_utils.input_data_transform(rows)
        target = rank_utils.target_data_transform(rows)
        if self._rank_type == 'A':
            target_one_hot = target[:, 0:1]
        elif self._rank_type == 'B':
            target_one_hot = target[:, 1:2]
        elif self._rank_type == 'C':
            target_one_hot = target[:, 2:3]
        elif self._rank_type == 'D':
            target_one_hot = target[:, 3:4]
        else:
            target_one_hot = target[:, 4:5]

        return {
            'rank_input': input_one_hot
            }, {
            'rank_output': target_one_hot
            }

    def __len__(self) -> int:
        '''
        Batch length
        '''
        return self.num_batches_per_epoch

    def on_epoch_end(self) -> None:
        '''
        Task when end of epoch
        '''
        #self.logger.info('on_epoch_end batch_size:%d,length:%d,num_batches_per_epoch:%d' % (self._batch_size, self._length, self._num_batches_per_epoch))
