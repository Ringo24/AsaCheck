"""
models.py
住所コード変換関連Models
"""

#ログ関連設定
import os
import pathlib
import logging.config
logging.config.fileConfig(pathlib.Path(__file__).parent / 'config/logging.ini')
logger = logging.getLogger("bizmerge")

import numpy as np

#AIに必要なものをインポート
from keras import layers
from keras import models
from keras import utils
from keras.utils import np_utils
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt

from utils import AddressCodeBinarizer, TextToArrays
from traning_generators import TDFKN_CD_generator, SCYOSN_CD_generator, OAZA_TSHUM_CD_generator, AZCHM_CD_generator, ZIPCODE_generator

class TDFKN_CD_Classifier:
    """
    都道府県コード分類モデルクラス
    """

    lb:AddressCodeBinarizer = None
    t2a:TextToArrays = None
    model:models.Model = None
    history = None

    def __init__(self, lb:AddressCodeBinarizer, t2a:TextToArrays):
        self.lb = lb
        self.t2a = t2a

    def assemble_model(self)->None:
        """ モデル定義 """

        #入力を定義
        addressname_input = layers.Input(name='addressname_input', shape=(10, 100, ), dtype='float32')

        #中間層を定義
        x = layers.LSTM(name='addressname_lstm_1', units=300)(addressname_input)
        #Dropoutを利用して過剰適合を回避する
        x = layers.Dropout(name='addressname_dropout_1', rate = 0.1)(x)

        #出力を定義
        x = layers.Dense(name='tdfkn_cd_output', units=self.lb.TDFKN_CD_LEN, activation='softmax')(x)

        #モデル定義とコンパイル
        self.model = models.Model(name='tdfkn_cd_classification_model', inputs=addressname_input, outputs=x)
        self.model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])
        self.model.summary()

    def fit_from_file(self, train_file_path:str, val_file_path:str, batch_size:int, epochs:int):
        """ モデル訓練 """
        #logger.info('都道府県コード分類モデルを訓練します')
        train_gen = TDFKN_CD_generator(self.lb, self.t2a, train_file_path, batch_size=batch_size)
        val_gen = TDFKN_CD_generator(self.lb, self.t2a, val_file_path, batch_size=batch_size)
        self.history = self.model.fit_generator(
            train_gen,
            steps_per_epoch=train_gen.num_batches_per_epoch,
            validation_data=val_gen,
            validation_steps=val_gen.num_batches_per_epoch,
            epochs=epochs,
            workers=0)
        return self.history

    def save_model(self)->None:
        """ モデル保存 """
        model_json_str = self.model.to_json()
        open(pathlib.Path(__file__).parent / 'model/02_都道府県コード予測.Model', 'w').write(model_json_str)
        self.model.save_weights(pathlib.Path(__file__).parent / 'model/02_都道府県コード予測.Weights')
        utils.plot_model(self.model, to_file=pathlib.Path(__file__).parent / 'model/02_都道府県コード予測.png', show_shapes=True)

    def save_training_accuracy(self)->None:
        #訓練の正確度と損失グラフを作成する
        history_dict = self.history.history

        acc = history_dict['accuracy']
        val_acc = history_dict['val_accuracy']
        loss = history_dict['loss']

        epochs = range(1, len(loss) + 1)

        #訓練と検証正確度をイメージで保存
        plt.plot(epochs, acc, 'bo', label='Training acc')      #bo:青い点線
        plt.plot(epochs, val_acc, 'b', label='Validation acc') #b:青い実線
        plt.title('Training and validation accuracy')
        plt.xlabel('Epochs')
        plt.ylabel('Accuracy')
        plt.legend()
        plt.savefig(pathlib.Path(__file__).parent / 'Model/02_都道府県コード予測_訓練と検証正確度.png')

    def save_training_loss(self)->None:
        #訓練の正確度と損失グラフを作成する
        history_dict = self.history.history

        loss = history_dict['loss']
        val_loss = history_dict['val_loss']

        epochs = range(1, len(loss) + 1)

        #訓練と検証損失をイメージで保存
        plt.plot(epochs, loss, 'bo', label='Training loss')      #bo:青い点線
        plt.plot(epochs, val_loss, 'b', label='Validation loss') #b:青い実線
        plt.title('Training and validation loss')
        plt.xlabel('Epochs')
        plt.ylabel('Loss')
        plt.legend()
        plt.savefig(pathlib.Path(__file__).parent / 'Model/02_都道府県コード予測_訓練と検証損失.png')

    def load_model(self):
        logger.info('保存した都道府県コードモデルを読み込みます')
        #モデルを読み込む
        self.model = models.model_from_json(open(pathlib.Path(__file__).parent / 'model/02_都道府県コード予測.Model').read())

        #学習結果を読み込む
        self.model.load_weights(pathlib.Path(__file__).parent / 'model/02_都道府県コード予測.Weights')

        #モデルコンパイル
        self.model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])

    def predict(self, address_list, batch_size=1, verbose=1):
        logger.info('都道府県コードを予測します')
        address_listV = np.array([self.t2a.word2vec(s) for s in address_list])
        predV = self.model.predict(x=address_listV, batch_size=batch_size, verbose=verbose)
        return self.lb.inverse_transform_TDFKN_CD(predV)

    def predict_from_file(self, file_path, batch_size=1, verbose=1):
        logger.info('都道府県コードを予測します')
        predict_gen = TDFKN_CD_generator(self.lb, self.t2a, file_path, batch_size=batch_size)
        return self.model.predict_generator(predict_gen, batch_size=batch_size, workers=0, verbose=verbose)

    @property
    def metrics_names(self):
        return self.model.metrics_names

    def evaluate_model(self, address_list, tdfkn_cd_list, batch_size=1):
        address_listV = np.array([self.t2a.word2vec(s) for s in address_list])
        tdfkn_cd_listV = self.lb.transform_TDFKN_CD(tdfkn_cd_list)
        return self.model.evaluate(x=address_listV, y=tdfkn_cd_listV, batch_size=batch_size)

class SCYOSN_CD_Classifier:
    """
    市区町村コード分類モデルクラス
    """

    lb:AddressCodeBinarizer = None
    t2a:TextToArrays = None
    model:models.Model = None
    history = None

    def __init__(self, lb:AddressCodeBinarizer, t2a:TextToArrays):
        self.lb = lb
        self.t2a = t2a

    def assemble_model(self)->None:
        """ モデル定義 """

        #入力を定義
        addressname_input = layers.Input(name='addressname_input', shape=(10, 100, ), dtype='float32')
        tdfkn_cd_input = layers.Input(name='tdfkn_cd_input', shape=(self.lb.TDFKN_CD_LEN, ), dtype='float32')

        #中間層を定義
        x = layers.LSTM(name='addressname_lstm_1', units=1000)(addressname_input)
        x = models.Model(inputs=addressname_input, outputs=x)

        y = layers.Dense(name='tdfkn_cd_dense_1', units=48, activation="relu")(tdfkn_cd_input)
        y = models.Model(inputs=tdfkn_cd_input, outputs=y)

        #結合
        combined = layers.concatenate(inputs=[x.output, y.output])

        #密結合
        z = layers.Dense(units=848, activation='tanh')(combined)
        #Dropoutを利用して過剰適合を回避する
        z = layers.Dropout(rate=0.1)(z)

        #出力を定義
        z = layers.Dense(name='scyosn_cd_output', units=self.lb.SCYOSN_CD_LEN, activation='softmax')(z)

        #モデル定義とコンパイル
        self.model = models.Model(name='scyosn_cd_classification_model', inputs=[x.input, y.input], outputs=z)
        self.model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])
        self.model.summary()

    def fit_from_file(self, train_file_path:str, val_file_path:str, batch_size:int, epochs:int):
        """ モデル訓練 """
        logger.info('市区町村コード分類モデルを訓練します')
        train_gen = SCYOSN_CD_generator(self.lb, self.t2a, train_file_path, batch_size=batch_size)
        val_gen = SCYOSN_CD_generator(self.lb, self.t2a, val_file_path, batch_size=batch_size)
        self.history = self.model.fit_generator(
            train_gen,
            steps_per_epoch=train_gen.num_batches_per_epoch,
            validation_data=val_gen,
            validation_steps=val_gen.num_batches_per_epoch,
            epochs=epochs,
            workers=0)
        return self.history

    def save_model(self)->None:
        """ モデル保存 """
        model_json_str = self.model.to_json()
        open(pathlib.Path(__file__).parent / 'model/04_市区町村コード予測.Model', 'w').write(model_json_str)
        self.model.save_weights(pathlib.Path(__file__).parent / 'model/04_市区町村コード予測.Weights')
        utils.plot_model(self.model, to_file=pathlib.Path(__file__).parent / 'model/04_市区町村コード予測.png', show_shapes=True)

    def save_training_accuracy(self)->None:
        #訓練の正確度と損失グラフを作成する
        history_dict = self.history.history

        acc = history_dict['accuracy']
        val_acc = history_dict['val_accuracy']
        loss = history_dict['loss']

        epochs = range(1, len(loss) + 1)

        #訓練と検証正確度をイメージで保存
        plt.plot(epochs, acc, 'bo', label='Training acc')      #bo:青い点線
        plt.plot(epochs, val_acc, 'b', label='Validation acc') #b:青い実線
        plt.title('Training and validation accuracy')
        plt.xlabel('Epochs')
        plt.ylabel('Accuracy')
        plt.legend()
        plt.savefig(pathlib.Path(__file__).parent / 'Model/04_市区町村コード予測_訓練と検証正確度.png')

    def save_training_loss(self)->None:
        #訓練の正確度と損失グラフを作成する
        history_dict = self.history.history

        loss = history_dict['loss']
        val_loss = history_dict['val_loss']

        epochs = range(1, len(loss) + 1)

        #訓練と検証損失をイメージで保存
        plt.plot(epochs, loss, 'bo', label='Training loss')      #bo:青い点線
        plt.plot(epochs, val_loss, 'b', label='Validation loss') #b:青い実線
        plt.title('Training and validation loss')
        plt.xlabel('Epochs')
        plt.ylabel('Loss')
        plt.legend()
        plt.savefig(pathlib.Path(__file__).parent / 'Model/04_市区町村コード予測_訓練と検証損失.png')

    def load_model(self):
        logger.info('保存した市区町村コードモデルを読み込みます')
        #モデルを読み込む
        self.model = models.model_from_json(open(pathlib.Path(__file__).parent / 'model/04_市区町村コード予測.Model').read())

        #学習結果を読み込む
        self.model.load_weights(pathlib.Path(__file__).parent / 'model/04_市区町村コード予測.Weights')

        #モデルコンパイル
        self.model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])

    def predict(self, address_list, tdfkn_cd_list, batch_size=1, verbose=1):
        logger.info('市区町村コードを予測します')
        address_listV = np.array([self.t2a.word2vec(s) for s in address_list])
        tdfkn_cd_listV = self.lb.transform_TDFKN_CD(tdfkn_cd_list)
        predV = self.model.predict(x=[address_listV, tdfkn_cd_listV], batch_size=batch_size, verbose=verbose)
        return self.lb.inverse_transform_SCYOSN_CD(predV)

    def predict_file(self, file_path, batch_size=1, verbose=1):
        logger.info('市区町村コードを予測します')
        predict_gen = SCYOSN_CD_generator(self.lb, self.t2a, file_path, batch_size=batch_size)
        return self.model.predict_generator(predict_gen, batch_size=batch_size, workers=0, verbose=verbose)

    @property
    def metrics_names(self):
        return self.model.metrics_names

    def evaluate_model(self, address_list, tdfkn_cd_list, scyosn_cd_list, batch_size=1):
        address_listV = np.array([self.t2a.word2vec(s) for s in address_list])
        tdfkn_cd_listV = self.lb.transform_TDFKN_CD(tdfkn_cd_list)
        scyosn_cd_listV = self.lb.transform_SCYOSN_CD(scyosn_cd_list)
        return self.model.evaluate(x=[address_listV, tdfkn_cd_listV], y=scyosn_cd_listV, batch_size=batch_size)

class OAZA_TSHUM_CD_Classifier:
    """
    大字通称コード分類モデルクラス
    """

    lb:AddressCodeBinarizer = None
    t2a:TextToArrays = None
    model:models.Model = None
    history = None

    def __init__(self, lb:AddressCodeBinarizer, t2a:TextToArrays):
        self.lb = lb
        self.t2a = t2a

    def assemble_model(self)->None:
        """ モデル定義 """

        #入力を定義
        addressname_input = layers.Input(name='addressname_input', shape=(10, 100, ), dtype='float32')
        tdfkn_cd_input = layers.Input(name='tdfkn_cd_input', shape=(self.lb.TDFKN_CD_LEN, ), dtype='float32')
        scyosn_cd_input = layers.Input(name='scyosn_cd_input', shape=(self.lb.SCYOSN_CD_LEN,), dtype='float32')

        #中間層を定義
        x = layers.LSTM(name='addressname_lstm_1', units=1000)(addressname_input)
        x = models.Model(inputs=addressname_input, outputs=x)

        y1 = layers.Dense(name='tdfkn_cd_dense_1', units=48, activation="relu")(tdfkn_cd_input)
        y1 = models.Model(inputs=tdfkn_cd_input, outputs=y1)

        y2 = layers.Dense(name='scyosn_cd_dense_1', units=426, activation="relu")(scyosn_cd_input)
        y2 = models.Model(inputs=scyosn_cd_input, outputs=y2)

        #結合
        combined = layers.concatenate([x.output, y1.output, y2.output])

        #密結合
        z = layers.Dense(units=1300, activation='tanh')(combined)
        #Dropoutを利用して過剰適合を回避する
        z = layers.Dropout(rate=0.1)(z)

        #出力を定義
        z = layers.Dense(name='oaza_tshum_cd_output', units=self.lb.OAZA_TSHUM_CD_LEN, activation='softmax')(z)

        #モデル定義とコンパイル
        self.model = models.Model(name='oaza_tshum_cd_classification_model', inputs=[x.input, y1.input, y2.input], outputs=z)
        self.model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])
        self.model.summary()

    def fit_from_file(self, train_file_path:str, val_file_path:str, batch_size:int, epochs:int):
        """ モデル訓練 """
        logger.info('大字通称コード分類モデルを訓練します')
        train_gen = OAZA_TSHUM_CD_generator(self.lb, self.t2a, train_file_path, batch_size=batch_size)
        val_gen = OAZA_TSHUM_CD_generator(self.lb, self.t2a, val_file_path, batch_size=batch_size)
        self.history = self.model.fit_generator(
            train_gen,
            steps_per_epoch=train_gen.num_batches_per_epoch,
            validation_data=val_gen,
            validation_steps=val_gen.num_batches_per_epoch,
            epochs=epochs,
            workers=0)
        return self.history

    def save_model(self)->None:
        """ モデル保存 """
        model_json_str = self.model.to_json()
        open(pathlib.Path(__file__).parent / 'model/06_大字通称コード予測.Model', 'w').write(model_json_str)
        self.model.save_weights(pathlib.Path(__file__).parent / 'model/06_大字通称コード予測.Weights')
        utils.plot_model(self.model, to_file=pathlib.Path(__file__).parent / 'model/06_大字通称コード予測.png', show_shapes=True)

    def save_training_accuracy(self)->None:
        #訓練の正確度と損失グラフを作成する
        history_dict = self.history.history

        acc = history_dict['accuracy']
        val_acc = history_dict['val_accuracy']
        loss = history_dict['loss']

        epochs = range(1, len(loss) + 1)

        #訓練と検証正確度をイメージで保存
        plt.plot(epochs, acc, 'bo', label='Training acc')      #bo:青い点線
        plt.plot(epochs, val_acc, 'b', label='Validation acc') #b:青い実線
        plt.title('Training and validation accuracy')
        plt.xlabel('Epochs')
        plt.ylabel('Accuracy')
        plt.legend()
        plt.savefig(pathlib.Path(__file__).parent / 'Model/06_大字通称コード予測_訓練と検証正確度.png')

    def save_training_loss(self)->None:
        #訓練の正確度と損失グラフを作成する
        history_dict = self.history.history

        loss = history_dict['loss']
        val_loss = history_dict['val_loss']

        epochs = range(1, len(loss) + 1)

        #訓練と検証損失をイメージで保存
        plt.plot(epochs, loss, 'bo', label='Training loss')      #bo:青い点線
        plt.plot(epochs, val_loss, 'b', label='Validation loss') #b:青い実線
        plt.title('Training and validation loss')
        plt.xlabel('Epochs')
        plt.ylabel('Loss')
        plt.legend()
        plt.savefig(pathlib.Path(__file__).parent / 'Model/06_大字通称コード予測_訓練と検証損失.png')

    def load_model(self):
        logger.info('保存した大字通称コードモデルを読み込みます')
        #モデルを読み込む
        self.model = models.model_from_json(open(pathlib.Path(__file__).parent / 'model/06_大字通称コード予測.Model').read())

        #学習結果を読み込む
        self.model.load_weights(pathlib.Path(__file__).parent / 'model/06_大字通称コード予測.Weights')

        #モデルコンパイル
        self.model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])

    def predict(self, address_list, tdfkn_cd_list, scyosn_cd_list, batch_size=1, verbose=1):
        logger.info('大字通称コードを予測します')
        address_listV = np.array([self.t2a.word2vec(s) for s in address_list])
        tdfkn_cd_listV = self.lb.transform_TDFKN_CD(tdfkn_cd_list)
        scyosn_cd_listV = self.lb.transform_SCYOSN_CD(scyosn_cd_list)

        predV = self.model.predict(x=[address_listV, tdfkn_cd_listV, scyosn_cd_listV], batch_size=batch_size, verbose=verbose)
        return self.lb.inverse_transform_OAZA_TSHUM_CD(predV)

    def predict_file(self, file_path, batch_size=1, verbose=1):
        logger.info('大字通称コードを予測します')
        predict_gen = OAZA_TSHUM_CD_generator(self.lb, self.t2a, file_path, batch_size=batch_size)
        return self.model.predict_generator(predict_gen, batch_size=batch_size, workers=0, verbose=verbose)

    @property
    def metrics_names(self):
        return self.model.metrics_names

    def evaluate_model(self, address_list, tdfkn_cd_list, scyosn_cd_list, oaza_tshum_cd_list, batch_size=1):
        address_listV = np.array([self.t2a.word2vec(s) for s in address_list])
        tdfkn_cd_listV = self.lb.transform_TDFKN_CD(tdfkn_cd_list)
        scyosn_cd_listV = self.lb.transform_SCYOSN_CD(scyosn_cd_list)
        oaza_tshum_cd_listV = self.lb.transform_OAZA_TSHUM_CD(oaza_tshum_cd_list)
        return self.model.evaluate(x=[address_listV, tdfkn_cd_listV, scyosn_cd_listV], y=oaza_tshum_cd_listV, batch_size=batch_size)

class AZCHM_CD_Classifier:
    """
    字丁目コード分類モデルクラス
    """

    lb:AddressCodeBinarizer = None
    t2a:TextToArrays = None
    model:models.Model = None
    history = None

    def __init__(self, lb:AddressCodeBinarizer, t2a:TextToArrays):
        self.lb = lb
        self.t2a = t2a

    def assemble_model(self)->None:
        """ モデル定義 """

        #入力を定義
        addressname_input = layers.Input(name='addressname_input', shape=(10, 100, ), dtype='float32')
        tdfkn_cd_input = layers.Input(name='tdfkn_cd_input', shape=(self.lb.TDFKN_CD_LEN, ), dtype='float32')
        scyosn_cd_input = layers.Input(name='scyosn_cd_input', shape=(self.lb.SCYOSN_CD_LEN,), dtype='float32')
        oaza_tshum_cd_input = layers.Input(name='oaza_tshum_cd_input', shape=(self.lb.OAZA_TSHUM_CD_LEN,), dtype='float32')

        #中間層を定義
        x = layers.LSTM(name='addressname_lstm_1', units=1000)(addressname_input)
        x = models.Model(inputs=addressname_input, outputs=x)

        y1 = layers.Dense(name='tdfkn_cd_dense_1', units=48, activation="relu")(tdfkn_cd_input)
        y1 = models.Model(inputs=tdfkn_cd_input, outputs=y1)

        y2 = layers.Dense(name='scyosn_cd_dense_1', units=426, activation="relu")(scyosn_cd_input)
        y2 = models.Model(inputs=scyosn_cd_input, outputs=y2)

        y3 = layers.Dense(name='oaza_tshum_cd_dense_1', units=1095, activation="relu")(oaza_tshum_cd_input)
        y3 = models.Model(inputs=oaza_tshum_cd_input, outputs=y3)

        #結合
        combined = layers.concatenate([x.output, y1.output, y2.output, y3.output])

        #密結合
        z = layers.Dense(units=1420, activation='tanh')(combined)
        #Dropoutを利用して過剰適合を回避する
        z = layers.Dropout(rate=0.1)(z)

        #出力を定義
        z = layers.Dense(name='azchm_cd_output', units=self.lb.AZCHM_CD_LEN, activation='softmax')(z)

        #モデル定義とコンパイル
        self.model = models.Model(name='azchm_cd_classification_model', inputs=[x.input, y1.input, y2.input, y3.input], outputs=z)
        self.model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])
        self.model.summary()

    def fit_from_file(self, train_file_path:str, val_file_path:str, batch_size:int, epochs:int):
        """ モデル訓練 """
        logger.info('字丁目コード分類モデルを訓練します')
        train_gen = AZCHM_CD_generator(self.lb, self.t2a, train_file_path, batch_size=batch_size)
        val_gen = AZCHM_CD_generator(self.lb, self.t2a, val_file_path, batch_size=batch_size)
        self.history = self.model.fit_generator(
            train_gen,
            steps_per_epoch=train_gen.num_batches_per_epoch,
            validation_data=val_gen,
            validation_steps=val_gen.num_batches_per_epoch,
            epochs=epochs,
            workers=0)
        return self.history

    def save_model(self)->None:
        """ モデル保存 """
        model_json_str = self.model.to_json()
        open(pathlib.Path(__file__).parent / 'model/08_字丁目コード予測.Model', 'w').write(model_json_str)
        self.model.save_weights(pathlib.Path(__file__).parent / 'model/08_字丁目コード予測.Weights')
        utils.plot_model(self.model, to_file=pathlib.Path(__file__).parent / 'model/08_字丁目コード予測.png', show_shapes=True)

    def save_training_accuracy(self)->None:
        #訓練の正確度と損失グラフを作成する
        history_dict = self.history.history

        acc = history_dict['accuracy']
        val_acc = history_dict['val_accuracy']
        loss = history_dict['loss']

        epochs = range(1, len(loss) + 1)

        #訓練と検証正確度をイメージで保存
        plt.plot(epochs, acc, 'bo', label='Training acc')      #bo:青い点線
        plt.plot(epochs, val_acc, 'b', label='Validation acc') #b:青い実線
        plt.title('Training and validation accuracy')
        plt.xlabel('Epochs')
        plt.ylabel('Accuracy')
        plt.legend()
        plt.savefig(pathlib.Path(__file__).parent / 'Model/08_字丁目コード予測_訓練と検証正確度.png')

    def save_training_loss(self)->None:
        #訓練の正確度と損失グラフを作成する
        history_dict = self.history.history

        loss = history_dict['loss']
        val_loss = history_dict['val_loss']

        epochs = range(1, len(loss) + 1)

        #訓練と検証損失をイメージで保存
        plt.plot(epochs, loss, 'bo', label='Training loss')      #bo:青い点線
        plt.plot(epochs, val_loss, 'b', label='Validation loss') #b:青い実線
        plt.title('Training and validation loss')
        plt.xlabel('Epochs')
        plt.ylabel('Loss')
        plt.legend()
        plt.savefig(pathlib.Path(__file__).parent / 'Model/08_字丁目コード予測_訓練と検証損失.png')

    def load_model(self):
        logger.info('保存した字丁目コードモデルを読み込みます')
        #モデルを読み込む
        self.model = models.model_from_json(open(pathlib.Path(__file__).parent / 'model/08_字丁目コード予測.Model').read())

        #学習結果を読み込む
        self.model.load_weights(pathlib.Path(__file__).parent / 'model/08_字丁目コード予測.Weights')

        #モデルコンパイル
        self.model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])

    def predict(self, address_list, tdfkn_cd_list, scyosn_cd_list, oaza_tshum_cd_list, batch_size=1, verbose=1):
        logger.info('字丁目コードを予測します')
        address_listV = np.array([self.t2a.word2vec(s) for s in address_list])
        tdfkn_cd_listV = self.lb.transform_TDFKN_CD(tdfkn_cd_list)
        scyosn_cd_listV = self.lb.transform_SCYOSN_CD(scyosn_cd_list)
        oaza_tshum_cd_listV = self.lb.transform_OAZA_TSHUM_CD(oaza_tshum_cd_list)
        predV = self.model.predict(x=[address_listV, tdfkn_cd_listV, scyosn_cd_listV, oaza_tshum_cd_listV], batch_size=batch_size, verbose=verbose)
        return self.lb.inverse_transform_AZCHM_CD(predV)

    def predict_file(self, file_path, batch_size=1, verbose=1):
        logger.info('字丁目コードを予測します')
        predict_gen = AZCHM_CD_generator(self.lb, self.t2a, file_path, batch_size=batch_size)
        return self.model.predict_generator(predict_gen, batch_size=batch_size, workers=0, verbose=verbose)

    @property
    def metrics_names(self):
        return self.model.metrics_names

    def evaluate_model(self, address_list, tdfkn_cd_list, scyosn_cd_list, oaza_tshum_cd_list, azchm_cd_list, batch_size=1):
        address_listV = np.array([self.t2a.word2vec(s) for s in address_list])
        tdfkn_cd_listV = self.lb.transform_TDFKN_CD(tdfkn_cd_list)
        scyosn_cd_listV = self.lb.transform_SCYOSN_CD(scyosn_cd_list)
        oaza_tshum_cd_listV = self.lb.transform_OAZA_TSHUM_CD(oaza_tshum_cd_list)
        azchm_cd_listV = self.lb.transform_AZCHM_CD(azchm_cd_list)
        return self.model.evaluate(x=[address_listV, tdfkn_cd_listV, scyosn_cd_listV, oaza_tshum_cd_listV], y=azchm_cd_listV, batch_size=batch_size)

class addresscode_Classifier:
    """
    住所コード分類モデルクラス
    """

    tdfkn_cd_classifier = None
    scyosn_cd_classifier = None
    oaza_tshum_cd_classifier = None
    azchm_cd_classifier = None

    def __init__(self, lb:AddressCodeBinarizer, t2a:TextToArrays):
        self.tdfkn_cd_classifier = TDFKN_CD_Classifier(lb, t2a)
        self.scyosn_cd_classifier = SCYOSN_CD_Classifier(lb, t2a)
        self.oaza_tshum_cd_classifier = OAZA_TSHUM_CD_Classifier(lb, t2a)
        self.azchm_cd_classifier = AZCHM_CD_Classifier(lb, t2a)

    def load_model(self):
        logger.info('住所コードモデルを読み込みます')
        self.tdfkn_cd_classifier.load_model()
        self.scyosn_cd_classifier.load_model()
        self.oaza_tshum_cd_classifier.load_model()
        self.azchm_cd_classifier.load_model()

    def predict(self, address_list, batch_size=1, verbose=1):
        logger.info('住所コードを予測します')
        #都道府県コード予測
        tdfkn_cd = self.tdfkn_cd_classifier.predict(address_list, batch_size=batch_size, verbose=verbose)
        #市区町村コード予測
        scyosn_cd = self.scyosn_cd_classifier.predict(address_list, tdfkn_cd, batch_size=batch_size, verbose=verbose)
        #大字通称コード予測
        oaza_tshum_cd = self.oaza_tshum_cd_classifier.predict(address_list, tdfkn_cd, scyosn_cd, batch_size=batch_size, verbose=verbose)
        #字丁目コード予測
        azchm_cd = self.azchm_cd_classifier.predict(address_list, tdfkn_cd, scyosn_cd, oaza_tshum_cd, batch_size=batch_size, verbose=verbose)

        return tdfkn_cd.astype(object) + scyosn_cd.astype(object) + oaza_tshum_cd.astype(object) + azchm_cd.astype(object)

class ZIPCODE_Classifier:
    """
    郵便番号分類モデルクラス
    """

    lb:AddressCodeBinarizer = None
    t2a:TextToArrays = None
    model:models.Model = None
    history = None

    def __init__(self, lb:AddressCodeBinarizer, t2a:TextToArrays):
        self.lb = lb
        self.t2a = t2a

    def assemble_model(self)->None:
        """ モデル定義 """

        #入力を定義
        addressname_input = layers.Input(name='addressname_input', shape=(10, 100, ), dtype='float32')

        #中間層を定義
        x = layers.LSTM(name='addressname_lstm_1', units=300)(addressname_input)
        #Dropoutを利用して過剰適合を回避する
        x = layers.Dropout(name='addressname_dropout_1', rate = 0.1)(x)

        #出力を定義
        x = layers.Dense(name='zipcode_output', units=self.lb.ZIPCODE_LEN, activation='softmax')(x)

        #モデル定義とコンパイル
        self.model = models.Model(name='zipcode_classification_model', inputs=addressname_input, outputs=x)
        self.model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])
        self.model.summary()

    def fit_from_file(self, train_file_path:str, val_file_path:str, batch_size:int, epochs:int):
        """ モデル訓練 """
        logger.info('郵便番号分類モデルを訓練します')
        train_gen = ZIPCODE_generator(self.lb, self.t2a, train_file_path, batch_size=batch_size)
        val_gen = ZIPCODE_generator(self.lb, self.t2a, val_file_path, batch_size=batch_size)
        self.history = self.model.fit_generator(
            train_gen,
            steps_per_epoch=train_gen.num_batches_per_epoch,
            validation_data=val_gen,
            validation_steps=val_gen.num_batches_per_epoch,
            epochs=epochs,
            workers=0)
        return self.history

    def save_model(self)->None:
        """ モデル保存 """
        model_json_str = self.model.to_json()
        open(pathlib.Path(__file__).parent / 'model/11_郵便番号予測.Model', 'w').write(model_json_str)
        self.model.save_weights(pathlib.Path(__file__).parent / 'model/11_郵便番号予測.Weights')
        utils.plot_model(self.model, to_file=pathlib.Path(__file__).parent / 'model/11_郵便番号予測.png', show_shapes=True)

    def save_training_accuracy(self)->None:
        #訓練の正確度と損失グラフを作成する
        history_dict = self.history.history

        acc = history_dict['accuracy']
        val_acc = history_dict['val_accuracy']
        loss = history_dict['loss']

        epochs = range(1, len(loss) + 1)

        #訓練と検証正確度をイメージで保存
        plt.plot(epochs, acc, 'bo', label='Training acc')      #bo:青い点線
        plt.plot(epochs, val_acc, 'b', label='Validation acc') #b:青い実線
        plt.title('Training and validation accuracy')
        plt.xlabel('Epochs')
        plt.ylabel('Accuracy')
        plt.legend()
        plt.savefig(pathlib.Path(__file__).parent / 'Model/11_郵便番号予測_訓練と検証正確度.png')

    def save_training_loss(self)->None:
        #訓練の正確度と損失グラフを作成する
        history_dict = self.history.history

        loss = history_dict['loss']
        val_loss = history_dict['val_loss']

        epochs = range(1, len(loss) + 1)

        #訓練と検証損失をイメージで保存
        plt.plot(epochs, loss, 'bo', label='Training loss')      #bo:青い点線
        plt.plot(epochs, val_loss, 'b', label='Validation loss') #b:青い実線
        plt.title('Training and validation loss')
        plt.xlabel('Epochs')
        plt.ylabel('Loss')
        plt.legend()
        plt.savefig(pathlib.Path(__file__).parent / 'Model/11_郵便番号予測_訓練と検証損失.png')

    def load_model(self):
        logger.info('保存した郵便番号を読み込みます')
        #モデルを読み込む
        self.model = models.model_from_json(open(pathlib.Path(__file__).parent / 'model/11_郵便番号予測.Model').read())

        #学習結果を読み込む
        self.model.load_weights(pathlib.Path(__file__).parent / 'model/11_郵便番号予測.Weights')

        #モデルコンパイル
        self.model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])

    def predict(self, address_list, batch_size=1, verbose=1):
        logger.info('郵便番号を予測します')
        address_listV = np.array([self.t2a.word2vec(s) for s in address_list])
        predV = self.model.predict(x=address_listV, batch_size=batch_size, verbose=verbose)
        return self.lb.inverse_transform_ZIPCODE(predV)

    def predict_from_file(self, file_path, batch_size=1, verbose=1):
        logger.info('郵便番号を予測します')
        predict_gen = ZIPCODE_generator(self.lb, self.t2a, file_path, batch_size=batch_size)
        return self.model.predict_generator(predict_gen, batch_size=batch_size, workers=0, verbose=verbose)

    @property
    def metrics_names(self):
        return self.model.metrics_names

    def evaluate_model(self, x, y, batch_size=1):
        return self.model.evaluate(x=x, y=y, batch_size=batch_size)
