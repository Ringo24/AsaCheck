# 標準ライブラリインポート
import logging
import configparser
import datetime
import pathlib
import os
import argparse
from abc import ABC
from abc import abstractmethod

# サードパーティライブラリインポート

# プロジェクトライブラリインポート
from . import const
from . import utils
from .task import BaseTask
from .task import TaskResult
from .dto.RGLT_INFO import RGLT_INFO
from .dao.crmdb_dao import CrmDBDao

class BaseBatch(ABC):
    '''
    バッチ基底クラス
    '''

    def __init__(self, appId: str, appName: str):
        '''
        コンストラクタ
        子クラスはこの関数を必ず呼び出す必要があります。
        以下の様に初期化します。
        super().__init__('機能ID', '機能名')
        '''

        # 機能ID
        self._appId: str = appId
        # 機能名
        self._appName: str = appName
        # 現在日時
        self._startDateTime: datetime.datetime = datetime.datetime.now()
        # ロガー
        self._logger: logging.Logger = utils.getLogger()
        # 規制情報
        self._rglt_info: RGLT_INFO = None

    @property
    def appHomePath(self) -> pathlib.PurePath:
        '''
        アプリケーションホームフォルダ
        '''

        return const.APP_HOME_PATH

    @property
    def appDataPath(self) -> pathlib.PurePath:
        '''
        アプリケーションデータフォルダ
        '''

        return const.APP_DATA_PATH

    @property
    def appId(self) -> str:
        '''
        アプリケーションID(機能ID)
        '''

        return self._appId

    @property
    def appName(self) -> str:
        '''
        アプリケーション名(機能名)
        '''

        return self._appName

    @property
    def appConfig(self) -> configparser.ConfigParser:
        '''
        アプリケーション環境設定
        以下の様に利用する
        self.appConfig.get('section1', 'key1')
        self.appConfig.getboolean('section2', 'key2')
        '''

        return const.APP_CONFIG

    @property
    def startDateTime(self) -> datetime.datetime:
        '''
        バッチ開始年月日時分秒
        '''

        return self._startDateTime

    @property
    def logger(self) -> logging.Logger:
        '''
        ロガー
        '''

        return self._logger

    @property
    def rglt_info(self) -> RGLT_INFO:
        '''
        規制情報
        '''
        if self._rglt_info is None:
            raise RuntimeError("規制情報が存在しません。")

        return self._rglt_info

    def beginBatchExeclusion(self) -> bool:
        '''
        バッチの排他処理を開始する
        '''
        with CrmDBDao() as dao:
            self._rglt_info = dao.try_get_locked_rglt_info(self.appId, 100)
            if self._rglt_info:
                # 更新完了でコミット実施
                dao.commit()
                return True
        return False

    def endBatchExeclusion(self, result: int, spr3: str = None, spr4: str = None) -> bool:
        '''
        バッチの排他処理を終了する
        '''

        if not self._rglt_info:
            return False

        with CrmDBDao() as dao:
            self._rglt_info.SPR1 = 0
            self._rglt_info.SPR2 = str(result)
            self._rglt_info.SPR3 = spr3
            self._rglt_info.SPR4 = spr4
            if dao.update_rglt_info(self._rglt_info) > 0:
                # 更新完了でコミット実施
                dao.commit()
                return True
        return False

    def is_prohibition_period(self) -> bool:
        '''
        運用制限中かどうかチェックする。
        '''
        if self._rglt_info is None:
            raise RuntimeError("規制情報が存在しません。")

        rglt_kikan_strt = self._rglt_info.RGLT_KIKAN_STRT
        rglt_kikan_end = self._rglt_info.RGLT_KIKAN_END
        rglt_time_strt = self._rglt_info.RGLT_TIME_STRT
        if not rglt_time_strt:
            rglt_time_strt = "0000"
        rglt_time_end = self._rglt_info.RGLT_TIME_END
        if not rglt_time_end:
            rglt_time_end = "9999"

        current_datetime = int(self._startDateTime.strftime('%Y%m%d%H%M'))

        # 規制期間開始かつ、規制期間終了がNULLの場合
        if (not rglt_kikan_strt) and (not rglt_kikan_end):
            return False

        # 規制期間開始がNULL以外かつ、規制期間開始＋規制時間開始＞現在日時の場合
        if rglt_kikan_strt:
            rglt_start_datetime = int("{0}{1}".format(
                rglt_kikan_strt, rglt_time_strt))
            if rglt_start_datetime > current_datetime:
                return False

        # 規制期間終了がNULL以外かつ、規制期間終了＋規制時間終了＜現在日時の場合
        if rglt_kikan_end:
            rglt_end_datetime = int("{0}{1}".format(
                rglt_kikan_end, rglt_time_end))
            if rglt_end_datetime < current_datetime:
                return False

        return True

    @abstractmethod
    def execute(self, args: argparse.Namespace) -> int:
        '''
        バッチ実行メソッド
        子クラスはこのメソッドを実装しなければなりません。
        '''

        raise NotImplementedError
