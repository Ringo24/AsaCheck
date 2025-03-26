# 標準ライブラリインポート
import logging
from abc import abstractmethod

# サードパーティライブラリインポート

# プロジェクトライブラリインポート
from .. import const
from .. import utils

class BaseDao(object):
    '''
    '''

    def __init__(self):
        '''
        '''
        self._logger: logging.Logger = utils.getLogger()

    @property
    def logger(self) -> logging.Logger:
        '''
        ロガー
        '''

        return self._logger

    @abstractmethod
    def conn(self):
        '''
        DB接続メソッド
        子クラスはこのメソッドを実装しなければなりません。
        '''

        raise NotImplementedError

    @abstractmethod
    def cursor(self):
        '''
        DBカーソルメソッド
        子クラスはこのメソッドを実装しなければなりません。
        '''

        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        '''
        DBカーソルメソッド
        子クラスはこのメソッドを実装しなければなりません。
        '''

        raise NotImplementedError
