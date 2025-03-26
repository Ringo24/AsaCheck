# 標準ライブラリインポート
import logging
import uuid
import dataclasses
from abc import abstractmethod

# サードパーティライブラリインポート

# プロジェクトライブラリインポート
from .. import const
from .. import utils

@dataclasses.dataclass
class RGLT_INFO(object):
    '''
    規制情報DTO
    '''

    RGLT_APID : str
    REC_NO : str
    RGLT_KIKAN_STRT : str
    RGLT_KIKAN_END : str
    RGLT_TIME_STRT : str
    RGLT_TIME_END : str
    SPR1 : str
    SPR2 : str
    SPR3 : str
    SPR4 : str
    SPR5 : str
    SPR6 : str
    SPR7 : str
    SPR8 : str
    SPR9 : str
    SPR10 : str

    def __init__(self):
        '''
        初期化関数
        '''
        pass
