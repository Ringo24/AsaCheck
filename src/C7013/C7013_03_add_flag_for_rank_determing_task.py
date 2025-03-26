# 標準ライブラリインポート
import os

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
import inject

# プロジェクトライブラリインポート
from .task import BaseTask, TaskResult

class C7013_03_add_flag_for_rank_determing_task(BaseTask):
    '''
    '''

    @inject.autoparams()
    def __init__(self):
        '''
        '''
        #親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self)->str:
        '''
        '''
        self.logger.info(f'タスクを実行します。')

        #TODO:↓↓ここにビジネスロジックを実装します。
        return 'my name is TEMPLATETask\'s TEMPLATEMethod'
        #TODO:↑↑ここにビジネスロジックを実装します。

