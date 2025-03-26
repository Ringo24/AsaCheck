# 標準ライブラリインポート
from typing import Dict, List

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
from jaconv import h2z

# プロジェクトライブラリインポート
from . import onehot_utils


# 当初注文内容
ordercontents_dict: Dict[str, int] = {
     '新設': 1
    ,'移転': 2
    ,'変更': 4
    ,'その他１': 7
    ,'その他２': 8
    ,'増設': 9
    ,'ch増': 10
    ,'番号増': 11
    ,'休止廃止': 12
    ,'ch減': 13
    ,'番号減': 14
    ,'問い合せ': 15
    ,'該当なし': 0    # 該当なし当初注文内容
}

# ランク
rank_dict: Dict[str, int] = {
     'A': 100
    ,'B': 200
    ,'C': 300
    ,'D': 400
    ,'-': 500
    ,'X': 0    # 該当なしランク
}

# ランク判定用フラグ一覧
rank_flag_columns: List[str] = [
     "rank_flag01"
    ,"rank_flag02"
    ,"rank_flag03"
    ,"rank_flag04"
    ,"rank_flag05"
    ,"rank_flag06"
    ,"rank_flag07"
    ,"rank_flag08"
    ,"rank_flag09"
    ,"rank_flag10"
    ,"rank_flag11"
    ,"rank_flag12"
    ,"rank_flag13"
    ,"rank_flag14"
    ,"rank_flag15"
    ,"rank_flag16"
    ,"rank_flag17"
    ,"rank_flag18"
    ,"rank_flag19"
    ,"rank_flag20"
    ,"rank_flag21"
    ,"rank_flag22"
    ,"rank_flag23"
    ,"rank_flag24"
    ,"rank_flag25"
    ,"rank_flag26"
    ,"rank_flag27"
    ,"rank_flag28"
    ,"rank_flag29"
    ,"rank_flag30"
    ,"rank_flag31"
    ,"rank_flag32"
    ,"rank_flag33"
    ,"rank_flag34"
    ,"rank_flag35"
    ,"rank_flag36"
    ,"rank_flag37"
    ,"rank_flag38"
    ,"rank_flag39"
    ,"rank_flag40"
    ,"rank_flag41"
    ,"rank_flag42"
    ,"rank_flag43"
    ,"rank_flag44"
    ,"rank_flag45"
    ,"rank_flag46"
    ,"rank_flag47"
    ,"rank_flag48"
    ,"rank_flag49"
    ,"rank_flag50"
    ,"rank_flag51"
    ,"rank_flag52"
    ,"rank_flag53"
    ,"rank_flag54"
    ,"rank_flag55"
    ,"rank_flag56"
    ,"rank_flag57"
    ,"rank_flag58"
    ,"rank_flag59"
    ,"rank_flag60"
    ,"rank_flag61"
    ,"rank_flag62"
    ,"rank_flag63"
    ,"rank_flag64"
    ,"rank_flag65"
    ,"rank_flag66"
    ,"rank_flag67"
    ,"rank_flag68"
    ,"rank_flag69"
    ,"rank_flag70"
    ,"rank_flag71"
]

def input_data_transform(input_data: pd.DataFrame) -> np.array:
    '''
    説明変数データを作成します。

    Args:
        input_data: ランク判定学習データ

    Returns:
        説明変数
    '''
    rank_flag_data = input_data[rank_flag_columns].values
    # 欠損値NaNを0に置換する
    np.nan_to_num(rank_flag_data, copy=False)

    # 当初注文内容をone-hot-encodingする
    ordercontents_one_hot = [onehot_utils.ordercontents_one_hot_encode(o) for o in input_data['ordercontents'].values]

    return np.concatenate((ordercontents_one_hot, rank_flag_data), axis=1)

def target_data_transform(input_data: pd.DataFrame) -> np.array:
    '''
    目的変数データを作成します。

    Args:
        input_data: ランク判定学習データ

    Returns:
        目的変数
    '''

    rank_one_hot = np.zeros((len(input_data), len(onehot_utils.rank_onehot_zero)), dtype=np.int32)
    for idx, rank in enumerate(input_data['rank_system']):
        rank_one_hot[idx] = onehot_utils.rank_one_hot_encode(rank)

    return rank_one_hot

def decide_rank(rank_a_predict: np.array, rank_b_predict: np.array, rank_c_predict: np.array, rank_d_predict: np.array, rank_bar_predict: np.array) -> np.array:
    '''
    予測したランクを判定します。
    '''
    result = np.zeros(len(rank_a_predict), dtype=np.int32)

    for idx in range(len(rank_a_predict)):
        if rank_a_predict[idx][0] == 1:
            result[idx] = rank_dict['A'] # A
        elif rank_b_predict[idx][0] == 1:
            result[idx] = rank_dict['B'] # B
        elif rank_c_predict[idx][0] == 1:
            result[idx] = rank_dict['C'] # C
        elif rank_d_predict[idx][0] == 1:
            result[idx] = rank_dict['D'] # D
        elif rank_bar_predict[idx][0] == 1:
            result[idx] = rank_dict['-'] # bar
        else:
            result[idx] = rank_dict['X'] # 該当なし

    return result

def clear_all_rank_flag(row: pd.Series) -> pd.Series:
    '''
    全てのランクフラグをクリアする

    Args:
        row: ランク判定用データフレームの１行
    '''

    row[rank_flag_columns] = 0

    return row