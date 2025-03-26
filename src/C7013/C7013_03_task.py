# 標準ライブラリインポート
import os
import warnings
warnings.filterwarnings('ignore', 'This pattern has match groups.')

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
import inject
import jaconv

# プロジェクトライブラリインポート
from . import const, message
from .task import BaseTask, TaskResult
from .dao.crmdb_dao import CrmDBDao

# キーワード設定ファイル名
KEYWORD_SET = 'keyword_'
# 予備キーワード設定ファイル名
RSV = 'reserve_'
KEYWORD1 = '_keyword1'
KEYWORD2 = '_keyword2'
# 予備当初注文内容ファイル名
ORDERCONTENTS_SET = '_ordercontents'
# 拡張子
EXTENSION = '.txt'
# ランク判定用フラグ名
ADD_COLUMN_NAME = 'rank_flag'
# キーワード設定ファイル最大数
MAX_KEYWORD_SET = 40
# 予備キーワード設定ファイル最大数
MAX_RSV_KEYWORD_SET = 30
#データ格納用の辞書のkey
DIC_KEY_KEYWOED_1 = 'keyword_1'
DIC_KEY_KEYWOED_2 = 'keyword_2'
DIC_KEY_KEYWOED_3 = 'keyword_3'
DIC_KEY_PATTERN = 'pattern'
DIC_KEY_NEW_AUTOAGENT = 'new_autoagent'

# DataFrameカラム名
# 取次内容(クレンジング済み)
COMMISSION_CLEANSING = 'contents_commission_cleansing'
# 当初注文内容
ORDERCONTENTS = 'ordercontents'
# 会社名(情報発信元)(クレンジング済み)
SOURCECOMPANY_CLEANSING = 'sourcecompany_cleansing'
# 担当者(クレンジング済み)
PERSONINCHARGE_CLEANSING = 'personincharge_cleansing'
# 自動差配設定(GUID)
AUTOAGENTID_GUID = 'autoagentid_guid'
# 取次区分
COMMISSIONCLASSIFICATION = 'commissionclassification'

# 特定ベンダEカラム名
NEW_AUTOAGENT_SPECIFIC_VENDOR = 'new_autoagent_specific_vendor'

# フラグ毎の算出パターン
CALCULATION_PATTERN = [1,1,1,1,1,1,1,1,1,1,
                       1,1,1,1,1,1,1,1,1,1,
                       1,1,1,1,1,1,1,1,1,1,
                       1,1,2,1,5,1,1,1,1,1,
                       3,4,4,4,4,4,4,4,4,4,
                       4,4,4,4,4,4,4,4,4,4,
                       4,4,4,4,4,4,4,4,4,4,
                       4]

class C7013_03_task(BaseTask):
    '''
    ランク判定用フラグ追加を行うタスククラス
    '''

    # 機能ID
    APP_ID: str = 'C7013_03'
    # 機能名
    APP_NAME: str = 'ランク判定用フラグ追加'

    @inject.autoparams()
    def __init__(self, dao: CrmDBDao):
        '''
        初期化関数

        Args:
            dao: CrmDBDao
        '''
        self.__dao = dao
        #親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self, input_data:pd.DataFrame)->TaskResult:
        '''
        データ抽出/事前チェックで出力したデータとデータクレンジング済のデータをもとに
        ランク判定用フラグ付与を行うクラス

        Args:
            input_data: 入力データ
        Returns:
            TaskResult: ランク判定用フラグ付与済みのタスク結果クラス
        '''
        # 処理開始メッセージ出力
        self.logger.info(message.MSG['MSG0001'], C7013_03_task.APP_ID, C7013_03_task.APP_NAME)

        df = input_data.copy()

        # ランク判定用フラグ追加処理
        # 3.キーワードリスト、算出パターンを取得
        dic_keyword = self._read_keyword(df)

        # 4.キーワード設定ファイル辞書からフラグ追加処理
        self._add_flag(df, dic_keyword)

        # 処理終了メッセージ出力
        self.logger.info(message.MSG['MSG0002'], C7013_03_task.APP_ID, C7013_03_task.APP_NAME)
        
        taskResult = TaskResult(const.BATCH_SUCCESS, df, None)
        return taskResult

    def _get_keyword_list_from_path(self, fpath):
        '''
        設定パスからファイル読み込みをしリスト型で返却

        Args:
                fpath: 設定ファイルパス
        Returns:
                []: キーワードリスト
        '''
        result = []
        if fpath.exists() and os.path.getsize(fpath) > 0:
            with open(fpath, 'r', encoding='utf-8') as f:
                line = f.readline()
                while line:
                    result.append(line.strip())
                    line = f.readline()

        return result

    def _get_new_autoagent(self, df):
        '''
        特定ベンダ名を取得し一覧を作成する
        
        Args:
            df：クレンジング済情報DataFrame
        '''
        dic_new_autoagent = {}
        s_autoagentid_guid = df[AUTOAGENTID_GUID].copy()
        # 重複行の除外
        s_autoagentid_guid = s_autoagentid_guid[~s_autoagentid_guid.duplicated()]
        
        for autoagentid_guid in s_autoagentid_guid:
            if not pd.isna(autoagentid_guid):
                # 特定ベンダEより自動差配設定(GUID)と特定ベンダ名を取得する
                dic_new_autoagent[autoagentid_guid] = \
                    self._get_new_autoagent_specific_vendor(autoagentid_guid)
        return dic_new_autoagent

    def _get_new_autoagent_specific_vendor(self, autoagentid_guid):
        '''
        特定ベンダEより自動差配設定(GUID)と特定ベンダ名を取得する
        取得したベンダ名はスペースの除去、全角化、大文字化を行う
        
        Args:
            autoagentid_guid：自動差配設定(GUID)
        '''
        df_new_autoagent_specific_vendor = self.__dao.retrive_new_autoagent_specific_vendor(autoagentid_guid)
        
        # 大文字化
        df_new_autoagent_specific_vendor[NEW_AUTOAGENT_SPECIFIC_VENDOR] \
            = df_new_autoagent_specific_vendor[NEW_AUTOAGENT_SPECIFIC_VENDOR].str.upper()

        # 全角化
        s_vendor = df_new_autoagent_specific_vendor[NEW_AUTOAGENT_SPECIFIC_VENDOR]
        for num in range(0, len(s_vendor)):
            s_vendor[num] = jaconv.h2z(s_vendor[num], digit=True, ascii=True)
        df_new_autoagent_specific_vendor[NEW_AUTOAGENT_SPECIFIC_VENDOR] = s_vendor

        # 空白の削除
        df_new_autoagent_specific_vendor[NEW_AUTOAGENT_SPECIFIC_VENDOR] \
            = df_new_autoagent_specific_vendor[NEW_AUTOAGENT_SPECIFIC_VENDOR].str.replace('　', '')

        return df_new_autoagent_specific_vendor
    
    def _get_keyword_file_path(self):
        return const.APP_KEYWORD_FILE_PATH

    def _read_keyword(self, df):
        '''
        キーワード設定ファイルの読み込み
        
        キーワード設定ファイル：keyword_NN.txt　NN:キーワードファイルの番号01～40
        予備キーワードファイル１：reserve_NN_keyword1.txt　NN:予備キーワードファイル(キーワード含まれる用)の番号01～30
        予備キーワードファイル２：reserve_NN_keyword2.txt　NN:予備キーワードファイル(キーワード含まれない用)の番号01～30
        予備当初注文内容ファイル：reserve_NN_ordercontents.txt　NN:予備当初注文内容ファイルの番号01～30
        
        Returns:
                キーワード設定ファイル辞書
                {
                    "ランク判定用フラグ名":{
                        "算出パターン" : int,
                        "キーワード設定ファイル_1" : []
                        "キーワード設定ファイル_2" : []
                        "キーワード設定ファイル_3" : []
                    },
                    .
                    .
                    .
                }

                ex)
                {
                    "rank_flag01":{
                        "pattern" : 1,
                        "keyword_1" : ['フレッツ光.?(新規|新設|申込|導入)', 'ＡＬ.?(新規|新設|申込|導入)',...]
                    },
                    .
                    .
                    .
                    "rank_flag41":{
                        "pattern" : 3,
                        "new_autoagent": {
                            "xxxx-xxxx-xxxx-xxx1" : 
                                new_autoagent new_autoagent_specific_vendor
                            0   xxxx-xxxx-xxxx-xxx1     ベンダAAAA
                            1   xxxx-xxxx-xxxx-xxx1     ベンダBBBB
                            2   xxxx-xxxx-xxxx-xxx1     ベンダCCCC
                            ,
                            "xxxx-xxxx-xxxx-xxx2" : 
                                new_autoagent new_autoagent_specific_vendor
                            0   xxxx-xxxx-xxxx-xxx2     ベンダXXXX
                            1   xxxx-xxxx-xxxx-xxx2     ベンダYYYY
                        }
                    },
                    .
                    .
                    .
                    "rank_flag71":{
                        "pattern" : 4,
                        "keyword_1": ['ビジネスホン.*新規', 'ビジネスホン.*新設', ...]
                        "keyword_2": []
                        "keyword_3": ['0', '2', '3']
                    }
                }
        '''
        dic_keyword = {}
        # keyword_01.txt ～ keyword_40.txtの読み込み
        # フラグ１～４０までの処理を定義
        for num in range(1, MAX_KEYWORD_SET + 1):
            dic_sub = {}

            # キーワード設定ファイル名をセット
            keyword_set_file_name = KEYWORD_SET + '{:0=2}'.format(num) + EXTENSION
            keyword_set_file_path = self._get_keyword_file_path() / keyword_set_file_name

            key = ADD_COLUMN_NAME + '{:0=2}'.format(num)

            dic_sub[DIC_KEY_PATTERN] = CALCULATION_PATTERN[num - 1]
            dic_sub[DIC_KEY_KEYWOED_1] = self._get_keyword_list_from_path(keyword_set_file_path)
            dic_keyword[key] = dic_sub
        
        # フラグ４１の処理を定義
        dic_sub = {}
        dic_sub[DIC_KEY_PATTERN] = CALCULATION_PATTERN[MAX_KEYWORD_SET]
        # 特定ベンダ名一覧を取得
        dic_sub[DIC_KEY_NEW_AUTOAGENT] = self._get_new_autoagent(df)
        dic_keyword[ADD_COLUMN_NAME + '{:0=2}'.format(MAX_KEYWORD_SET + 1)] = dic_sub
 
        # reserve_01_keyword1.txt ～ reserve_30_keyword1.txt
        # reserve_01_keyword2.txt ～ reserve_30_keyword2.txt
        # reserve_01_ordercontents.txt ～ reserve_30_ordercontents.txt  の読み込み
        # フラグ４２～７１までの処理を定義
        for num in range(1, MAX_RSV_KEYWORD_SET + 1):
            rsv_keyword_set_file_name_1 = RSV + '{:0=2}'.format(num) + KEYWORD1 + EXTENSION
            rsv_keyword_set_file_name_2 = RSV + '{:0=2}'.format(num) + KEYWORD2 + EXTENSION
            rsv_ordercontents_set_file_name = RSV + '{:0=2}'.format(num) + ORDERCONTENTS_SET + EXTENSION

            rsv_keyword_set_file_path_1 = self._get_keyword_file_path() / rsv_keyword_set_file_name_1
            rsv_keyword_set_file_path_2 = self._get_keyword_file_path() / rsv_keyword_set_file_name_2
            rsv_ordercontents_set_file_path = self._get_keyword_file_path() / rsv_ordercontents_set_file_name

            key = ADD_COLUMN_NAME + '{:0=2}'.format(num + MAX_KEYWORD_SET + 1)
            dic_sub = {}

            dic_sub[DIC_KEY_PATTERN] = CALCULATION_PATTERN[num + MAX_KEYWORD_SET]
            # 予備キーワードファイル1
            dic_sub[DIC_KEY_KEYWOED_1] = self._get_keyword_list_from_path(rsv_keyword_set_file_path_1)
            # 予備キーワードファイル2
            dic_sub[DIC_KEY_KEYWOED_2] = self._get_keyword_list_from_path(rsv_keyword_set_file_path_2)
            # 予備当初注文内容ファイル
            dic_sub[DIC_KEY_KEYWOED_3] = self._get_keyword_list_from_path(rsv_ordercontents_set_file_path)
            dic_keyword[key] = dic_sub
        return dic_keyword

    def _add_flag(self, df, dic_keyword):
        '''
        算出パターンと判定に使用するリストを基にして、ランク判定用フラグを算出する
        
        Args:
            df：クレンジング済情報DataFrame
            dic_keyword：キーワード設定ファイル辞書
        '''
        for key in dic_keyword:
            pattern = dic_keyword[key][DIC_KEY_PATTERN]
            if pattern == 1:
                # 算出パターン１
                self._caluculation_pattern_1(df, key, dic_keyword[key].get(DIC_KEY_KEYWOED_1))
            elif pattern == 2:
                # 算出パターン２
                self._caluculation_pattern_2(df, key, dic_keyword[key].get(DIC_KEY_KEYWOED_1))
            elif pattern == 3:
                # 算出パターン３
                self._caluculation_pattern_3(df, key, dic_keyword[key].get(DIC_KEY_NEW_AUTOAGENT))
            elif pattern == 4:
                # 算出パターン４
                self._caluculation_pattern_4(df, key, 
                                            dic_keyword[key].get(DIC_KEY_KEYWOED_1),
                                            dic_keyword[key].get(DIC_KEY_KEYWOED_2),
                                            dic_keyword[key].get(DIC_KEY_KEYWOED_3))
            elif pattern == 5:
                # 算出パターン５
                self._caluculation_pattern_5(df, key, dic_keyword[key].get(DIC_KEY_KEYWOED_1))


    def _caluculation_pattern_1(self, df, column, keyword_list):
        '''
        算出パターン１

        クレンジング済情報CSVの取次内容(クレンジング済)にキーワード設定ファイルに
        設定されたキーワードが含まれ　かつ
        キーワード設定ファイルにレコードが存在する場合
            フラグ付与済情報CSVリストのフラグを1に設定する。
        それ以外の場合
            フラグ付与済情報CSVリストのフラグを0に設定する。

        Args:
            df：クレンジング済情報DataFrame
            column：フラグ追加カラム名
            keyword_list：キーワード設定ファイルより取得した正規表現一覧
        '''
        #初期値にFalseで追加
        df[column] = False

        if len(df[COMMISSION_CLEANSING]) != df[COMMISSION_CLEANSING].isnull().sum():
            for row in keyword_list:
                # 設定されたキーワードが1度でも含まれればTrue
                df[column] = df[column] | df[COMMISSION_CLEANSING].str.contains(row)

        # booleanをintに変換
        df[column] *= 1

    def _caluculation_pattern_2(self, df, column, keyword_list):
        '''
        算出パターン２

        クレンジング済情報CSVの会社名（情報発信元）(クレンジング済)がキーワード設定ファイルに
        設定されたキーワードと同一　かつ
        キーワード設定ファイルにレコードが存在する場合
            フラグ付与済情報CSVリストのフラグを1に設定する。
        異なる場合
            フラグ付与済情報CSVリストのフラグを０に設定する。

        Args:
            df：クレンジング済情報DataFrame
            column：フラグ追加カラム名
            keyword_list：キーワード設定ファイルより取得したキーワード一覧
        '''
        #初期値にFalseで追加
        df[column] = False

        for row in keyword_list:
            # 設定されたキーワードが1度でも一致すればTrue
            df[column] = df[column] | ( df[SOURCECOMPANY_CLEANSING] == row)

        # booleanをintに変換
        df[column] *= 1

    def _caluculation_pattern_3(self, df, column, dic_new_autoagent):
        '''
        算出パターン３

        クレンジング済情報CSVの担当者(クレンジング済)またはクレンジング済情報CSVの
        取次内容(クレンジング済)に特定ベンダ名一覧のキーワードが１件以上含まれた場合
            フラグ付与済情報CSVリストのフラグを1に設定する。
        それ以外の場合
            フラグ付与済情報CSVリストのフラグを0に設定する。

        ※判定を行う場合は、担当者、取次内容、特定ベンダ名一覧の各レコードに
        含まれるスペースを除去し、全角化および大文字化行い、比較する。

        Args:
            df：クレンジング済情報DataFrame
            column：フラグ追加カラム名
            dic_new_autoagent：自動差配設定(GUID)をkeyにした特定ベンダ一覧
        '''
        #初期値にFalseで追加
        df[column] = False
        
        s_commission = df[column].copy()
        s_personincharge = df[column].copy()

        commission_is_not_all_NaN = False
        personincharge_is_not_all_NaN = False

        if len(df[COMMISSION_CLEANSING]) != df[COMMISSION_CLEANSING].isnull().sum():
            commission_is_not_all_NaN = True
        if len(df[PERSONINCHARGE_CLEANSING]) != df[PERSONINCHARGE_CLEANSING].isnull().sum():
            personincharge_is_not_all_NaN = True

        for key in dic_new_autoagent:
            # 対象は同一の自動差配設定(GUID)のみ
            s_target = df[AUTOAGENTID_GUID] == key
            for row in dic_new_autoagent[key].itertuples():
                # 特定ベンダ名に.+:?等の正規表現のメタ文字が含まれる可能性があるのでregex=Falseとする
                # （全角化しているので正規表現で検索しても結果は変わらない）
                # 取次内容(クレンジング済)の検索（全角スペースのみ除去して比較）
                if commission_is_not_all_NaN:
                    s_commission = s_commission | df[COMMISSION_CLEANSING].str.replace('　','').str.contains(row.new_autoagent_specific_vendor, regex=False)
                # 担当者(クレンジング済)の検索（全角スペースのみ除去して比較）
                if personincharge_is_not_all_NaN:
                    s_personincharge = s_personincharge | df[PERSONINCHARGE_CLEANSING].str.replace('　','').str.contains(row.new_autoagent_specific_vendor, regex=False)
            
            df[column] = df[column] | (s_target & (s_commission | s_personincharge))
            s_commission[:] = False
            s_personincharge[:] = False

        # booleanをintに変換
        df[column] *= 1

    def _caluculation_pattern_4(self, df, column, keyword_1_list, keyword_2_list, keyword_3_list):
        '''
        算出パターン４
        
        1.取次内容に予備Nキーワード１ファイルに設定されたキーワード１件以上含み　かつ
        2.取次内容に予備Nキーワード２ファイルに設定されたキーワードが１件も含まれていない　かつ
        3.当初注文内容が予備N当初注文内容ファイルに設定された値のうちのいずれかに一致する場合
            フラグ付与済情報CSVリストのフラグを1に設定する。
        それ以外の場合
            フラグ付与済情報CSVリストのフラグを0に設定する。
        
        ※設定値なしの場合は条件から除外する
        ただし、全て設定値なしの場合は
            フラグ付与済情報CSVリストのフラグを0に設定する。
        
        Args:
            df：クレンジング済情報DataFrame
            column：フラグ追加カラム名
            keyword_1_list：予備キーワード設定ファイル１より取得した正規表現一覧
            keyword_2_list：予備キーワード設定ファイル２より取得した正規表現一覧
            keyword_3_list：予備当初注文内容ファイル
        '''
        #初期値にFalseで追加
        df[column] = False

        # 作業用にSeriesとして抜き出す
        s_work_1 = df[column].copy()
        s_work_2 = df[column].copy()
        s_work_3 = df[column].copy()

        # 全て設定なしの場合、フラグ0で後続は実行しない
        if len(keyword_1_list) == 0 and len(keyword_2_list) == 0 and len(keyword_3_list) == 0:
            # booleanをintに変換
            df[column] *= 1
            return

        # A.予備Nキーワードファイル1
        if len(keyword_1_list) == 0:
            # 設定値なしの場合は判定条件外のためTrueを設定
            s_work_1[:] = True
        else:
            if len(df[COMMISSION_CLEANSING]) != df[COMMISSION_CLEANSING].isnull().sum():
                # 取次内容に予備Nキーワード1ファイルに設定されたキーワード1件以上含むか
                for row in keyword_1_list:
                    s_work_1 = s_work_1 | df[COMMISSION_CLEANSING].str.contains(row)

        # B.予備Nキーワードファイル2
        if len(keyword_2_list) == 0:
            # 設定値なしの場合は判定条件外のためTrueを設定
            s_work_2[:] = True
        else:
            if len(df[COMMISSION_CLEANSING]) != df[COMMISSION_CLEANSING].isnull().sum():
                # 設定されたキーワードが1件以上含まれるか
                for row in keyword_2_list:
                    s_work_2 = s_work_2 | df[COMMISSION_CLEANSING].str.contains(row)
            # 結果の反転（＝取次内容に予備Nキーワード2ファイルに設定されたキーワード１件も含まれていない）
            s_work_2 = ~s_work_2

        # C.予備N当初注文内容ファイル
        if len(keyword_3_list) == 0:
            # 設定値なしの場合は判定条件外のためTrueを設定
            s_work_3[:] = True
        else:
            # 当初注文内容が予備N当初注文内容ファイルに設定された値のうちのいずれかに一致
            for row in keyword_3_list:
                # 数値以外はスキップ
                if row.isdecimal():
                    s_work_3 = s_work_3 | (df[ORDERCONTENTS] == int(row))

        df[column] = s_work_1 & s_work_2 & s_work_3
        
        # booleanをintに変換
        df[column] *= 1

    def _caluculation_pattern_5(self, df, column, keyword_list):
        '''
        算出パターン５

        クレンジング済情報CSVの取次区分にキーワード設定ファイルに
        設定された取次区分ピックリスト値が含まれ　かつ
        キーワード設定ファイルにレコードが存在する場合
            フラグ付与済情報CSVリストのフラグを1に設定する。
        それ以外の場合
            フラグ付与済情報CSVリストのフラグを0に設定する。

        Args:
            df：クレンジング済情報DataFrame
            column：フラグ追加カラム名
            keyword_list：キーワード設定ファイルより取得した取次区分ピックリスト値
        '''
        #初期値にFalseで追加
        df[column] = False

        for row in keyword_list:
            if row.isdecimal():
                # 設定されたキーワードが1度でも一致すればTrue
                df[column] = df[column] | ( df[COMMISSIONCLASSIFICATION] == int(row) )

        # booleanをintに変換
        df[column] *= 1
