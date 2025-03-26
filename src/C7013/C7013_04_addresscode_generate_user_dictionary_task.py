# 標準ライブラリインポート
import os
import pathlib

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
import inject
from janome.dic import UserDictionary
from janome.progress import SimpleProgressIndicator
from janome import sysdic
import janome.tokenizer

# プロジェクトライブラリインポート
from . import const
from . import message
from .task import BaseTask
from .task import TaskResult
from .addresscode_utils import unification_text
from .addresscode_utils import JapaneseSentenceVectorizer

_user_dictionary_columns = ['表層形', '左文脈ID', '右文脈ID', 'コスト', '品詞',
                            '品詞細分類1', '品詞細分類2', '品詞細分類3', '活用型', '活用形', '原形', '読み', '発音']


class C7013_04_addresscode_generate_user_dictionary_task(BaseTask):
    '''
    住所コード用ユーザー定義辞書を作成する
    '''

    @inject.autoparams()
    def __init__(self):
        '''
        左文脈IDは, その単語を左から見たときの内部状態IDです. 通常システム 辞書と同一場所にある left-id.def から該当する ID を選択します. 空にしておくと mecab-dict-index が自動的に ID を付与します. 
        右文脈IDは, その単語を右から見たときの内部状態IDです. 通常システム 辞書と同一場所にある right-id.def から該当する ID を選択します. 空にしておくと, mecab-dict-index が自動的に ID を付与します. 
        コストは,その単語がどれだけ出現しやすいかを示しています. 小さいほど, 出現しやすいという意味になります. 似たような単語と 同じスコアを割り振り, その単位で切り出せない場合は, 徐々に小さくしていけばいいと思います. 
        '''
        # 親クラスの初期化関数を呼び出す
        super().__init__()

    def execute(self, addresscode_file_path: pathlib.PurePath, output_file_path: pathlib.PurePath) -> int:
        '''
        ユーザー定義辞書を作成する

        Args:
            addresscode_file_path: 住所コードファイルのパス
            dict_outputpath: 出力辞書ファイルのパス

        Returns:
            タスク実行結果（0:正常、1:異常、2:警告）
        '''
        self.logger.info(f'住所コード用ユーザー定義辞書タスクを実行します。')

        self.logger.debug(f'住所コードCSVファイルを読み込みます。 {addresscode_file_path}')
        input_data = pd.read_csv(addresscode_file_path, sep=',', dtype=str, encoding='utf-8')
        # 入力ファイルを読込みました。ファイル名＝{0}
        self.logger.info(message.MSG['MSG0012'], addresscode_file_path)

        if input_data.empty:
            # 入力ファイルにデータが存在しません。ファイル名＝{0}
            self.logger.error(message.MSG['MSG2010'], addresscode_file_path)
            return const.BATCH_ERROR

        generated_dict = self.generateUserDict(input_data)
        custom_dict = self.loadCustomUserDict()
        self.logger.debug(f'自動生成されたユーザ辞書：{len(generated_dict)}件 カスタムユーザー定義辞書：{len(custom_dict)}件')

        # 結合
        output_data = pd.concat([custom_dict, generated_dict], axis=0)
        # 表層形が同じデータを削除（カスタムユーザー定義辞書を優先する）
        output_data = output_data.drop_duplicates(subset=['表層形'], keep='first')

        self.logger.debug(f'ユーザ辞書CSVファイルを書き込みます。 格納先:{output_file_path} 件数:{len(output_data)}')
        output_data.to_csv(output_file_path, sep=',', encoding='utf-8', index=False, header=False)
        # 辞書ファイルを出力しました。ファイル名＝{0}
        self.logger.info(message.MSG['MSG0013'], output_file_path)

        self.logger.debug(f'ユーザ辞書をコンパイルします。')
        #　ユーザ辞書をコンパイルする
        all_user_dict = UserDictionary(str(output_file_path), "utf8", "ipadic", sysdic.connections, progress_handler=SimpleProgressIndicator(update_frequency=0.01))
        # コンパイルした辞書を保存する
        all_user_dict_save_path = output_file_path.parent
        self.logger.debug(f'コンパイルしたユーザ辞書を書き込みます。　{all_user_dict_save_path}')
        all_user_dict.save(str(all_user_dict_save_path))
        # ファイルを保存しました。保存先＝{0}
        self.logger.info(message.MSG['MSG0014'], all_user_dict_save_path)

        self.logger.debug(f'JapaneseSentenceVectorizerを書き込みます。')
        jsv = JapaneseSentenceVectorizer(
            max_tokens=const.APP_CONFIG['addresscode_config']['max_vocab_size'],
            output_sequence_length=const.APP_CONFIG['addresscode_config']['max_sequence_length'],
            tokenizer=janome.tokenizer.Tokenizer(str(all_user_dict_save_path)))

        addr_nm = input_data['addr_nm'].values
        # テキストを統一する
        proc = np.frompyfunc(unification_text, 1, 1)
        addr_nm = proc(addr_nm)
        jsv.fit_on_texts(addr_nm)
        jsv.save()
        # ファイルを保存しました。保存先＝{0}
        self.logger.info(message.MSG['MSG0014'], all_user_dict_save_path)

        self.logger.debug(f'max_tokens:{jsv.max_tokens},output_sequence_length:{jsv.output_sequence_length}')
        self.logger.debug(f'word counts:{len(jsv.word_counts)}')

        return const.BATCH_SUCCESS

    def loadCustomUserDict(self, file_path: pathlib.PurePath = None) -> pd.DataFrame:
        '''
        カスタムユーザー定義辞書をロードする

        Args:
            file_path: カスタムユーザー定義辞書

        Returns:
            ユーザー定義辞書DataFrame
        '''

        # ファイルを指定しない場合、app_configフォルダのファイルを指定する
        if not file_path:
            file_path = const.APP_CONFIG_PATH / 'addresscode_udict.csv'

        # ファイルが存在しない場合、空のDataFrameを返却する
        if not file_path.exists():
            return pd.DataFrame(columns=_user_dictionary_columns, dtype=object)
        else:
            self.logger.debug(f'カスタムユーザ辞書CSVファイルを読み込みます。 格納先:{file_path}')
            # ヘッダなし辞書CSVファイルをロードする
            return pd.read_csv(file_path,
            sep=',',
            encoding='utf-8',
            dtype=object,
            header=None,
            names=_user_dictionary_columns)

    def generateUserDict(self, input_data: pd.DataFrame) -> pd.DataFrame:
        '''
        ユーザー定義辞書を作成する

        Args:
            input_data: 住所コードDataFrame

        Returns:
            ユーザー定義辞書DataFrame
        '''

        # 単語を抽出する
        words = input_data['tdfkn_nm'].append(input_data['scyosn_nm']).append(
            input_data['oaza_tshum_nm']).append(input_data['azchm_nm'])
        # NaNを削除する
        words = words.dropna()
        # テキストを統一する
        words = words.apply(unification_text)
        # 重複を除外する
        words = words.drop_duplicates()
        # 辞書登録する単語を制限する
        words = self._filter_words(words)
        # 特集文字を追加する。
        # UNK = unknown, OOV = out of vocabulary
        words = words.append(pd.Series(['［ＵＮＫ］', '［ＯＯＶ］']))

        output_data = pd.DataFrame(columns=_user_dictionary_columns, dtype=object)
        output_data['表層形'] = words.values
        output_data['左文脈ID'] = 0
        output_data['右文脈ID'] = 0
        output_data['コスト'] = -100000
        output_data['品詞'] = '名詞'
        output_data['品詞細分類1'] = '固有名詞'
        output_data['品詞細分類2'] = '地域'
        output_data['品詞細分類3'] = '一般'
        output_data['活用型'] = '*'
        output_data['活用形'] = '*'
        output_data['原形'] = '*'
        output_data['読み'] = '*'
        output_data['発音'] = '*'

        output_data = output_data.reindex(output_data['表層形'].str.len().sort_values(ascending=True).index)

        return output_data

    def _filter_words(self, words: pd.Series) -> pd.Series:
        '''
        辞書登録する単語を制限する

        Args:
            words: 辞書登録する単語一覧

        Returns:
            フィルタリングした単語一覧
        '''
        # 2文字単語を選択する
        two_length_words = words[words.str.len() == 2]
        # 3文字単語を選択する
        three_length_words = words[words.str.len() == 3]

        # 2文字単語から3文字単語と部分一致する単語を除外する
        two_length_words = two_length_words[~(two_length_words.isin(three_length_words.str[:2]))]
        two_length_words = two_length_words[~(two_length_words.isin(three_length_words.str[-2:]))]
        
        return pd.concat([two_length_words, three_length_words], ignore_index=True)