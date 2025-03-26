# 標準ライブラリインポート
import pathlib
import logging
import re
from re import sub
import hashlib
import pickle
import itertools
from typing import Generator
from typing import Callable
from typing import Dict
from collections import OrderedDict
from collections import defaultdict

# サードパーティライブラリインポート
import numpy as np
import pandas as pd
from jaconv import h2z
from kanjize import int2kanji
import janome.tokenizer

# プロジェクトライブラリインポート
from . import const
from . import utils

tdfkn_cd_dict = {'北海道': '01',
                 '青森県': '02',
                 '岩手県': '03',
                 '宮城県': '04',
                 '秋田県': '05',
                 '山形県': '06',
                 '福島県': '07',
                 '茨城県': '08',
                 '栃木県': '09',
                 '群馬県': '10',
                 '埼玉県': '11',
                 '千葉県': '12',
                 '東京都': '13',
                 '神奈川県': '14',
                 '新潟県': '15',
                 '富山県': '16',
                 '石川県': '17',
                 '福井県': '18',
                 '山梨県': '19',
                 '長野県': '20',
                 '岐阜県': '21',
                 '静岡県': '22',
                 '愛知県': '23',
                 '三重県': '24',
                 '滋賀県': '25',
                 '京都府': '26',
                 '大阪府': '27',
                 '兵庫県': '28',
                 '奈良県': '29',
                 '和歌山県': '30',
                 '鳥取県': '31',
                 '島根県': '32',
                 '岡山県': '33',
                 '広島県': '34',
                 '山口県': '35',
                 '徳島県': '36',
                 '香川県': '37',
                 '愛媛県': '38',
                 '高知県': '39',
                 '福岡県': '40',
                 '佐賀県': '41',
                 '長崎県': '42',
                 '熊本県': '43',
                 '大分県': '44',
                 '宮崎県': '45',
                 '鹿児島県': '46',
                 '沖縄県': '47'}

tdfkn_cd_search_re = r"(^北海道)|(^青森県)|(^岩手県)|(^宮城県)|(^秋田県)|(^山形県)|(^福島県)|(^茨城県)|(^栃木県)|(^群馬県)|(^埼玉県)|(^千葉県)|(^東京都)|(^神奈川県)|(^新潟県)|(^富山県)|(^石川県)|(^福井県)|(^山梨県)|(^長野県)|(^岐阜県)|(^静岡県)|(^愛知県)|(^三重県)|(^滋賀県)|(^京都府)|(^大阪府)|(^兵庫県)|(^奈良県)|(^和歌山県)|(^鳥取県)|(^島根県)|(^岡山県)|(^広島県)|(^山口県)|(^徳島県)|(^香川県)|(^愛媛県)|(^高知県)|(^福岡県)|(^佐賀県)|(^長崎県)|(^熊本県)|(^大分県)|(^宮崎県)|(^鹿児島県)|(^沖縄県)"


def extract_tdfkn_from_address(addr_nm: str) -> tuple:
    '''
    都道府県コードを抽出する
    '''
    global tdfkn_cd_dict, tdfkn_cd_search_re

    tdfkn_nm: str = None
    tdfkn_cd: str = None
    street_addr_nm: str = None
    matched = re.search(tdfkn_cd_search_re, addr_nm)
    if matched:
        # 都道府県名称
        tdfkn_nm = matched.group(0)
        # 都道府県コード
        tdfkn_cd = tdfkn_cd_dict[tdfkn_nm]
        # 住所（市区町村以下）
        street_addr_nm = addr_nm[matched.end():]
    else:
        tdfkn_nm = '未検出'
        tdfkn_cd = 'ZZ'
        street_addr_nm = None

    return (tdfkn_nm, tdfkn_cd, street_addr_nm)

def address_cleansing(text: str) -> str:
    '''
    住所をクレンジングする
    住所クレンジング処理は学習データには実施せず、住所変換時と検証時のみ実施すること

    Args:
        text: クレンジング対象住所の文字列
    Returns:
        クレンジングした文字列
    '''
    if not text:
        return text

    result = text
    for key in const.CLENSING_CONFIG_ADDRESS['cleansing_settings']:
        result = sub(key['patternstring'], key['replacementstring'], result)
    return result

def unification_text(text: str) -> str:
    """
    文字列を統一化する
     ・大文字に変換する
     ・全角に変換する
     ・数字を漢数字に変換する

    Args:
        text: 変換したい文字列
    Returns:
        変換した文字列
    """

    if not text:
        return ""

    # 全角変換
    text = h2z(text.upper(), digit=True, ascii=True)
    # ハイフン文字を統一
    text = sub(r"(ー|－|‐|―)", "－", text)
    # 空白及び改行削除
    text = sub(r"(　|\n)", "", text)
    # 漢数字変換
    text = sub(r"([０-９]{1,20})", lambda m: int2kanji(int(m.group())), text)
    return text

def azchm_hypen_inverse_convert(text: str) -> str:
    '''
    字丁目ハイフンの逆変換

    Args:
        text: 変換したい文字列
    Returns:
        変換した文字列
    '''
    if "丁目" in text: return text
    text = sub(r"([一二三四五六七八九十百千万]{1,20})－([一二三四五六七八九十百千万]{1,20}.*)", "\\1丁目\\2", text)
    return text

def azchm_after_address_truncate(text: str) -> str:
    '''
    字丁目以降住所切り捨て

    Args:
        text: 変換したい文字列
    Returns:
        変換した文字列
    '''
    matched = re.match(r".*[一|二|三|四|五|六|七|八|九|十](丁目|番町)", text)
    return matched.group() if matched else text


class JapaneseSentenceVectorizer(object):
    """Text vectorizer utility class.

    This class allows to vectorize a text corpus, by turning each
    text into either a sequence of integers (each integer being the index
    of a token in a dictionary) or into a vector where the coefficient
    for each token could be binary, based on word count, based on tf-idf...

    # Arguments
        max_tokens: the maximum number of words to keep, based
            on word frequency. Only the most common `max_tokens-2` words will
            be kept.
        output_sequence_length: the output will have its time dimension padded
            or truncated to exactly `output_sequence_length` values, resulting
            in a tensor of shape [batch_size, output_sequence_length] regardless
            of how many tokens resulted from the splitting step.

    `0` is a reserved index for empty word.
    `1` is a reserved index that won't be assigned to any word.
    """

    def __init__(self,
                 max_tokens: int,
                 output_sequence_length: int,
                 tokenizer: janome.tokenizer.Tokenizer=None,
                 unk_token: str='［ＵＮＫ］',
                 oov_token: str='［ＯＯＶ］',
                 document_count=0):

        # OrderedDict[str, int]
        self.word_counts: OrderedDict = OrderedDict()
        self.output_sequence_length: int = output_sequence_length
        self.word_docs: defaultdict = defaultdict(int)
        if tokenizer is None:
            tokenizer = janome.tokenizer.Tokenizer(str(const.APP_MODEL_PATH))
        self.tokenizer: janome.tokenizer.Tokenizer = tokenizer
        self.max_tokens: int = max_tokens
        self.document_count: int = document_count
        self.unk_token: str = unk_token
        self.oov_token: str = oov_token
        self.index_docs: defaultdict = defaultdict(int)
        self.word_index: Dict[str, int] = {}
        self.index_word: Dict[int, str] = {}

    def fit_on_texts(self, texts):
        """Updates internal vocabulary based on a list of texts.

        In the case where texts contains lists,
        we assume each entry of the lists to be a token.

        Required before using `texts_to_sequences` or `texts_to_matrix`.

        # Arguments
            texts: can be a list of strings,
                a generator of strings (for memory-efficiency),
                or a list of list of strings.
        """

        if isinstance(texts, str):
            raise TypeError("expects an array of text on input, not a single string")

        for text in texts:
            if text is None:
                continue
            self.document_count += 1
            seq = list(self.tokenizer.tokenize(text, wakati=True))
            for w in seq:
                if w in self.word_counts:
                    self.word_counts[w] += 1
                else:
                    self.word_counts[w] = 1
            for w in set(seq):
                # In how many documents each word occurs
                self.word_docs[w] += 1

        wcounts = list(self.word_counts.items())
        wcounts.sort(key=lambda x: x[1], reverse=True)
        sorted_voc = []
        # forcing the oov_token to index 1 if it exists
        if self.oov_token is not None:
            sorted_voc.append(self.oov_token)
        if self.unk_token is not None:
            sorted_voc.append(self.unk_token)
        sorted_voc.extend(wc[0] for wc in wcounts)

        # note that index 0 is reserved, never assigned to an existing word
        self.word_index = dict(
            zip(sorted_voc, list(range(1, len(sorted_voc) + 1))))

        self.index_word = {c: w for w, c in self.word_index.items()}

        for w, c in list(self.word_docs.items()):
            self.index_docs[self.word_index[w]] = c

    def texts_to_sequences(self, texts) -> np.ndarray:
        """Transforms each text in texts to a sequence of integers.

        Only top `max_tokens-2` most frequent words will be taken into account.
        Only words known by the tokenizer will be taken into account.

        # Arguments
            texts: A list of texts (strings).

        # Returns
            A list of sequences.
        """
        if isinstance(texts, str):
            raise TypeError("expects an array of text on input, not a single string")

        text_len = len(texts)
        sequences = itertools.chain.from_iterable(self.texts_to_sequences_generator(texts))
        results = np.fromiter(sequences, dtype=np.int32, count=text_len * self.output_sequence_length)
        results.shape = (text_len, self.output_sequence_length)
        return results

    def texts_to_sequences_generator(self, texts):
        """Transforms each text in `texts` to a sequence of integers.

        Each item in texts can also be a list,
        in which case we assume each item of that list to be a token.

        Only top `max_tokens-2` most frequent words will be taken into account.
        Only words known by the tokenizer will be taken into account.

        # Arguments
            texts: A list of texts (strings).

        # Yields
            Yields individual sequences.
        """
        max_tokens = self.max_tokens
        emt_toekn_index = 0
        oov_token_index = self.word_index.get(self.oov_token)
        unk_token_index = self.word_index.get(self.unk_token)
        for text in texts:
            
            if text is None:
                # 固定長のリストを返却する
                yield [emt_toekn_index] * self.output_sequence_length
            else:
                vect = []
                seq = list(self.tokenizer.tokenize(text, wakati=True))
                for w, idx in itertools.zip_longest(seq, range(self.output_sequence_length)):
                    if w is None:
                        vect.append(emt_toekn_index)
                        continue
                    if idx is None:
                        break
                    i = self.word_index.get(w)
                    if i is not None:
                        if max_tokens and i >= max_tokens:
                            if oov_token_index is not None:
                                vect.append(oov_token_index)
                        else:
                            vect.append(i)
                    elif self.unk_token is not None:
                        vect.append(unk_token_index)
                    elif self.oov_token is not None:
                        vect.append(oov_token_index)
                    else:
                        vect.append(emt_toekn_index)
                yield vect

    def sequences_to_texts(self, sequences):
        """Transforms each sequence into a list of text.

        Only top `max_tokens-2` most frequent words will be taken into account.
        Only words known by the tokenizer will be taken into account.

        # Arguments
            sequences: A list of sequences (list of integers).

        # Returns
            A list of texts (strings)
        """
        return list(self.sequences_to_texts_generator(sequences))

    def sequences_to_texts_generator(self, sequences):
        """Transforms each sequence in `sequences` to a list of texts(strings).

        Each sequence has to a list of integers.
        In other words, sequences should be a list of sequences

        Only top `max_tokens-2` most frequent words will be taken into account.
        Only words known by the tokenizer will be taken into account.

        # Arguments
            sequences: A list of sequences.

        # Yields
            Yields individual texts.
        """
        max_tokens = self.max_tokens
        emt_toekn_index = 0
        oov_token_index = self.word_index.get(self.oov_token)
        unk_token_index = self.word_index.get(self.unk_token)
        for seq in sequences:
            vect = []
            for num in seq:
                if num == emt_toekn_index:
                    vect.append('')
                    continue

                word = self.index_word.get(num)
                if word is not None:
                    if max_tokens and num >= max_tokens:
                        if oov_token_index is not None:
                            vect.append(self.index_word[oov_token_index])
                    else:
                        vect.append(word)
                elif self.unk_token is not None:
                    vect.append(self.index_word[unk_token_index])
                elif self.oov_token is not None:
                    vect.append(self.index_word[oov_token_index])
                else:
                    vect.append('')
            yield vect

    def get_config(self):
        '''Returns the tokenizer configuration as Python dictionary.
        The word count dictionaries used by the tokenizer get serialized
        into plain JSON, so that the configuration can be read by other
        projects.

        # Returns
            A Python dictionary with the tokenizer configuration.
        '''

        return {
            'max_tokens': self.max_tokens,
            'output_sequence_length': self.output_sequence_length,
            'unk_token': self.unk_token,
            'oov_token': self.oov_token,
            'document_count': self.document_count,
            'word_counts': self.word_counts,
            'word_docs': self.word_docs,
            'index_docs': self.index_docs,
            'index_word': self.index_word,
            'word_index': self.word_index
        }

    def save(self, file_path = None) -> None:
        """
        save

        # Arguments
            file_path: path to save
        """
        if file_path is None:
            file_path = const.APP_DATA_PATH / "japanese_sentence_vectorizer.pickle"
        config = self.get_config()
        pickle.dump(config, open(file_path, "wb"))

    @staticmethod
    def load_from_file(file_path = None):
        """
        load
        """

        if file_path is None:
            file_path = const.APP_MODEL_PATH / "japanese_sentence_vectorizer.pickle"
        config = pickle.load(open(file_path, "rb"))
        vectorizer = JapaneseSentenceVectorizer(
            max_tokens=config['max_tokens'],
            output_sequence_length=config['output_sequence_length'],
            unk_token=config['unk_token'],
            oov_token=config['oov_token'],
            document_count=config['document_count']
            )

        vectorizer.word_counts = config['word_counts']
        vectorizer.word_docs = config['word_docs']
        vectorizer.index_docs = config['index_docs']
        vectorizer.word_index = config['word_index']
        vectorizer.index_word = config['index_word']

        return vectorizer
