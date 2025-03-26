# 標準ライブラリインポート
import configparser
import pathlib

# サードパーティライブラリインポート
import json5

# プロジェクトライブラリインポート

MSG = json5.load(open(pathlib.Path(__file__).resolve().parents[1] / 'config/message.json', 'r', encoding='utf-8'))