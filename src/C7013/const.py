# 標準ライブラリインポート
import configparser
import pathlib
import uuid

# サードパーティライブラリインポート
import json5

# プロジェクトライブラリインポート


# アプリケーションホーム
APP_HOME_PATH: pathlib.PurePath = pathlib.Path(__file__).resolve().parents[1]

# アプリケーション設定フォルダパス
APP_CONFIG_PATH: pathlib.PurePath = APP_HOME_PATH / 'config'

# キーワード設定ファイルフォルダパス
APP_KEYWORD_FILE_PATH: pathlib.PurePath = APP_CONFIG_PATH / 'C7013_03'

# ログ環境設定ファイルパス
LOG_CONFIG_FILE_PATH: pathlib.PurePath = APP_CONFIG_PATH / 'logging.json'

# アプリケーション環境設定
APP_CONFIG = json5.load(open(APP_CONFIG_PATH / 'app_config.json', 'r', encoding='utf-8'))

# アプリケーションデータフォルダのパス
APP_DATA_PATH: pathlib.PurePath = pathlib.WindowsPath(APP_CONFIG['global_config']['app_data_path']) if APP_CONFIG['global_config']['app_data_path'] else APP_HOME_PATH / 'data'

# アプリケーションモデルデータフォルダのパス
APP_MODEL_PATH: pathlib.PurePath = pathlib.WindowsPath(APP_CONFIG['global_config']['app_model_path']) if APP_CONFIG['global_config']['app_model_path'] else APP_HOME_PATH / 'model'

# SQLiteファイルパス
SQLITE_DB_PATH: pathlib.PurePath = APP_HOME_PATH / APP_CONFIG['database_config']['sqlite_db_path']

# CRMDB接続サーバーアドレス
CRMDB_CONN_SERVER: str = APP_CONFIG['database_config']['crmdb_conn_server']
# CRMDB接続DB名
CRMDB_CONN_DATABASE: str = APP_CONFIG['database_config']['crmdb_conn_database']
# CRMDB接続時ID/PWを利用するか
CRMDB_CONN_USE_ID_PASSWORD_AUTHENTICATION: bool = APP_CONFIG['database_config']['crmdb_conn_use_id_password_authentication']
# CRMDB接続ID
CRMDB_CONN_ID: str = APP_CONFIG['database_config']['crmdb_conn_id']
# CRMDB接続PW
CRMDB_CONN_PW: str = APP_CONFIG['database_config']['crmdb_conn_pw']

# NWMDB接続SID
NWMDB_CONN_SID: str = APP_CONFIG['database_config']['nwmdb_conn_sid']
# NWMDB接続ID
NWMDB_CONN_ID: str = APP_CONFIG['database_config']['nwmdb_conn_id']
# NWMDB接続PW
NWMDB_CONN_PW: str = APP_CONFIG['database_config']['nwmdb_conn_pw']

# 正常終了
BATCH_SUCCESS: int = 0
# 異常終了
BATCH_ERROR: int = 1
# 警告終了
BATCH_WARNING: int = 2

# 設定_データクレンジング_契約者名クレンジング
CLENSING_CONFIG_NAMECONTRACTOR_NAME = json5.load(open(APP_CONFIG_PATH / 'contractorname_cleansing.json', 'r', encoding='utf-8'))

# 設定_データクレンジング_取次内容クレンジング
CLENSING_CONFIG_CONTENTS_COMMISSION = json5.load(open(APP_CONFIG_PATH / 'contents_commission_cleansing.json', 'r', encoding='utf-8'))

# 設定_データクレンジング_住所クレンジング
CLENSING_CONFIG_ADDRESS = json5.load(open(APP_CONFIG_PATH / 'address_cleansing.json', 'r', encoding='utf-8'))

# DataFrame読み書き設定
DATAFRAME_COLUMN_CONFIG = {'jidou_sahai_rev' : object
    ,'sourcecompany' : object
    ,'contractorname' : object
    ,'next_account' : object
    ,'ordertelephonenumber' : object
    ,'contract_id' : object
    ,'contents_commission' : object
    ,'personincharge' : object
    ,'connectiontelephonenumber1' : object
    ,'next_account_code' : object
    ,'contractorname_cleansing' : object
    ,'contents_commission_cleansing' : object
    ,'sourcecompany_cleansing' : object
    ,'personincharge_cleansing' : object
    ,'accountperson_incharge_name' : object
    ,'policy_keywords' : object
    ,'agent_amount' : object
    ,'is_created_opportunity' : object
    ,'annotation_message' : object
    ,'update_date' : object}

# 空のUUID(GUID)
EMPTY_UUID: uuid.UUID = uuid.UUID('00000000-0000-0000-0000-000000000000')