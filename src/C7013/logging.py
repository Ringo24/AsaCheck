# 標準ライブラリインポート
import os
import datetime
from logging import FileHandler

# サードパーティライブラリインポート

# プロジェクトライブラリインポート


class BizMergeFileHandler(FileHandler):
    """
    BizMerge用ファイルhandlerクラス
    filepathには以下の特集文字を設定できます。
    %YYYYMMDD%：現在の年月日を指定
    %HHMMSS%：現在の時分秒を指定
    """

    def __init__(self, filepath: str, encoding = None, delay = False):
        """
        初期化関数

        Args:
            filepath: ログの出力パス
        """

        # フォルダが存在しない場合、フォルダを作成する。
        dirname = os.path.dirname(filepath)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        filename = self.resolvePathFromTemplate(filepath)

        super().__init__(filename=filename, mode='a', encoding=encoding, delay=delay)

    def _open(self):
        """
        このメソッドはSJIS出力時UnicodeEncodeErrorが発生することを防ぐため、
        FileHandlerの_openメソッドをオーバライドします。
        ファイルを開くときのエラー処理を追加し、streamをリターンします。
        """
        return open(self.baseFilename, self.mode, encoding=self.encoding, errors='replace')

    def resolvePathFromTemplate(self, filepath: str) -> str:
        """
        filepathから特集文字を置換します。
        filepathが既に存在する場合、_0から連番を付与します。

        Args:
            filepath: ログの出力パス
        Returns:
            特集文字が置換されたパス
        """

        now = datetime.datetime.now()
        yyyymmdd = now.strftime('%Y%m%d')
        hhmmss = now.strftime('%H%M%S')

        if '%YYYYMMDD%' in filepath:
            filepath = filepath.replace('%YYYYMMDD%', yyyymmdd)
        if '%HHMMSS%' in filepath:
            filepath = filepath.replace('%HHMMSS%', hhmmss)

        if os.path.exists(filepath):
            foldername, filename = os.path.split(filepath)
            filename_without_ext, file_ext = os.path.splitext(filename)
            index = 0
            while True:
                if not file_ext:
                    new_filepath = f'{foldername}\\{filename_without_ext}_{index}'
                else:
                    new_filepath = f'{foldername}\\{filename_without_ext}_{index}{file_ext}'
                if not os.path.exists(new_filepath):
                    filepath = new_filepath
                    break
                index = index + 1
        return filepath
