# 標準ライブラリインポート
import logging

# サードパーティライブラリインポート
import clr

# プロジェクトライブラリインポート
from . import const
from . import utils

# Fast Search関連DLLがロードされたか表すフラグ
__dll_loaded__: bool = False

class FastSearchHelper(object):
    '''
    SharepointのFast検索を操作するためのHelperクラスです
    '''

    def __init__(self):
        '''
        初期化関数
        '''
        self._logger: logging.Logger = utils.getLogger()
        self.loadDlls()
        self._service = None # FastSearchQuery.QueryWebServiceProxy.QueryService

    @property
    def logger(self) -> logging.Logger:
        '''
        ロガー
        '''

        return self._logger

    def loadDlls(self) -> None:
        '''
        .NET ライブラリをロードします。
        '''
        global __dll_loaded__

        if __dll_loaded__ == False:
            # .NET 標準Namespaceをインポートする
            self.logger.debug('.NET ライブラリをロードします。')
            clr.AddReference('System')
            clr.AddReference('System.Core')
            clr.AddReference('System.Configuration')
            clr.AddReference('System.Data')
            clr.AddReference('System.Data.Entity')
            clr.AddReference('System.Data.Services')
            clr.AddReference('System.Design')
            clr.AddReference('System.ServiceModel')
            clr.AddReference('System.ServiceModel.Web')
            clr.AddReference('System.Web')
            clr.AddReference('System.IdentityModel')
            clr.AddReference('System.Security')
            clr.AddReference('System.Runtime.Caching')
            clr.AddReference('System.Xaml')
            clr.AddReference('System.Xml')
            clr.AddReference('System.Xml.Linq')

            # Dynamics CRM様に必要なファイルをインポートする
            self.logger.debug('外部DLLファイルをロードします。')
            clr.AddReference(str(const.APP_HOME_PATH / 'dll/microsoft.identitymodel.dll'))
            clr.AddReference(str(const.APP_HOME_PATH / 'dll/FastSearchQuery.dll'))

            __dll_loaded__ = True

    def Conn(self) -> None:
        self.logger.debug('SharePointを接続します。')

        fast_search_url = const.APP_CONFIG['sharepoint_config']['fast_search_url']
        sps_userid = const.APP_CONFIG['sharepoint_config']['sps_userid']
        sps_password = const.APP_CONFIG['sharepoint_config']['sps_password']
        sps_domain = const.APP_CONFIG['sharepoint_config']['sps_domain']

        #self.logger.debug(f'fast_search_url = {fast_search_url}, sps_userid = {sps_userid}, sps_password = {sps_password}, sps_domain = {sps_domain}')

        self.fast_search_content_source = const.APP_CONFIG['sharepoint_config']['fast_search_content_source']

        url = clr.System.String(fast_search_url)

        # pylint: disable=import-error
        from FastSearchQuery.QueryWebServiceProxy import QueryService
        # クエリWebサービスをインスタンス化します。
        queryService = QueryService(url)

        # SPSサービスアカウントの資格情報を使用します。
        queryService.Credentials = clr.System.Net.NetworkCredential(clr.System.String(sps_userid), clr.System.String(sps_password), clr.System.String(sps_domain))

        self._service = queryService

    def Close(self) -> None:
        self.logger.debug('SharePoint接続を閉じます。')
        if self._service:
            self._service.Dispose()
            self._service = None

    def FindAccount(self, customername, addresscode) -> dict:
        '''
        顧客事業所を検索する

        Args:
            customername: お客様名
            addresscode: 住所コード
        Returns:
            検索結果が存在しない場合、Noneを返却する
            検索結果が存在する場合、最初のレコードdictで返却する
        '''
        self.logger.debug('お客様FAST検索を実施します。')

        contentsource = self.fast_search_content_source

        fql = f"""and(customername:string("{customername}", mode="and"),accountaddresscode:string("{addresscode}", mode="and"),contentsource:equals("{contentsource}"))"""

        query = """<QueryPacket xmlns='urn:Microsoft.Search.Query'>
    <Query>
        <SupportedFormats>
            <Format revision='1'>urn:Microsoft.Search.Response.Document:Document</Format>
        </SupportedFormats>
        <Context>
            <QueryText language='en' type='FQL'>%s</QueryText>
        </Context>
        <ResultProvider>FASTSearch</ResultProvider>
        <Range>
            <Count>1000</Count>
            <StartAt>1</StartAt>
        </Range>
        <Properties>
            <Property name='customerid'/>
            <Property name='customername'/>
            <Property name='customernamesort'/>
            <Property name='accountaddresscode'/>
            <Property name='accountaddress'/>
        </Properties>
        <SortByProperties>
            <SortByProperty name='customernamesort' direction='Ascending'/>
            <SortByProperty name='accountaddresscode' direction='Ascending'/>
        </SortByProperties>
    </Query>
</QueryPacket>
""" % fql

        self.logger.debug(query)
        queryResults = self._service.QueryEx(query)
        customerRow = queryResults.Tables["RelevantResults"]
        totalRows = customerRow.ExtendedProperties.get_Item("TotalRows")
        self.logger.debug(f'total rows:{totalRows}, rows:{customerRow.Rows.Count}')
        for row in customerRow.Rows:
            self.logger.debug(f'row:{row}')
            return {
                'customerid':row['customerid'],                  # 企業ID
                'customername':row['customername'],              # お客様名
                'customernamesort':row['customernamesort'],      # 検索ソート順
                'accountaddresscode':row['accountaddresscode'],  # 顧客事業所住所コード
                'accountaddress':row['accountaddress']           # 顧客事業所住所
                }
        return None
