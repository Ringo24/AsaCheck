# 標準ライブラリインポート
import logging
import uuid

# サードパーティライブラリインポート
import clr

# プロジェクトライブラリインポート
from . import const
from . import utils
from .dto.dcrm_sdk import Entity
from .dto.dcrm_sdk import EntityReference
from .dto.dcrm_sdk import OptionSetValue

# Dynamics CRM関連DLLがロードされたか表すフラグ
__dll_loaded__: bool = False

class DcrmHelper(object):
    '''
    Dynamics CRMを操作するためのHelperクラスです
    '''

    def __init__(self):
        '''
        初期化関数
        '''
        self._logger: logging.Logger = utils.getLogger()
        self.loadDlls()
        self._service = None # clr.Microsoft.Xrm.Client.Services.OrganizationService

    @property
    def logger(self) -> logging.Logger:
        '''
        ロガー
        '''

        return self._logger

    def loadDlls(self) -> None:
        '''
        .NET ライブラリ及びDynamics CRM SDKをロードします。
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

            # Dynamics CRM利用に必要なファイルをインポートする
            self.logger.debug('外部DLLファイルをロードします。')
            clr.AddReference(str(const.APP_HOME_PATH / 'dll/microsoft.identitymodel.dll'))
            clr.AddReference(str(const.APP_HOME_PATH / 'dll/Microsoft.Xrm.Sdk.dll'))
            clr.AddReference(str(const.APP_HOME_PATH / 'dll/Microsoft.Xrm.Sdk.Deployment.dll'))
            clr.AddReference(str(const.APP_HOME_PATH / 'dll/Microsoft.Crm.Sdk.Proxy.dll'))
            clr.AddReference(str(const.APP_HOME_PATH / 'dll/Microsoft.Xrm.Client.dll'))

            # DLL読込済み
            __dll_loaded__ = True

    def Conn(self) -> None:
        '''
        Dynamics CRMに接続します
        '''

        self.logger.debug('Dynamics CRMを接続します。')

        ntteast_conn_url = const.APP_CONFIG['crm_config']['ntteast_conn_url']
        ntteast_conn_use_id_password_authentication = const.APP_CONFIG['crm_config']['ntteast_conn_use_id_password_authentication']
        ntteast_conn_id = const.APP_CONFIG['crm_config']['ntteast_conn_id']
        ntteast_conn_pw = const.APP_CONFIG['crm_config']['ntteast_conn_pw']

        self.logger.debug(f'ntteast_conn_url = {ntteast_conn_url}, ntteast_conn_use_id_password_authentication = {ntteast_conn_use_id_password_authentication}, ntteast_conn_id = {ntteast_conn_id}, ntteast_conn_pw = {ntteast_conn_pw}')

        url = clr.System.String('Url=%s' % ntteast_conn_url)
        connection = clr.Microsoft.Xrm.Client.CrmConnection.Parse(url)
        if ntteast_conn_use_id_password_authentication:
            authCredentials = clr.Microsoft.Xrm.Sdk.Client.AuthenticationCredentials()
            authCredentials.ClientCredentials.UserName.UserName = ntteast_conn_id
            authCredentials.ClientCredentials.UserName.Password = ntteast_conn_pw
            connection.ClientCredentials = authCredentials.ClientCredentials

        self._service = clr.Microsoft.Xrm.Client.Services.OrganizationService(connection)
        whoamiRequest = clr.Microsoft.Crm.Sdk.Messages.WhoAmIRequest()
        whoamiResponse = self._service.Execute(whoamiRequest)

        self.logger.debug(f'UserId = {whoamiResponse.UserId}')

        self.logger.debug('Dynamics CRMに接続しました。')

    def Close(self) -> None:
        self.logger.debug('Dynamics CRMを閉じます。')
        if self._service:
            self._service.Dispose()
            self._service = None
        self.logger.debug('Dynamics CRMを閉じました。')

    def UpdateEntity(self, entity: Entity) -> None:
        '''
        エンティティを更新します

        Args:
            entity: Entity python wrapper object

        '''
        crmEntity = self.ConvertEntityToClrEntity(entity)
        self._service.Update(crmEntity)

    def CreateEntity(self, entity: Entity) -> uuid.UUID:
        '''
        エンティティを作成します

        Args:
            entity: Entity python wrapper object

        Returns:
            作成されたEntityのGuid
        '''
        crmEntity = self.ConvertEntityToClrEntity(entity)
        crmGuid = self._service.Create(crmEntity)
        return uuid.UUID(str(crmGuid))

    def ConvertEntityToClrEntity(self, entity:Entity) -> object:
        '''
        Entityをclr形式のEntityに変換します

        Args:
            entity: Entity python wrapper object

        Returns:
            Microsoft.Xrm.Sdk.Entity object
        '''
        crmEntity = clr.Microsoft.Xrm.Sdk.Entity()
        crmEntity.LogicalName = clr.System.String(entity.LogicalName or '')
        crmEntity.Id = clr.System.Guid(str(entity.Id))
        for attrKey, attrValue in entity.Attributes.items():
            if isinstance(attrValue, EntityReference):
                crmEntity.Attributes.Add(attrKey, self.ConvertEntityReferenceToClrEntityReference(attrValue))
            elif isinstance(attrValue, OptionSetValue):
                crmEntity.Attributes.Add(attrKey, self.ConvertOptionSetValueToClrOptionSetValue(attrValue))
            else:
                crmEntity.Attributes.Add(attrKey, attrValue)
        return crmEntity

    def ConvertEntityReferenceToClrEntityReference(self, entityRefrence:EntityReference) -> object:
        '''
        EntityReferenceをclr形式のEntityReferenceに変換します

        Args:
            entity: EntityReference python wrapper object

        Returns:
            Microsoft.Xrm.Sdk.EntityReference object
        '''

        crmEntityRefrence = clr.Microsoft.Xrm.Sdk.EntityReference()
        crmEntityRefrence.LogicalName = clr.System.String(entityRefrence.LogicalName or '')
        crmEntityRefrence.Name = clr.System.String(entityRefrence.Name or '')
        crmEntityRefrence.Id = clr.System.Guid(str(entityRefrence.Id))
        return crmEntityRefrence

    def ConvertOptionSetValueToClrOptionSetValue(self, optionset:OptionSetValue) -> object:
        '''
        OptionSetValueをclr形式のOptionSetValueに変換します

        Args:
            entity: OptionSetValue python wrapper object

        Returns:
            Microsoft.Xrm.Sdk.OptionSetValue object
        '''
        crmOptionSetValue = clr.Microsoft.Xrm.Sdk.OptionSetValue()
        crmOptionSetValue.Value = clr.System.Int32(optionset.Value)
        return crmOptionSetValue
