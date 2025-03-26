# 標準ライブラリインポート
import logging
import uuid
import dataclasses
import decimal
from abc import abstractmethod
from typing import Dict
from typing import Iterator

# サードパーティライブラリインポート

# プロジェクトライブラリインポート
from .. import const
from .. import utils

@dataclasses.dataclass
class EntityReference(object):
    '''
    Dynamics CRM SDK EntityReference ClassのWrapper Class
    '''
    Id: uuid.UUID
    LogicalName: str
    Name: str

    def __init__(self, logicalName: str = None, id: uuid.UUID = None):
        '''
        初期化関数

        Args:
            logicalName: エンティティの物理名
            id: エンティティのGUID(UUID)
        '''
        self.LogicalName = logicalName
        self.Id = id
        self.Name = None

@dataclasses.dataclass
class OptionSetValue(object):
    '''
    Dynamics CRM SDK OptionSetValue ClassのWrapper Class
    '''
    Value: int

    def __init__(self, value: int):
        '''
        初期化関数

        Args:
            value: Picklistの値（int値）
        '''
        self.Value = value

@dataclasses.dataclass
class Entity(object):
    '''
    Dynamics CRM SDK Entity ClassのWrapper Class
    '''
    Id: uuid.UUID
    LogicalName: str
    Attributes: Dict[str, object]

    def __init__(self, entityName: str = None):
        '''
        初期化関数

        Args:
            entityName: エンティティの物理名
        '''
        self.LogicalName = entityName
        self.Id = const.EMPTY_UUID
        self.Attributes = {}

    def ToEntityReference(self) -> EntityReference:
        '''
        EntityReferenceに変換する
        '''
        return EntityReference(self.LogicalName, self.Id)

    def __contains__(self, key: str) -> bool:
        '''
        属性が存在する場合、Trueを返却する
        '''
        return self.Attributes.__contains__(key)

    def __getitem__(self, key: str) -> object:
        '''
        属性を取得する
        '''
        return self.Attributes.__getitem__(key)

    def __setitem__(self, key: str, value: object) -> None:
        '''
        属性を設定する
        '''
        self.Attributes.__setitem__(key, value)

    def __delitem__(self, key: str) -> None:
        '''
        属性を削除する
        '''
        self.Attributes.__delitem__(key)

    def __iter__(self) -> Iterator[str]:
        '''
        属性のIteratorを返却する
        '''
        return self.Attributes.__iter__()

    def __len__(self) -> int:
        '''
        要素数を返却する
        '''
        return len(self.Attributes)

@dataclasses.dataclass
class Money(object):
    '''
    Dynamics CRM SDK Money ClassのWrapper Class
    '''
    Value: decimal.Decimal

    def __init__(self, value: decimal.Decimal):
        '''
        初期化関数

        Args:
            value: Picklistの値（Decimal値）
        '''
        self.Value = value

@dataclasses.dataclass
class AliasedValue(object):
    '''
    Dynamics CRM SDK AliasedValue ClassのWrapper Class
    '''
    AttributeLogicalName: str
    EntityLogicalName: str
    Value: object

    def __init__(self, entityLogicalName: str = None, attributeLogicalName: str = None, value: object = None):
        '''
        初期化関数

        Args:
            entityLogicalName: The name of the entity the attribute belongs to.
            attributeLogicalName: The attribute on which the aggregate, group by, or select operation was performed.
            value: The value returned by the query.
        '''
        self.EntityLogicalName = entityLogicalName
        self.AttributeLogicalName = attributeLogicalName
        self.Value = value

