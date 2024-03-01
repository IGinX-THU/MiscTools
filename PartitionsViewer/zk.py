import json

from kazoo.client import KazooClient
from entity import KeyRange, ColumnRange, Fragment, StorageEngine, StorageUnit

class ZooKeeperMetaServer(object):

    FRAGMENT = "/fragment"

    STORAGE_ENGINE = "/storage"

    UNIT = "/unit"

    def __init__(self, hosts):
        self.zk = KazooClient(hosts=hosts)

    def start(self, timeout=10):
        self.zk.start(timeout=timeout)

    def close(self):
        self.zk.stop()
        self.zk.close()

    def get_fragments(self):
        fragments = []
        column_ranges = self.zk.get_children(ZooKeeperMetaServer.FRAGMENT)
        for column_range in column_ranges:
            start_keys = self.zk.get_children(ZooKeeperMetaServer.FRAGMENT + "/" + column_range)
            for start_key in start_keys:
                value = self.zk.get(ZooKeeperMetaServer.FRAGMENT + "/" + column_range + "/" + start_key)[0]
                data = json.loads(value)
                key_range = KeyRange(data['keyInterval']['startKey'], data['keyInterval']['endKey'])
                column_range = ColumnRange(data['columnsInterval'].get('startColumn', ColumnRange.UNBOUNDED_FROM), data['columnsInterval'].get('endColumn', ColumnRange.UNBOUNDED_TO))
                fragments.append(Fragment(key_range, column_range, data['masterStorageUnitId']))
        return fragments


    def get_storage_units(self):
        storage_units = []
        unit_name_list = self.zk.get_children(ZooKeeperMetaServer.UNIT)
        for unit_name in unit_name_list:
            value = self.zk.get(ZooKeeperMetaServer.UNIT + "/" + unit_name)[0]
            data = json.loads(value)
            storage_units.append(StorageUnit(data['id'], data['masterId'], data['storageEngineId']))
        return storage_units

    def get_storage_engines(self):
        storage_engines = []
        storage_list = self.zk.get_children(ZooKeeperMetaServer.STORAGE_ENGINE)
        for storage in storage_list:
            value = self.zk.get(ZooKeeperMetaServer.STORAGE_ENGINE + "/" + storage)[0]
            data = json.loads(value)
            storage_engines.append(StorageEngine(data['id'], data['ip'], data['port']))
        return storage_engines