import json

from kazoo.client import KazooClient

# if we have 1000 trucks, the fragments will be like:：
#       null - readings.truck_0500, readings.truck_0500 - null
#       null - diagnostics.truck_0500, diagnostics.truck_0500 - null

# 用于生成初始分片信息

scale = 8

zookeeper_ip = "127.0.0.1"
#zookeeper_ip = "172.16.17.21"
zookeeper_port = 2181

replica = 1

engines_local = [
    {
        "ip": "127.0.0.1",
        "port": 6667,
        "storageEngine": "iotdb12"
    },
    {
        "ip": "127.0.0.1",
        "port": 6668,
        "storageEngine": "iotdb12"
    },
]

engines_remote = [
    {
        "ip": "172.16.17.21",
        "port": 6324,
        "storageEngine": "iotdb12"
    },
    {
        "ip": "172.16.17.22",
        "port": 6324,
        "storageEngine": "iotdb12"
    },
    {
        "ip": "172.16.17.23",
        "port": 6324,
        "storageEngine": "iotdb12"
    },
    {
        "ip": "172.16.17.24",
        "port": 6324,
        "storageEngine": "iotdb12"
    },
]

ts_intervals = [
    {
        "closed": False,
        "normal": True,
        "prefix": False,
        "endTimeSeries": "diagnostics.truck_{:04d}.East.Albert.F_150.v1_0.a".format(scale // 2),
        "type": 1
    },
    {
        "closed": False,
        "normal": True,
        "prefix": False,
        "startTimeSeries": "diagnostics.truck_{:04d}.East.Albert.F_150.v1_0.a".format(scale // 2),
        "endTimeSeries": "readings.truck_0000.East.Albert.F_150.v1_0.a",
        "type": 1
    },
    {
        "closed": False,
        "normal": True,
        "prefix": False,
        "startTimeSeries": "readings.truck_0000.East.Albert.F_150.v1_0.a",
        "endTimeSeries": "readings.truck_{:04d}.East.Albert.F_150.v1_0.a".format(scale // 2),
        "type": 1
    },
    {
        "closed": False,
        "normal": True,
        "prefix": False,
        "startTimeSeries": "readings.truck_{:04d}.East.Albert.F_150.v1_0.a".format(scale // 2),
        "type": 1
    }
]

time_interval = {
    "endTime": 9223372036854775807,
    "span": 9223372036854775807,
    "startTime": 0
}

def format_unit_id(id):
    return 'unit' + str(id).zfill(10)


def format_storage_id(id):
    return 'node' + str(id).zfill(10)


def to_zk_path(ts_interval):
    return ts_interval.get('startTimeSeries', 'null') + '-' + ts_interval.get('endTimeSeries', 'null')

# some constants
FRAGMENT = "/fragment"
STORAGE_ENGINE = "/storage"
UNIT = "/unit"

CREATOR = 100

MAX_TIME = 9223372036854775807

unit_template = '{"createdBy":0,"dummy":false,"id":"unit0000000000","ifValid":true,"initialStorageUnit":true,' \
                '"master":true,"masterId":"unit0000000000","storageEngineId":0}'

storage_template = '{"createdBy":0,"extraParams":{"password":"root","has_data":"false","is_read_only":"false",' \
                   '"username":"root","sessionPoolSize":"150"},"hasData":false,"id":0,"ip":"127.0.0.1",' \
                   '"needReAllocate":false,"port":6667,"readOnly":false,"storageEngine":"iotdb12"}'

fragment_template = '{"createdBy":0,"dummyFragment":false,"initialFragment":true,"masterStorageUnitId":"unit0000000000",' \
                    '"timeInterval":{"endTime":9223372036854775807,"span":7771765636854775807,"startTime":1451606400000000000},' \
                    '"tsInterval":{"closed":false,"normal":true,"prefix":false,"startTimeSeries":"diagnostics.truck_0037.South.Andy.G_2000.v2_0.current_load","type":1},' \
                    '"updatedBy":0,"valid":true}'

if __name__ == '__main__':
    engines_info = engines_local
    #engines_info = engines_remote

    storage_engines = []
    for i in range(len(engines_info)):
        storage_engine = json.loads(storage_template)
        storage_engine['id'] = i
        storage_engine['createdBy'] = CREATOR

        engine_info = engines_info[i]
        storage_engine.update(engine_info)
        storage_engines.append(storage_engine)

    storage_units = []
    id = 0
    for i in range(4):
        master_id = id
        for j in range(replica + 1):
            unit = json.loads(unit_template)
            unit['createdBy'] = CREATOR
            unit['master'] = master_id == id
            unit['id'] = format_unit_id(id)
            unit['masterId'] = format_unit_id(master_id)

            storage_id = id % len(storage_engines)
            if (id // len(storage_engines)) % 2 == 1:
                storage_id = len(storage_engines) - storage_id - 1
            unit['storageEngineId'] = storage_id

            storage_units.append(unit)
            id += 1

    fragments = []
    for i in range(4):
        fragment = json.loads(fragment_template)
        fragment['createdBy'] = CREATOR
        fragment['updatedBy'] = CREATOR
        fragment['timeInterval'] = time_interval
        fragment['tsInterval'] = ts_intervals[i]
        fragment['masterStorageUnitId'] = storage_units[i * (replica + 1)]['id']
        fragments.append(fragment)

    connection_string = zookeeper_ip + ':' + str(zookeeper_port)

    print("try to connect to " + connection_string + " ...")
    zk = KazooClient(connection_string)
    zk.start()
    print("has connect to " + connection_string)

    zk.create(STORAGE_ENGINE)
    for storage_engine in storage_engines:
        zk.create(STORAGE_ENGINE + '/node', bytes(json.dumps(storage_engine), 'utf-8'), ephemeral=False, sequence=True)

    zk.create(UNIT)
    for unit in storage_units:
        zk.create(UNIT + '/unit', bytes(json.dumps(unit), 'utf-8'), ephemeral=False, sequence=True)

    zk.create(FRAGMENT)
    for fragment in fragments:
        zk.create(FRAGMENT + '/' + to_zk_path(fragment['tsInterval']))
        zk.create(FRAGMENT + '/' + to_zk_path(fragment['tsInterval']) + '/0', bytes(json.dumps(fragment, separators=(',', ':')), 'utf-8'), ephemeral=False, sequence=False)

    zk.stop()
    zk.close()
    print("close connect to " + connection_string)
