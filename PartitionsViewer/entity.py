
class KeyRange(object):

    MAX_KEY = 9223372036854775807

    def __init__(self, start=0, end=MAX_KEY):
        self.start = start
        self.end = end

    def __str__(self):
        return "KeyRange{" + str(self.start) + " - " + str(self.end) + "}"


class ColumnRange(object):

    UNBOUNDED_FROM = "null_from"

    UNBOUNDED_TO = "null_to"

    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __str__(self):
        return "ColumnRange{" + str(self.start) + " - " + str(self.end) + "}"


class Fragment(object):

    def __init__(self, key_range, column_range, storage_unit_id):
        self.key_range = key_range
        self.column_range = column_range
        self.storage_unit_id = storage_unit_id

    def __str__(self):
        return "Fragment{keyRange=" + str(self.key_range) + ", columnRange=" + str(self.column_range) + ", du = " + self.storage_unit_id + "}"


class StorageEngine(object):

    def __init__(self, id, ip, port):
        self.id = id
        self.ip = ip
        self.port = port

    def __str__(self):
        return "StorageEngine{id=" + str(self.id) + ", ip=" + self.ip + ", port=" + str(self.port) + "}"


class StorageUnit(object):

    def __init__(self, id, master_id, storage_id):
        self.id = id
        self.master_id = master_id
        self.storage_id = storage_id


    def is_master(self):
        return self.id == self.master_id

    def __str__(self):
        return "StorageUnit{id=" + self.id + ", masterId=" + self.master_id + ", storageId=" + str(self.storage_id) + "}"