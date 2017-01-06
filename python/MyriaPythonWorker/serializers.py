from __future__ import print_function
import sys
import types
import struct
import cPickle
import itertools
import cloud
import base64


# pickling protocol to use
protocol = 2


class SpecialLengths(object):
    PYTHON_EXCEPTION_THROWN = -3
    END_OF_STREAM = -4
    NULL = -5


class DataType(object):
    INT = 1
    LONG = 2
    FLOAT = 3
    DOUBLE = 4
    BLOB = 5


def read_long(stream):
    obj = stream.read(8)
    if not obj:
        raise EOFError
    return struct.unpack("!q", obj)[0]


def read_float(stream):
    obj = stream.read(4)
    if not obj:
        raise EOFError
    return struct.unpack("!f", obj)[0]


def read_double(stream):
    obj = stream.read(8)
    if not obj:
        raise EOFError
    return struct.unpack("!d", obj)[0]


def read_int(stream):
    obj = stream.read(4)
    if not obj:
        raise EOFError
    return struct.unpack("!i", obj)[0]


def write_int(value, stream):
    stream.write(struct.pack("!i", value))


def write_float(value, stream):
    stream.write(struct.pack("!f", value))


def write_double(value, stream):
    stream.write(struct.pack("!d", value))


def write_long(value, stream):
    stream.write(struct.pack("!q", value))


def write_with_length(obj, stream, outputType, serialiser):
    if(outputType == DataType.INT):
        write_int(DataType.INT, stream)
        write_int(obj, stream)
    elif(outputType == DataType.LONG):
        write_int(DataType.LONG, stream)
        write_long(stream.write(obj))
    elif(outputType == DataType.FLOAT):
        write_int(DataType.FLOAT, stream)
        write_float(stream.write(obj))
    elif(outputType == DataType.DOUBLE):
        write_int(DataType.DOUBLE, stream)
        write_double(stream.write(obj))
    elif(outputType == DataType.BLOB):
        write_int(DataType.BLOB, stream)
        serialiser.write_with_length(obj, stream)


class PickleSerializer(object):

    def write_with_length(self, obj, stream):
        serialized = self.dumps(obj)

        if serialized is None:
            raise ValueError("Serialized value should not be None.")
        elif len(serialized) > (1 << 31):
            raise ValueError("Cannot serialize object larger than 2G.")

        write_int(len(serialized), stream)
        stream.write(serialized)

    def read_item(self, stream, itemType, length):
        obj = None
        if(itemType == DataType.INT):
            obj = read_int(stream)
        elif(itemType == DataType.LONG):
            obj = read_long(stream)
        elif(itemType == DataType.FLOAT):
            obj = read_float(stream)
        elif(itemType == DataType.DOUBLE):
            obj = read_double(stream)
        elif(itemType == DataType.BYTES):
            obj = self.loads(stream.read(length))
        return obj

    def read_tuple(self, stream, tuplesize):
        datalist = []
        for i in range(tuplesize):
            # first element read type
            elementType = read_int(stream)
            # Second read the length
            length = read_int(stream)

            if (length == SpecialLengths.NULL):
                datalist.append(0)
            # length is >0, read the item now
            elif (length > 0):
                obj = self.read_item(stream, elementType, length)
                datalist.append(obj)

        return datalist

    def read_command(self, stream):
        length = read_int(stream)

        if length < 0:
            print("command length is less than zero", file=sys.stderr)
            return None
        s = stream.read(length)

        if len(s) < length:
            raise EOFError
        unenc = base64.urlsafe_b64decode(s)
        return self.loads(unenc)

    def dumps(self, obj):
        return cPickle.dumps(obj, protocol)

    def loads(self, obj):
        return cPickle.loads(obj)
