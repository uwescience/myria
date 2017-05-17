from __future__ import print_function
import os
import sys
import socket
import traceback
import struct
import cPickle
import base64


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


class Serializer(object):

    @staticmethod
    def read_long(stream):
        obj = stream.read(8)
        if not obj:
            raise EOFError
        return struct.unpack("!q", obj)[0]

    @staticmethod
    def read_float(stream):
        obj = stream.read(4)
        if not obj:
            raise EOFError
        return struct.unpack("!f", obj)[0]

    @staticmethod
    def read_double(stream):
        obj = stream.read(8)
        if not obj:
            raise EOFError
        return struct.unpack("!d", obj)[0]

    @staticmethod
    def read_int(stream):
        obj = stream.read(4)
        if not obj:
            raise EOFError
        return struct.unpack("!i", obj)[0]

    @staticmethod
    def write_int(value, stream):
        stream.write(struct.pack("!i", value))

    @staticmethod
    def write_float(value, stream):
        stream.write(struct.pack("!f", value))

    @staticmethod
    def write_double(value, stream):
        stream.write(struct.pack("!d", value))

    @staticmethod
    def write_long(value, stream):
        stream.write(struct.pack("!q", value))


class PickleSerializer(Serializer):

    @classmethod
    def read_item(cls, stream, item_type, length):
        obj = None
        if item_type == DataType.INT:
            obj = cls.read_int(stream)
        elif item_type == DataType.LONG:
            obj = cls.read_long(stream)
        elif item_type == DataType.FLOAT:
            obj = cls.read_float(stream)
        elif item_type == DataType.DOUBLE:
            obj = cls.read_double(stream)
        elif item_type == DataType.BLOB:
            obj = cls.loads(stream.read(length))
        return obj

    @classmethod
    def read_tuple(cls, stream, tuplesize):
        datalist = []
        for i in range(tuplesize):
            # first element read type
            element_type = cls.read_int(stream)
            # Second read the length
            length = cls.read_int(stream)

            if length == SpecialLengths.NULL or length == 0:
                datalist.append(0)
            # length is > 0, read the item now
            elif length > 0:
                obj = cls.read_item(stream, element_type, length)
                datalist.append(obj)
            else:
                raise ValueError("Invalid length for item.")

        return datalist

    @classmethod
    def write_with_length(cls, obj, stream, output_type):
        if output_type == DataType.INT:
            cls.write_int(DataType.INT, stream)
            cls.write_int(obj, stream)
        elif output_type == DataType.LONG:
            cls.write_int(DataType.LONG, stream)
            cls.write_long(obj, stream)
        elif output_type == DataType.FLOAT:
            cls.write_int(DataType.FLOAT, stream)
            cls.write_float(obj, stream)
        elif output_type == DataType.DOUBLE:
            cls.write_int(DataType.DOUBLE, stream)
            cls.write_double(obj, stream)
        elif output_type == DataType.BLOB:
            cls.write_int(DataType.BLOB, stream)
            cls.pickle_and_write(obj, stream)

    @classmethod
    def read_command(cls, stream):
        length = cls.read_int(stream)

        if length < 0:
            raise ValueError("Command cannot be less than zero.")
        s = stream.read(length)

        if len(s) < length:
            raise EOFError
        unenc = base64.urlsafe_b64decode(s)
        return cls.loads(unenc)

    @staticmethod
    def dumps(obj):
        protocol = 2
        return cPickle.dumps(obj, protocol)

    @staticmethod
    def loads(obj):
        return cPickle.loads(obj)

    @classmethod
    def pickle_and_write(cls, obj, stream):
        serialized = cls.dumps(obj)

        if serialized is None:
            raise ValueError("Serialized value should not be None.")
        elif len(serialized) > (1 << 31):
            raise ValueError("Cannot serialize object larger than 2G.")

        cls.write_int(len(serialized), stream)
        stream.write(serialized)


def main(in_file, out_file):
    pickle_serializer = PickleSerializer()
    try:
        func = pickle_serializer.read_command(in_file)
        tuple_size = pickle_serializer.read_int(in_file)
        output_type = pickle_serializer.read_int(in_file)
        is_flatmap = pickle_serializer.read_int(in_file)

        if tuple_size < 1:
            raise ValueError("Size of tuple should not be less than 1.")

        while True:
            num_tuples = pickle_serializer.read_int(in_file)
            if num_tuples == SpecialLengths.END_OF_STREAM:
                break

            tuple_list = []
            for j in range(num_tuples):
                tuple_list.append(pickle_serializer.read_tuple(in_file, tuple_size))

            retval = func(tuple_list)
            if is_flatmap > 0:
                count = len(retval)
                pickle_serializer.write_int(count, out_file)
                for i in range(count):
                    pickle_serializer.write_with_length(
                        retval[i], out_file, output_type)
            else:
                pickle_serializer.write_with_length(retval, out_file, output_type)

            out_file.flush()

    except Exception:
        try:
            pickle_serializer.write_int(SpecialLengths.PYTHON_EXCEPTION_THROWN, out_file)
            pickle_serializer.write_with_length(traceback.format_exc().encode(
                "utf-8"), out_file, DataType.BLOB)
            print(traceback.format_exc(), file=sys.stderr)
        except IOError:
            # JVM closed the socket
            print("IOError: \n{}".
                  format(traceback.format_exc()), file=sys.stderr)
        except Exception:
            print("Python worker process failed with exception:\n{}".
                  format(traceback.format_exc()), file=sys.stderr)
        exit(-1)


if __name__ == '__main__':
    # Read a local port to connect to from stdin
    port_number = int(sys.stdin.readline())
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", port_number))
    with os.fdopen(os.dup(sock.fileno()), "rb", 65536) as infile,\
            os.fdopen(os.dup(sock.fileno()), "wb", 65536) as outfile:
        main(infile, outfile)

