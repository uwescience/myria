from __future__ import print_function
import os
import sys
import time
import socket
import traceback
import struct

from MyriaPythonWorker.serializers import read_int, write_int,SpecialLengths,write_with_length,PickleSerializer

pickleSer = PickleSerializer()


def main(infile, outfile):
    try:
        #get code

        func = pickleSer.read_command(infile)
        print("read command!")
        tuplesize = read_int(infile)
        print("read tuple size")
        outputType = read_int(infile)
        print("read output type")
        isFlatmap = read_int(infile)

        if tuplesize < 1:
            raise ValueError("size of tuple should not be less than 1 ")

        print(tuplesize)
        print(isFlatmap)
        while True:
            print("python process trying to read tuple")
            tup =pickleSer.read_tuple(infile,tuplesize)

            print("python process done reading tuple, now writing ")
            retval = func(tup)
            if(isFlatmap>0):
                count = len(retval)
                write_int(count,outfile)
                for i in range(count):
                    write_with_length(retval[i],outfile, outputType,pickleSer)
            else:
                write_with_length(retval, outfile, outputType, pickleSer)

            outfile.flush()


    except Exception:
        try:
            write_int(SpecialLengths.PYTHON_EXCEPTION_THROWN,outfile)
            write_with_length(traceback.format_exc().encode("utf-8"),outfile,5,pickleSer)
            print(traceback.format_exc(), file=sys.stderr)
        except Exception:
            print("python process failed with exception: ", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)



if __name__ == '__main__':
    # Read a local port to connect to from stdin

    java_port = int(sys.stdin.readline())
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", java_port))
    infile = os.fdopen(os.dup(sock.fileno()), "rb", 65536)
    outfile = os.fdopen(os.dup(sock.fileno()), "wb", 65536)
    exit_code = 0

    try:
        main(infile,outfile)
    except SystemExit as exc:
        exit_code = exc.code
    finally:
        outfile.flush()
        sock.close()
