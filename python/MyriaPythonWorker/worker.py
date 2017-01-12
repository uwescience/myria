from __future__ import print_function
import os
import sys
import time
import socket
import traceback
import struct

from MyriaPythonWorker.serializers import read_int, write_int, SpecialLengths, write_with_length, PickleSerializer

pickleSer = PickleSerializer()


def main(infile, outfile):
    try:
        func = pickleSer.read_command(infile)
        tupleSize = read_int(infile)
        outputType = read_int(infile)
        isFlatmap = read_int(infile)

        if tupleSize < 1:
            raise ValueError("size of tuple should not be less than 1 ")

        while True:
            numTuples = read_int(infile)
            if (numTuples == SpecialLengths.END_OF_STREAM):
                break
            tupleList = []
            for j in range(numTuples):
                tupleList.append(pickleSer.read_tuple(infile, tupleSize))


            retval = func(tupleList)
            if(isFlatmap > 0):
                count = len(retval)
                write_int(count, outfile)
                for i in range(count):
                    write_with_length(
                        retval[i], outfile, outputType, pickleSer)
            else:
                write_with_length(retval, outfile, outputType, pickleSer)

            outfile.flush()

    except Exception:
        try:
            write_int(SpecialLengths.PYTHON_EXCEPTION_THROWN, outfile)
            write_with_length(traceback.format_exc().encode(
                "utf-8"), outfile, 5, pickleSer)
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
        main(infile, outfile)
    except SystemExit as exc:
        exit_code = exc.code
    finally:
        outfile.flush()
        sock.close()
