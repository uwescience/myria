from __future__ import print_function
import sys
import types
import struct
import cPickle
import itertools

#pickling protocol to use
protocol = 2

class SpecialLengths(object):
  PYTHON_EXCEPTION_THROWN = -3
  END_OF_STREAM = -4
  NULL = -5


class SpecialCommands(object):
    SUM = -1
    AVERAGE = -2

def read_long(stream):
    length = stream.read(8)
    if not length:
        raise EOFError
    return struct.unpack("!q", length)[0]

def read_int(stream):
    length = stream.read(4)
    if not length:
        raise EOFError
    return struct.unpack("!i",length)[0]

def write_int(value, stream):
    stream.write(struct.pack("!i",value))

def write_with_length(obj, stream):
    write_int(len(obj),stream)
    stream.write(obj)


class PickleSerializer(object):

  def dump_stream(self,iterator,stream):
    for obj in iterator:
      self._write_with_length(obj,stream)


  def load_stream(self,stream, size):
    while True:
      try:
        (yield self._read_with_length(stream, size))
      except EOFError:
        return


  def write_with_length(self, obj, stream):
      serialized = self.dumps(obj)

      if serialized is None:
          raise ValueError("serialized value should not be None")
      elif len(serialized)>(1<<31):
          raise ValueError("can not serialize object larger than 2G")

      write_int(len(serialized), stream)
      stream.write(serialized)


  def read_with_length(self,stream):

     length = read_int(stream)
     if length < 0:
         print("this is a command!", file=sys.err)
         return None
     obj = stream.read(length)
     if len(obj) < length:
         raise EOFError

     return obj

  def read_tuple(self,stream, tuplesize):
      datalist= []

      for i in range (tuplesize):
          length = read_int(stream)

          if (length ==-5 ):
              print("this element is null", file=sys.stderr)
              return None
          elif (length<0):
              print("this is a command!", file=sys.stderr)
              return None
          obj = stream.read(length)
          if len(obj) < length:
              raise EOFError
          datalist.append(self.loads(obj))

      return datalist

  def _read_command(self,stream):
      length = read_int(stream)

      if length < 0:
          print("this is a command!", file=sys.stderr)
          return None
      obj = stream.read(length)
      if len(obj) < length:
          raise EOFError
      return self.loads(obj)

  def dumps(self, obj):
      return cPickle.dumps(obj,protocol)

  def loads(self,obj):
      return cPickle.loads(obj)
