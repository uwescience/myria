from __future__ import print_function
import sys
import types
import struct
import cPickle
import itertools
import cloud
import base64


#pickling protocol to use
protocol = 2

class SpecialLengths(object):
  PYTHON_EXCEPTION_THROWN = -3
  END_OF_STREAM = -4
  NULL = -5

class DataType(object):
     INT =1
     LONG = 2
     FLOAT = 3
     DOUBLE = 4
     BYTES = 5

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
    return struct.unpack("!i",obj)[0]

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
         print("this is a command!")
         if (length == SpecialLengths.NULL):
             print("got a null value")
             return 0
         else:
             return None
     obj = stream.read(length)
     if len(obj) < length:
         raise EOFError

     return obj

  def write_tuple(self, stream, tuptype, tuplesize):
      if(len(tuptype)!=tuplesize):
         raise ValueError("type list is not the same as tuple size")

  def read_item(self, stream,elementType,length):
    if(elementType ==DataType.INT):
        obj = read_int(stream)
    elif(elementType == DataType.LONG):
        obj = read_long(stream)
    elif(elementType == DataType.FLOAT):
        obj = read_float(stream)
    elif(elementType == DataType.DOUBLE ):
        obj  = read_double(stream)
    elif(elementType == DataType.BYTES):
        obj = self.loads(stream.read(length))

    return obj


  def read_tuple(self, stream, tuplesize):
      datalist= []
      print ("length is  tuple is "+str(tuplesize))
      for i in range (tuplesize):
          #first element read type
          elementType = read_int(stream)
          print("type : " + str(elementType))
          #Second read the length
          length = read_int(stream)
          print("length of item : " + str(length))

          if (length == -5 ):
              print("this element is null")
              print("tuple item "+ str(i))
              datalist.append(0)
          #length is >0, read the item now
          elif (length > 0):
              print("tuple item "+ str(i))
              obj = self.read_item(stream, elementType, length)
              datalist.append(obj)

      print ( "datalist length "+str(len(datalist)))
      return datalist



  def _read_command(self,stream):
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
      return cPickle.dumps(obj,protocol)

  def loads(self,obj):
      return cPickle.loads(obj)
