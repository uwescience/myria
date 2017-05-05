from myria import *
import numpy
import json
# Create a connection to the Myria *Production* cluster
connection = MyriaConnection(rest_url='http://localhost:8753')

py = myria.udf.functionTypes.PYTHON
outType= "BLOB_TYPE"

def simpleApplyTest(dt):
    image = dt[0][0]#first item of first tuple
    return image

connection.create_function("simpleApplyTest","function text",outType, py,False, simpleApplyTest)

def flatmapApplyTest(dt):
    import itertools
    image = dt[0][0] #first item of first tuple.
    [xp,yp,zp] = [4,4,4]
    datalist =[]
    [xSize,ySize,zSize] = [image.shape[0]/xp, image.shape[1]/yp, image.shape[2]/zp]
    for x,y,z in itertools.product(range(xp), range(yp), range(zp)):
        [xS, yS, zS] = [x*xSize, y*ySize, z*zSize]
        [xE, yE, zE] = [image.shape[0] if x == xp - 1 else (x+1)*xSize, \
                        image.shape[1] if y == yp - 1 else (y+1)*ySize, \
                        image.shape[2] if z == zp - 1 else (z+1)*zSize]
        datalist.append(image[xS:xE, yS:yE, zS:zE])
    return datalist

connection.create_function("flatmapApplyTest","function text",outType, py,True, flatmapApplyTest)


def udfAgg(dt):
    import numpy as np
    tuplist = dt
    state = None
    n = len(tuplist)
    for i in tuplist:
        imgid = i[1]
        subjid = i[0]
        img = np.asarray(i[2])
        if state is None:
            shape = img.shape + (5,)
            state = np.empty(shape)
            state[:,:,:,imgid]=img
        else:
            state[:,:,:,imgid]=img
    return (state)


MyriaPythonFunction(udfAgg, outType).register()



def pyAdd(dt):
    tuplist = dt
    retval = None
    for i in tuplist:
        if retval is None:
            retval = i[0]
        else:
            retval = retval+i[0]
    return retval


MyriaPythonFunction(pyAdd, outType).register()


def pyMean(dt):
    print dt
    return dt[0][0]/dt[0][1]

MyriaPythonFunction(pyMean, outType).register()


outType= "LONG_TYPE"
def pyAggInt(dt):
    return 5



connection.create_function("pyAggInt","function text",outType, py,False, pyAggInt)


def pyFlatmapInt(dt):
    return [2,3]


connection.create_function("pyFlatmapInt","function text",outType, py,True, pyFlatmapInt)
