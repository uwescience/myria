import socket;
import time
s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
s.connect(("hongkong",19980))


tenG = 10*1024*1024*1024
bufSize = 1024*512
num=tenG/bufSize

sent=0
start = time.time()
buf = "0"*bufSize

print "start send: "+str(start)

while sent<num:
  data = s.send(buf)
  sent+=1
  if sent % 1000==0:
    print sent,"sent"

end = time.time()
print "end send: "+str(end)
print "time spent : "+str(end-start)
