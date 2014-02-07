import socket
import time
serversocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
serversocket.bind((socket.gethostname(),19980))
serversocket.listen(5)
conn,addr = serversocket.accept()

tenG = 10*1024*1024*1024
bufSize = 1024*512
num=tenG/bufSize

received=0
start = time.time()
print "start receive: "+str(start)
while received<tenG:
	data = conn.recv(bufSize)
	received+=len(data)
	#print received

end = time.time()
print "end receive: "+str(end)
print "time spent : "+str(end-start)
print "speed: " + str(10*1024*1.0/(end-start))+"mega-bytes/s"
print "speed: " + str(80*1.0/(end-start))+"giga-bits/s"
