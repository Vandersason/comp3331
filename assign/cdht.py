#!/usr/bin/python2.7
import sys
import socket
import time
import threading
import re
import os
import signal
from collections import defaultdict


successor = defaultdict(dict)
parent = defaultdict(dict)
request = defaultdict(dict)
respond = defaultdict(dict)

peer = int(sys.argv[1])
successor[1] = int(sys.argv[2])
successor[2] = int(sys.argv[3])
parent[1] = 0
parent[2] = 0
request[1] = 0
request[2] = 0
respond[1] = 0
respond[2] = 0



serverName = 'localhost'
peerPort = 50000 + int(peer)
address = (serverName,peerPort)

#-------------proceed input string---------------------------
def getFileNum(name):
	fileValid = re.match('^request ([0-9]{4})$',name) 
	if fileValid is None:
		print 'invalid file!'
		return
	else:
		hashName = fileValid.group(1)
		return hashName

#--------------get peer 's parents---------------------------
def getParent(message):
	an = re.match('^A ping request message was received from Peer ([0-9]{1,3})',message)
	if an is not None:
		pre = int(an.group(1))
		if parent[1] == 0 and parent[2] == 0:
			parent[1] = pre
		elif parent[1] != 0 and parent[2] == 0:
			parent[2] = pre
		elif parent[1] == pre or parent[2] == pre:
			pass
		elif parent[1] != pre and parent[2] != pre:	
			parent[1] = pre
			parent[2] = 0
	if parent[1] != 0 and parent[2] != 0:
		if parent[1] < peer and parent[2] < peer:
			if parent[1] < parent[2]:
				temp = parent[2]
				parent[2] = parent[1]
				parent[1] = temp
		elif parent[1] > peer and parent[2] > peer:
			if parent[1] < parent[2]:
                                temp = parent[2]
                                parent[2] = parent[1]
                                parent[1] = temp
		elif (parent[1] > peer and parent[2] < peer) or (parent[1] < peer and parent[2] > peer):
			if parent[1] > parent[2]:		
                                temp = parent[2]
                                parent[2] = parent[1]
                                parent[1] = temp

#------------setup server------------------------------------
def server():
	global flag
	serverSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	serverSocket.bind((serverName,peerPort))
	print "UDP server is ready to recieve\n"
	while flag == 0:
		mess, (address,port) = serverSocket.recvfrom(2048)
		getParent(mess)
		print 'parent 1 is ' + str(parent[1])
		print 'parent 2 is '+ str(parent[2])
		print 'successor 1 is '+str(successor[1])
                print 'successor 2 is '+str(successor[2])
		an = re.match('.*,([0-9]{1,}).',mess)
		if an:
                	mess = re.sub(',[0-9]{1,}','',mess)
			print mess
			sequence = an.group(1)
			mess = 'A ping response message was received from Peer '+ str(peer) + ','+ sequence +'.\n'
			serverSocket.sendto(mess,(address,port))
	serverSocket.close()
#------------setup client------------------------------------
def client(n):
	global flag
	while flag == 0:
		time.sleep(10)	
		clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		clientSocket.settimeout(5)
		message = 'A ping request message was received from Peer '+ str(peer) +','+ str(request[n])+'.\n'
		request[n] = request[n] + 1
		serverPort = 50000 + int(successor[n])
		clientSocket.sendto(message,(serverName,serverPort))
		try:
			response, serverAddress = clientSocket.recvfrom(2048)
		except socket.error:
			print 'time out!!!!!!'
		if response:
			an = re.match('.*,([0-9]{1,}).',response)
			response = re.sub(',[0-9]{1,}','',response)
			if an:
				seq = int(an.group(1))
				if seq > respond[n]:
					respond[n] = seq
			print response
#		print 'request================= '+ str(successor[n])+ '  ' + str(request[n])
#		print 'response================' + str(successor[n])+ '  '+ str(respond[n])
		clientSocket.close()


#--------------------TCP server---------------------------------------
def serverTCP():
	global flag
	global serverSetUp
	global important
	serverSocketTCP = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	serverConn = False
	while not serverConn:
		try:
			serverSocketTCP.bind((serverName,peerPort))
			serverConn = True
			serverSocketTCP.listen(5)
			serverSetUp = 0
			print 'The TCP server is ready to receive'
		except:
			pass
	while flag == 0:
		connectionSocket, addr = serverSocketTCP.accept()
		threeinfo = connectionSocket.recv(1024)
		if re.match('^FILE_REQUEST',threeinfo):
			obj = re.match('^FILE_REQUEST([0-9]{4}),([0-9]{1,3}),([0-9]{1,3}),([01])$',threeinfo)
			if obj is not None:
				filename = obj.group(1)
				hashn = int(obj.group(2))
				peerID = int(obj.group(3))
				endCircle = int(obj.group(4))

				if (endCircle and peer >= hashn) or (not endCircle and hashn < 500 and peer <= hashn):
                                        print 'File '+ filename+' is here.'
                                        important = 'FIND'+str(peer)+','+ filename +','+ str(peerID)
				elif not endCircle and hashn > 500:
					hashn = hashn - 500
					if peer >= hashn:
						print '============================='
	                                        print 'File '+ filename+' is here.'
	                                        important = 'FIND'+str(peer)+','+ filename +','+ str(peerID)
					else:
						print '========================================'
	                                        print 'File ' +filename +' is not stored here. '
	                                        important = threeinfo

				else:
					print '========================================='
					print 'File ' +filename +' is not stored here. '
					important = threeinfo
					if peer > successor[1]:
						important = re.sub('1$','0',threeinfo)
		elif re.match('^FIND',threeinfo):
			dest = re.match('^FIND([0-9]{1,3}),([0-9]{4})',threeinfo)
			fromP = dest.group(1)
			fileP = dest.group(2)
			print 'Received a response message from peer '+fromP+', which has the file '+fileP

		elif re.match('^QUIT_INFO',threeinfo):
			if re.match('^QUIT_INFO_ONE',threeinfo):
				su = re.match('^QUIT_INFO_ONE([0-9]{1,3}),([0-9]{1,3})',threeinfo)
				successor[2] = int(su.group(1))
				depart = su.group(2)
			elif re.match('^QUIT_INFO_TWO',threeinfo):
				su = re.match('^QUIT_INFO_TWO([0-9]{1,3}),([0-9]{1,3}),([0-9]{1,3})',threeinfo)
				successor[1] =  int(su.group(1))
                                successor[2] =  int(su.group(2))
				depart = su.group(3)
                        print '==============================================='
			print 'Peer '+ depart +' will depart from the network.' 
			print 'My first successor is now peer '+ str(successor[1]) +'.'
                        print 'My second successor is now peer '+ str(successor[2]) +'.'
			print '==============================================='
			connectionSocket.send('OK')
		elif re.match('^ASK_SUCC$',threeinfo):
			info = str(successor[1])
			connectionSocket.send(info)
		connectionSocket.close()
	serverSocketTCP.close()
#--------------------TCP client----------------------------------------
def clientTCP():
	global flag
	global important
        nr = threading.Thread(target=inputFrame,args=())
	mr = threading.Thread(target=impoFrame,args=())
        ca = threading.Thread(target=checkAlive,args=())

	nr.start()
	mr.start()
	ca.start()

def inputFrame():
	global flag
	global important
	while flag == 0:
		clientConn = False
	        serverPortTCP = 50000 + successor[1]
		command = raw_input()
		if command:
			if re.match('^request',command):
	                	clientSocketTCP = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	                	while not clientConn:
        	                	try:
	                                	clientSocketTCP.connect((serverName,serverPortTCP))
        	                        	clientConn = True
	                                #	print "Now client connection works!!!!!"
					except:
						pass
				hashname = getFileNum(command)
				if hashname is not None:
					hashn = re.sub('^(0)*','',hashname)
					hashnum = int(hashn) % 256
				#	print 'hashnum is '+ str(hashnum)
					if hashnum < peer:
						hashnum = hashnum + 500
				#		print 'now it is '+str(hashnum)
					info = 'FILE_REQUEST'+ hashname + ','+ str(hashnum) + ','+ str(peer) + ',1'
					clientSocketTCP.send(info)
					print 'File request message for '+ str(hashname) + ' has been sent to my successor.'
				clientSocketTCP.close()
			elif re.match('^quit$',command):
				print 'now deal with quit'
				t1 = threading.Thread(target=tellParent,args=(parent[1],))
			        t2 = threading.Thread(target=tellParent,args=(parent[2],))
				t1.start()
				t2.start()
				t1.join()
				t2.join()
				flag = 1
				print 'Peer '+ str(peer) +' will depart from the network.'

def impoFrame():
	global important
	global flag
	while flag == 0:
		clientConn = False
	        serverPortTCP = 50000 + successor[1]
		if important:
			clientSocketTCP = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        if re.match('^FILE_REQUEST([0-9]{4}),',important):

	                        while not clientConn:
        	                        try:
                	                        clientSocketTCP.connect((serverName,serverPortTCP))
                        	                clientConn = True
                        	        #        print "Now client connection works!!!!!"
                        	        except:
                                	        pass
				an = re.match('^FILE_REQUEST([0-9]{4}),',important)
				hashname = an.group(1)
				clientSocketTCP.send(important)
                        	print 'File request message for '+ str(hashname) + ' has been sent to my successor.'
			elif re.match('^FIND',important):
                                obj = re.match('^FIND[0-9]{1,3},([0-9]{4}),([0-9]{1,3})',important)
                                n = int(obj.group(2))
				port = 50000+ n
                                info = important
				while not clientConn:
					try:
						clientSocketTCP.connect((serverName,port))
						clientConn = True
					#	print "Now client connection works!!!!!"
					except:
						pass				
				clientSocketTCP.send(info)
        			print 'A response message, destined for peer '+ str(n) +', has been sent.'
				print '==================================================================='
			important = ''
                        clientSocketTCP.close()




#--------------------------------------------------------abandon
def checkAlive():
	global flag
	while flag == 0:
		index = 0
		a = request[1] - respond[1]
		b = request[2] - respond[2]
		if a > 4:
			index = 2
			print '================================================='
			print 'Peer '+ str(successor[1]) +' is no longer alive.'
			print '================================================='	
		        ao = threading.Thread(target=askOne,args=())
			ao.start()
			ao.join()
                        request[1] = 0
                        respond[1] = 0
		elif int(b) > 4:
			index = 1
			print '================================================='
                        print 'Peer '+ str(successor[2]) +' is no longer alive.'
			print '================================================='
        		at = threading.Thread(target=askTwo,args=())
			at.start()
			at.join()
			request[2] = 0
			respond[2] = 0
#-------for su[1] killed----
def askTwo():
	clientSocketTCP = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	clientConn = False
	serverPortTCP = 50000 + successor[1]
	while not clientConn:
        	try:
      	 	       	clientSocketTCP.connect((serverName,serverPortTCP))
                       	clientConn = True
                #       	print "Now client connection works!!!!!"
		except:
			pass
	info = 'ASK_SUCC'
	clientSocketTCP.send(info)
	right = False
	while not right:
        	m = clientSocketTCP.recv(1024)
		if m:
			su2 = int(m)
			if su2 != successor[2]:
				right = True
	successor[2] = su2
	clientSocketTCP.close()
#-------------for su[2]----------------------
def askOne():
	clientSocketTCP = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	clientConn = False
	serverPortTCP = 50000 + successor[2]
	while not clientConn:
        	try:
      	 	       	clientSocketTCP.connect((serverName,serverPortTCP))
                       	clientConn = True
#                       	print "Now client connection works!!!!!"
		except:
			pass
        info = 'ASK_SUCC'
        clientSocketTCP.send(info)
        m = clientSocketTCP.recv(1024)
        successor[1] = successor[2]
	successor[2] = int(m)

        clientSocketTCP.close()


def tellParent(n):
	
	clientSocketTCP = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	clientConn = False
	serverPortTCP = 50000 + int(n)
	while not clientConn:
        	try:
      	 	       	clientSocketTCP.connect((serverName,serverPortTCP))
                       	clientConn = True
#                       	print "Now client connection works!!!!!"
		except:
			pass
	if parent[1] == int(n):
		info = 'QUIT_INFO_TWO'+ str(successor[1]) +','+ str(successor[2])+','+str(peer)
	elif parent[2] == int(n):
		info = 'QUIT_INFO_ONE'+ str(successor[1])+','+str(peer)
	clientSocketTCP.send(info)
	m = clientSocketTCP.recv(1024)
	while not m:
		pass
	clientSocketTCP.close()	 

#----------------start thread---------------------------------
#------adapt from https://www.tutorialspoint.com/python/python_multithreading.html --------
flag = 0
serverSetUp = 1
important = ''
findFile = False
oneTask = False
try:
	serUDP = threading.Thread(target=server,args=())
	cliUDPOne = threading.Thread(target=client,args=(1,))
	cliUDPTwo = threading.Thread(target=client,args=(2,))
	serTCP = threading.Thread(target=serverTCP,args=())
	cliTCP = threading.Thread(target=clientTCP,args=())
	serUDP.start()
	cliUDPOne.start()
	cliUDPTwo.start()
	serTCP.start()
	cliTCP.start()
except:
	print "thread can not be set up"
while flag == 0:
	try:
		sys.stdout.write('')
	except KeyboardInterrupt:
		flag = 1
		print "Peer "+ str(peer) + " is killed"
		os.kill(os.getppid(), signal.SIGHUP)









