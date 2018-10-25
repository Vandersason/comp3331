import sys
import socket
import re

'''
1. create a connection socket when contacted by a client
2. receive http request and process only GET 
3. parse and determine the file
4. get the file
5. create http response message 
6. send TCP response
7. if the file not exist, send '404 not found'

'''

def myServer():
    
    host = '127.0.0.1'
    port = int(sys.argv[1])

    mySocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    mySocket.bind((host,port))
    mySocket.listen(1)
    
    while True:
        csock, caddr = mySocket.accept()
        print("Connection from: " + `caddr`)
        request = csock.recv(1024) # get the request, 1kB max
        print(request)
        
        req = re.search('GET /(.+) HTTP/1.1', request).group(1)
        if req:
            #path = req.group(1)
            print(req)
            try:
                f = open(req)
                http_response = f.read()
                csock.send('HTTP/1.1 200 OK\n\n')
                csock.send(http_response)
                csock.close()
            
            except IOError:
                print(404)
                csock.sendall("""HTTP/1.0 200 OK
                    Content-Type: text/html

                    <html>
                    <head>
                    <title>404</title>
                    </head>
                    <body>
                    <h1>404 Not Found</h1>
                    <h3>Welcome to Trimester!!</h3>
                    </body>
                    </html>
                    """.encode())
                csock.close()
    
        
            
    


    

if  __name__ == '__main__':
    myServer()
