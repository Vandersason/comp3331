import sys
import socket
import time

'''
send 10 ping requests to the server, seperated by one second
payload of the data that includes 'PING', sequence num, timestamp
if 1 second goes by without a reply from server then client assumes the packet is lost

wait 1 second which is > rtt, 
non-blocking (must not wait indefinitely for a response)

send pings sequentually
compute the rtt in milliseconds
rtt should be printed in the output followed by the given format
'''

def Main():

    #host = '127.0.0.1'
    #get host address and port num
    host = str(sys.argv[1])
    port = int(sys.argv[2])
    seq_number = 0

    mySocket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    mySocket.settimeout(1)

    while seq_number < 10:
        send_time = int(round(time.time() * 1000))
        message = 'PING ' + str(seq_number) + ' ' + str(time.ctime())
        mySocket.sendto(message.encode(),(host,port))

        try:
            server_message, server_address = mySocket.recvfrom(1024)
        except socket.timeout:
            output = 'ping to '+ str(host) + ', seq = ' + str(seq_number) + ", timeout"
            print(output)
            seq_number = seq_number + 1
            continue

        receive_time = int(round(time.time() * 1000))
        total_time =  receive_time - send_time
        output = 'ping to '+ str(host) + ', seq = ' + str(seq_number) + ', ' + str(total_time)
        print(output)
        seq_number = seq_number +1
        time.sleep( 1 )
    
    print("closing socket")
    mySocket.close()
    

if  __name__ == '__main__':
    Main()


