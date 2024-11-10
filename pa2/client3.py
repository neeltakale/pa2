import socket
import sys
import time
import threading
from queue import Queue
from heapq import heapify, heappush, heappop
inputs = Queue(maxsize=20)
timestamp = 0
portnum = 9000
accessingResource = False
requests = {}
release_states = {}
tasks = []
heapify(tasks)
server_response = {}
mutex = threading.Lock()

def server1_output_handler(server1_socket, client1_socket, client2_socket):
    global accessingResource
    global tasks
    global server_response
    global mutex
    while True:
        try:
            message = server1_socket.recv(1024).decode('utf-8')
        except ConnectionResetError:
            break
        time.sleep(3)
        splitMes = message.split("\n")
        for i in splitMes:
            if i:
                iSplit = i.split(" ")
                stripped = i.lstrip("Success ")
                output = "Response from Server 1 for request " + stripped + " : " + iSplit[0]
                print(f"{output}")
                if stripped in server_response:
                    accessingResource = False
                    mutex.acquire()
                    heappop(tasks)
                    mutex.release()
                    message = "Sending release for request " + stripped + " to Client 1"
                    print(f"{message}")
                    message = "Sending release for request " + stripped + " to Client 2"
                    print(f"{message}")
                    message = "release (" + stripped + "\n"
                    client1_socket.send(message.encode('utf-8'))
                    client2_socket.send(message.encode('utf-8'))
                else:
                    server_response[stripped] = 1


def server2_output_handler(server2_socket, client1_socket, client2_socket):
    global accessingResource
    global tasks
    global server_response
    global mutex
    while True:
        try:
            message = server2_socket.recv(1024).decode('utf-8')
        except ConnectionResetError:
            break
        time.sleep(3)
        splitMes = message.split("\n")
        for i in splitMes:
            if i:
                iSplit = i.split(" ")
                if i.startswith("Success"):
                    stripped = i.lstrip("Success ")
                    output = "Response from Server 2 for request " + stripped + " : " + iSplit[0]
                    print(f"{output}")
                    if stripped in server_response:
                        accessingResource = False
                        mutex.acquire()
                        heappop(tasks)
                        mutex.release()
                        message = "Sending release for request " + stripped + " to Client 1"
                        print(f"{message}")
                        message = "Sending release for request " + stripped + " to Client 2"
                        print(f"{message}")
                        message = "release (" + stripped + "\n"
                        client1_socket.send(message.encode('utf-8'))
                        client2_socket.send(message.encode('utf-8'))
                    else:
                        server_response[stripped] = 1
                else:
                    print(i)    
                    if i.startswith("NOT"):  
                        output = "Response from Server 2 for operation \'" + iSplit[2] + " " + iSplit[3] + "\' : " + iSplit[0] + " " + iSplit[1]
                    else:
                        output = "Response from Server 2 for operation \'" + iSplit[1] + " " + iSplit[2] + "\' : " + iSplit[0]
                    print(f"{output}")


def client1_handler(client1_socket):
    global timestamp
    global tasks
    global requests
    global release_states
    global accessingResource
    global mutex
    while True:
        try:
            message = client1_socket.recv(1024).decode('utf-8')
        except ConnectionResetError:
            break
        time.sleep(3)
        split = message.split("\n")
        for i in split:
            if i:
                if i.startswith("Request"):
                    strip = i.lstrip("Request (")
                    strip = strip.rstrip(")")
                    tuple1 = tuple(map(int, strip.split(", ")))
                    release_states[tuple1] = 0
                    mutex.acquire()
                    heappush(tasks, tuple1)
                    mutex.release()
                    timestamp = max(tuple1[0]+1, timestamp+1)
                    message = "Received request " + str(tuple1)
                    print(f"{message}")
                    message = "reply " + str(tuple1) + "\n"
                    client1_socket.send(message.encode('utf-8'))
                elif i.startswith("reply"):
                    strip = i.lstrip("reply (")
                    strip = strip.rstrip(")")
                    tuple1 = tuple(map(int, strip.split(", ")))
                    if release_states[tuple1] != 2:
                        release_states[tuple1] += 1
                        message = "Received reply for " + str(tuple1) + ", incrementing reply count to " + str(release_states[tuple1])
                        print(f"{message}")
                elif i.startswith("release"):
                    strip = i.lstrip("release (")
                    strip = strip.rstrip(")")
                    tuple1 = tuple(map(int, strip.split(", ")))
                    mutex.acquire()
                    heappop(tasks)
                    mutex.release()
                    message = "Received release for request " + str(tuple1)
                    print(f"{message}")


def client2_handler(client2_socket):
    global timestamp
    global tasks
    global requests
    global release_states
    global accessingResource
    global mutex
    while True:
        try:
            message = client2_socket.recv(1024).decode('utf-8')
        except ConnectionResetError:
            break
        time.sleep(3)
        split = message.split("\n")
        for i in split:
            if i:
                if i.startswith("Request"):
                    strip = i.lstrip("Request (")
                    strip = strip.rstrip(")")
                    tuple1 = tuple(map(int, strip.split(", ")))
                    release_states[tuple1] = 0
                    mutex.acquire()
                    heappush(tasks, tuple1)
                    mutex.release()
                    timestamp = max(tuple1[0]+1, timestamp+1)
                    message = "Received request " + str(tuple1)
                    print(f"{message}")
                    message = "reply " + str(tuple1) + "\n"
                    client2_socket.send(message.encode('utf-8'))
                elif i.startswith("reply"):
                    strip = i.lstrip("reply (")
                    strip = strip.rstrip(")")
                    tuple1 = tuple(map(int, strip.split(", ")))
                    if release_states[tuple1] != 2:
                        release_states[tuple1] += 1
                        message = "Received reply for " + str(tuple1) + ", incrementing reply count to " + str(release_states[tuple1])
                        print(f"{message}\n", end="")
                elif i.startswith("release"):
                    strip = i.lstrip("release (")
                    strip = strip.rstrip(")")
                    tuple1 = tuple(map(int, strip.split(", ")))
                    mutex.acquire()
                    heappop(tasks)
                    mutex.release()
                    message = "Received release for request " + str(tuple1)
                    print(f"{message}")


def input_handler(server1_socket, server2_socket, client1_socket, client2_socket):
    global timestamp
    global tasks
    global requests
    global release_states
    global inputs
    global mutex
    while True:
        inp = inputs.get()
        if inp:
            splitInput = inp.split(" ")
            if splitInput[0] == "lookup":
                message = inp +"\n"
                server2_socket.send(message.encode('utf-8'))
            else:
                timestamp += 1
                message = "Request (" + str(timestamp) + ", 3)\n"
                output = "Sending request (" + str(timestamp) + ", 3) to"
                release_states[(timestamp, 3)] = 0
                requests[(timestamp, 3)] = inp
                mutex.acquire()
                heappush(tasks, (timestamp, 3))
                mutex.release()
                print(f"{output} Client 1")
                print(f"{output} Client 2")
                client1_socket.send(message.encode('utf-8'))
                client2_socket.send(message.encode('utf-8'))    


def heapHandler(client1_socket, client2_socket, server1_socket, server2_socket):
    global tasks
    global release_states
    global requests
    global accessingResource
    global mutex
    while True:
        if not accessingResource:
            mutex.acquire()
            if tasks:
                if tasks[0][1] == 1:
                    pass
                elif tasks[0][1] == 2:
                    pass
                elif release_states[tasks[0]] == 2:
                    command = requests[tasks[0]]
                    print(f"Sending operation \'{command}\' for request {str(tasks[0])} to primary server")
                    print(f"Sending operation \'{command}\' for request {str(tasks[0])} to secondary server")
                    command += " " + str(tasks[0])
                    server1_socket.send(command.encode('utf-8'))
                    server2_socket.send(command.encode('utf-8'))
                    accessingResource = True
            mutex.release()


def start_client():
    global portnum
    global inputs
    if len(sys.argv) == 2:
        portnum = int(sys.argv[1])
    server1_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server1_socket.connect(('127.0.0.1', portnum+2)) # Connect to the localhost server at port 9999
    server2_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server2_socket.connect(('127.0.0.1', portnum+5)) # Connect to the localhost server at port 9999

    client1_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client1_socket.connect(('127.0.0.1', portnum+7)) # Connect to the localhost server at port 9999
    client2_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client2_socket.connect(('127.0.0.1', portnum+8)) # Connect to the localhost server at port 9999

    inputHandler = threading.Thread(target = input_handler, args = (server1_socket, server2_socket, client1_socket, client2_socket))
    inputHandler.daemon = True
    inputHandler.start()

    client1 = threading.Thread(target = client1_handler, args = (client1_socket,))
    client1.daemon = True
    client1.start()
    
    client2 = threading.Thread(target = client2_handler, args = (client2_socket,))
    client2.daemon = True
    client2.start()

    heaper = threading.Thread(target = heapHandler, args = (client1_socket, client2_socket, server1_socket, server2_socket))
    heaper.daemon = True
    heaper.start()

    server1 = threading.Thread(target = server1_output_handler, args = (server1_socket, client1_socket, client2_socket))
    server1.daemon = True
    server1.start()

    server2 = threading.Thread(target = server2_output_handler, args = (server2_socket, client1_socket, client2_socket))
    server2.daemon = True
    server2.start()

    while True:
        inp = input("")
        if inp == "exit":
            break
        inputs.put(inp)
    sys.exit()

    
if __name__ == "__main__":
    start_client()