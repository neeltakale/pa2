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
printmutex = threading.Lock()

def server1_output_handler(server1_socket, client2_socket, client3_socket):
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
                if i.startswith("Success"):
                    stripped = i.lstrip("Success ")
                    output = "Response from Server 1 for request " + stripped + " : " + iSplit[0]
                    print(f"{output}")
                    if stripped in server_response:
                        accessingResource = False
                        mutex.acquire()
                        heappop(tasks)
                        mutex.release()
                        message = "Sending release for request " + stripped + " to Client 2"
                        print(f"{message}")
                        message = "Sending release for request " + stripped + " to Client 3"
                        print(f"{message}")
                        message = "release (" + stripped + "\n"
                        client2_socket.send(message.encode('utf-8'))
                        client3_socket.send(message.encode('utf-8'))
                    else:
                        server_response[stripped] = 1
                else:    
                    if i.startswith("NOT"):  
                        output = "Response from Server 1 for operation \'" + iSplit[2] + " " + iSplit[3] + "\' : " + iSplit[0] + " " + iSplit[1]
                    else:
                        output = "Response from Server 1 for operation \'" + iSplit[1] + " " + iSplit[2] + "\' : " + iSplit[0]
                    print(f"{output}")


def server2_output_handler(server2_socket, client2_socket, client3_socket):
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
                stripped = i.lstrip("Success ")
                output = "Response from Server 2 for request " + stripped + " : " + iSplit[0]
                print(f"{output}")
                if stripped in server_response:
                    accessingResource = False
                    mutex.acquire()
                    heappop(tasks)
                    mutex.release()
                    message = "Sending release for request " + stripped + " to Client 2"
                    print(f"{message}")
                    message = "Sending release for request " + stripped + " to Client 3"
                    print(f"{message}")
                    message = "release (" + stripped + "\n"
                    client2_socket.send(message.encode('utf-8'))
                    client3_socket.send(message.encode('utf-8'))
                else:
                    server_response[stripped] = 1


def client2_handler(client2_socket):
    global timestamp
    global tasks
    global requests
    global release_states
    global accessingResource
    global mutex
    while True:
        message = client2_socket.recv(1024).decode('utf-8')
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


def client3_handler(client3_socket):
    global timestamp
    global tasks
    global requests
    global release_states
    global accessingResource
    global mutex
    while True:
        message = client3_socket.recv(1024).decode('utf-8')
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
                    client3_socket.send(message.encode('utf-8'))

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


def input_handler(server1_socket, server2_socket, client2_socket, client3_socket):
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
                server1_socket.send(message.encode('utf-8'))
            else:
                timestamp += 1
                message = "Request (" + str(timestamp) + ", 1)\n"
                output = "Sending request (" + str(timestamp) + ", 1) to"
                release_states[(timestamp, 1)] = 0
                requests[(timestamp, 1)] = inp
                mutex.acquire()
                heappush(tasks, (timestamp, 1))
                mutex.release()
                print(f"{output} Client 2")
                print(f"{output} Client 3")
                client2_socket.send(message.encode('utf-8'))
                client3_socket.send(message.encode('utf-8'))


def heapHandler(client2_socket, client3_socket, server1_socket, server2_socket):
    global tasks
    global release_states
    global requests
    global accessingResource
    global mutex
    while True:
        if not accessingResource:
            mutex.acquire()
            if tasks:
                if tasks[0][1] == 2:
                    pass
                    #if release_states[tasks[0]] == 0:
                    #    release_states[tasks[0]] += 1
                    #    message = "reply " + str(tasks[0]) + "\n"
                    #    client2_socket.send(message.encode('utf-8'))
                    #    print(f"Received request {str(tasks[0])}")
                elif tasks[0][1] == 3:
                    pass
                    #if release_states[tasks[0]] == 0:
                    #    release_states[tasks[0]] += 1
                    #    message = "reply " + str(tasks[0]) + "\n"
                    #    client3_socket.send(message.encode('utf-8'))
                    #    print(f"Received request {str(tasks[0])}")
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
    server1_socket.connect(('127.0.0.1', portnum)) # Connect to the localhost server at port 9999
    server2_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server2_socket.connect(('127.0.0.1', portnum+3)) # Connect to the localhost server at port 9999

    client2socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client2socket.bind(('127.0.0.1', portnum+6)) # specify host (itself) and port number 9999
    client2socket.listen(5)
    client3socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client3socket.bind(('127.0.0.1', portnum+7)) # specify host (itself) and port number 9999
    client3socket.listen(5)

    client2_socket, t = client2socket.accept()
    client3_socket, t = client3socket.accept()

    inputHandler = threading.Thread(target = input_handler, args = (server1_socket, server2_socket, client2_socket, client3_socket))
    inputHandler.daemon = True
    inputHandler.start()

    client2 = threading.Thread(target = client2_handler, args = (client2_socket,))
    client2.daemon = True
    client2.start()

    client3 = threading.Thread(target = client3_handler, args = (client3_socket,))
    client3.daemon = True
    client3.start()

    heaper = threading.Thread(target = heapHandler, args = (client2_socket, client3_socket, server1_socket, server2_socket))
    heaper.daemon = True
    heaper.start()

    server1 = threading.Thread(target = server1_output_handler, args = (server1_socket, client2_socket, client3_socket))
    server1.daemon = True
    server1.start()

    server2 = threading.Thread(target = server2_output_handler, args = (server2_socket, client2_socket, client3_socket))
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