import socket
import sys
import time
import threading
portnum = 9000
storage = {}

def response_handler(data, client_socket):
    time.sleep(3)
    splitData = data.split(" ")
    if splitData[0] == "insert":
        storage[int(splitData[1])] = splitData[2]
        print(f"Successfully inserted key {int(splitData[1])}")
        test = splitData[3] + " " + splitData[4]
        message = "Success " + test + "\n"
        client_socket.send(message.encode('utf-8'))
    else:
        if int(splitData[1]) in storage:
            print(f"{storage[int(splitData[1])]}")
            message = storage[int(splitData[1])] + " "+ data + "\n"
            client_socket.send(message.encode('utf-8'))
            #add response behavior
        else:
            print("NOT FOUND")
            message = "NOT FOUND" + " "+ data + "\n"
            client_socket.send(message.encode('utf-8'))
            #add response behavior

def input_handler(client_socket):
    while True:
        data = client_socket.recv(1024).decode('utf-8') # Receive one message from the client
        if data:
            splitData = data.split("\n")
            for i in splitData:
                if i:
                    thread1 = threading.Thread(target = response_handler, args = (i, client_socket))
                    thread1.daemon = True
                    thread1.start()

def start_server():
    global portnum
    if len(sys.argv) == 2:
        portnum = int(sys.argv[1])
    server_socket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket1.bind(('127.0.0.1', portnum)) # specify host (itself) and port number 9999
    server_socket1.listen(5)
    server_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket2.bind(('127.0.0.1', portnum+1)) # specify host (itself) and port number 9999
    server_socket2.listen(5)
    server_socket3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket3.bind(('127.0.0.1', portnum+2)) # specify host (itself) and port number 9999
    server_socket3.listen(5)

    #0-1,1-2,2-3
    client_socket1, t = server_socket1.accept()
    client_socket2, t = server_socket2.accept()
    client_socket3, t = server_socket3.accept()

    thread1 = threading.Thread(target = input_handler, args = (client_socket1,))
    thread1.daemon = True
    thread1.start()
    thread2 = threading.Thread(target = input_handler, args = (client_socket2,))
    thread2.daemon = True
    thread2.start()
    thread3 = threading.Thread(target = input_handler, args = (client_socket3,))
    thread3.daemon = True
    thread3.start()

    while True:
        inp = input("")
        if inp == "exit":
            break
        if inp == "dictionary":
            message = "{"
            for i in sorted(list(storage.keys())):
                if i != max(sorted(list(storage.keys()))):
                    message  += str((i, int(storage[i]))) + ", "
                else:
                    message  += str((i, int(storage[i])))

            message += "}"
            print(message)
    sys.exit()


if __name__ == "__main__":
    start_server()