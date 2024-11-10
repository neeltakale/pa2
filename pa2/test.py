if __name__ == "__main__":
    from queue import Queue
    import time
    mes=Queue(maxsize=20)
    while True:
        inp = input("")
        if inp == "exit":
            break
        mes.put(inp)
    print(mes.get())
    print(mes.get())