import json
import os
import socket
import threading

UDP_MAX_SIZE = 65535


def listen(s: socket.socket):
    while True:
        msg = s.recv(UDP_MAX_SIZE)
        print(msg.decode('ascii'))


def connect(host: str = '127.0.0.1', port: int = 8080):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    s.connect((host, port))

    threading.Thread(target=listen, args=(s,), daemon=True).start()

    while True:
        print("Enter 'assets' to get assets names")
        print("Enter 'subscribe <id>' to subscribe to certain asset")
        commands = input().split()
        msg = None
        if commands[0].lower() == "assets":
            msg = {
                "action": "assets",
                "message": {},
            }
        elif commands[0].lower() == "subscribe":
            msg = {
                "action": "subscribe",
                "message": {
                    "assetId": commands[1],
                },
            }

        if msg:
            s.send(json.dumps(msg).encode('ascii'))


if __name__ == '__main__':
    os.system('clear')
    print('Welcome to chat!')
    connect()
