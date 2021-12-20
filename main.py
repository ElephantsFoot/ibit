import json
import socket

UDP_MAX_SIZE = 65535

ASSETS = {
    "action": "assets",
    "message": {
        "assets": [
            {"id": 1, "name": "EURUSD"},
            {"id": 2, "name": "USDJPY"},
            {"id": 3, "name": "GBPUSD"},
            {"id": 4, "name": "AUDUSD"},
            {"id": 5, "name": "USDCAD"},
        ],
    },
}

SUBSCRIBERS_TO_ASSETS = {
    "EURUSD": set(),
    "USDJPY": set(),
    "GBPUSD": set(),
    "AUDUSD": set(),
    "USDCAD": set(),
}


def get_asset_name_by_id(asset_id):
    return "EURUSD"


def get_asset_history(asset_name):
    return []


def get_assets(message, addr):
    return ASSETS


def subscribe(message, addr):
    for asset, subscribers in SUBSCRIBERS_TO_ASSETS.items():
        if addr in subscribers:
            subscribers.remove(addr)
    asset_name = get_asset_name_by_id(message.get("assetId"))
    SUBSCRIBERS_TO_ASSETS.get(asset_name).add(addr)
    return get_asset_history(asset_name)


HANDLERS = {
    "assets": get_assets,
    "subscribe": subscribe,
}


def listen(host: str = '127.0.0.1', port: int = 8080):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    s.bind((host, port))
    print(f'Listening at {host}:{port}')

    members = []
    while True:
        msg, addr = s.recvfrom(UDP_MAX_SIZE)

        if addr not in members:
            members.append(addr)

        if not msg:
            continue

        payload = json.loads(msg.decode('ascii'))
        handler = HANDLERS.get(payload.get("action"))
        if handler is not None:
            result = handler(payload.get("message", {}), addr)
            s.sendto(json.dumps(result).encode('ascii'), addr)


if __name__ == '__main__':
    listen()
