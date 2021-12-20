import json
import multiprocessing
import socket
import traceback
import urllib.request
from time import sleep, time

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


def get_asset_name_by_id(asset_id):
    return "EURUSD"


def get_asset_history(asset_name):
    return []


def get_assets(message, addr, subscribers_to_assets):
    return ASSETS


def subscribe(message, addr, subscribers_to_assets):
    for asset, subscribers in subscribers_to_assets.items():
        if addr in subscribers:
            subscribers.remove(addr)
    asset_name = get_asset_name_by_id(message.get("assetId"))
    subscribers_to_asset = subscribers_to_assets[asset_name]
    subscribers_to_asset.add(addr)
    subscribers_to_assets[asset_name] = subscribers_to_asset
    return get_asset_history(asset_name)


HANDLERS = {
    "assets": get_assets,
    "subscribe": subscribe,
}


def get_new_ratios(queue_out, assets):
    while True:
        with urllib.request.urlopen('https://ratesjson.fxcm.com/DataDisplayer') as f:
            result = json.loads(f.read()[5:].strip()[:-2].replace(b',}', b'}'))
        new_ratios = {}
        for rate in result.get("Rates", []):
            if rate["Symbol"] in assets:
                new_ratios[rate["Symbol"]] = (float(rate["Bid"]) + float(rate["Ask"])) / 2
        print(new_ratios)
        queue_out.put(new_ratios)
        sleep(1)


def notify_subscribers(in_queue, subscribers_to_assets, s):
    while True:
        new_ratios = in_queue.get()
        print("Got new message to notify")
        for asset in subscribers_to_assets.keys():
            for subscriber in subscribers_to_assets[asset]:
                result = {
                    "action": "point",
                    "message": {
                        "assetName": asset,
                        "time": int(time()),
                        "assetId": 1,
                        "value": new_ratios[asset],
                    },
                }
                s.sendto(json.dumps(result).encode('ascii'), subscriber)


def error_callback(e):
    traceback.print_exception(type(e), e, e.__traceback__)


def listen(host: str = '127.0.0.1', port: int = 8080):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    s.bind((host, port))

    manager = multiprocessing.Manager()
    new_ratios_queue = manager.Queue()
    subscribers_to_assets = manager.dict(
        {
            "EURUSD": set(),
            "USDJPY": set(),
            "GBPUSD": set(),
            "AUDUSD": set(),
            "USDCAD": set(),
        }
    )
    pool = multiprocessing.Pool()
    pool.apply_async(get_new_ratios, (new_ratios_queue, subscribers_to_assets.keys()), error_callback=error_callback)
    pool.apply_async(notify_subscribers, (new_ratios_queue, subscribers_to_assets, s), error_callback=error_callback)

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
            result = handler(payload.get("message", {}), addr, subscribers_to_assets)
            s.sendto(json.dumps(result).encode('ascii'), addr)


if __name__ == '__main__':
    listen()
