import json
import multiprocessing
import socket
import traceback
import urllib.request
from time import sleep, time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Base, Asset

UDP_MAX_SIZE = 65535


def get_asset_history(asset_name):
    return []


def get_assets(message, addr, subscribers_to_assets):
    with Session() as session:
        assets = session.query(Asset).all()
    result = {
        "action": "assets",
        "message": {
            "assets": [
                {
                    "id": asset.id,
                    "name": asset.name,
                } for asset in assets
            ],
        },
    }
    return result


def subscribe(message, addr, subscribers_to_assets):
    for asset, subscribers in subscribers_to_assets.items():
        if addr in subscribers:
            subscribers.remove(addr)
    with Session() as session:
        chosen_asset = session.query(Asset).filter(Asset.id == message.get("assetId")).first()
    if chosen_asset:
        subscribers_to_asset = subscribers_to_assets[chosen_asset.name]
        subscribers_to_asset.add(addr)
        subscribers_to_assets[chosen_asset.name] = subscribers_to_asset
        return get_asset_history(chosen_asset.name)


HANDLERS = {
    "assets": get_assets,
    "subscribe": subscribe,
}


def get_new_ratios(queue_out, assets):
    engine = create_engine("sqlite:////tmp/ibit.db", echo=False, future=True)
    Session = sessionmaker(bind=engine)
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
    engine = create_engine("sqlite:////tmp/ibit.db", echo=False, future=True)
    Session = sessionmaker(bind=engine)
    while True:
        new_ratios = in_queue.get()
        print("Got new message to notify")
        for asset_name in subscribers_to_assets.keys():
            for subscriber in subscribers_to_assets[asset_name]:
                with Session() as session:
                    chosen_asset = session.query(Asset).filter(Asset.name == asset_name).first()
                result = {
                    "action": "point",
                    "message": {
                        "assetName": asset_name,
                        "time": int(time()),
                        "assetId": chosen_asset.id,
                        "value": new_ratios[asset_name],
                    },
                }
                s.sendto(json.dumps(result).encode('ascii'), subscriber)


def error_callback(e):
    traceback.print_exception(type(e), e, e.__traceback__)


def listen(host: str = '127.0.0.1', port: int = 8080):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((host, port))
    print(f'Listening at {host}:{port}')

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

    while True:
        msg, addr = s.recvfrom(UDP_MAX_SIZE)

        if not msg:
            continue

        payload = json.loads(msg.decode('ascii'))
        handler = HANDLERS.get(payload.get("action"))
        if handler is not None:
            result = handler(payload.get("message", {}), addr, subscribers_to_assets)
            s.sendto(json.dumps(result).encode('ascii'), addr)


if __name__ == '__main__':
    engine = create_engine("sqlite:////tmp/ibit.db", echo=True, future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        assets = session.query(Asset).all()
        if not assets:
            session.add_all(
                [
                    Asset(name="EURUSD"),
                    Asset(name="USDJPY"),
                    Asset(name="GBPUSD"),
                    Asset(name="AUDUSD"),
                    Asset(name="USDCAD"),
                ]
            )
            session.commit()
    listen()
