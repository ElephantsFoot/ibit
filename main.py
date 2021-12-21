import json
import multiprocessing
import socket
import traceback
import urllib.request
from datetime import datetime, timedelta
from time import sleep, time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Base, Asset, Point

UDP_MAX_SIZE = 65535


def get_asset_history(chosen_asset):
    with Session() as session:
        points = session.query(Point).filter(Point.asset == chosen_asset).filter(
            Point.created_on > datetime.utcnow() - timedelta(minutes=2)).all()
    return [
        {
            "assetName": chosen_asset.name,
            "time": int(point.created_on.timestamp()),
            "assetId": chosen_asset.id,
            "value": point.value,
        }
        for point in points
    ]


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
    for asset in subscribers_to_assets.keys():
        subscribers_to_asset = subscribers_to_assets[asset]
        if addr in subscribers_to_asset:
            subscribers_to_asset.remove(addr)
            subscribers_to_assets[asset] = subscribers_to_asset
    with Session() as session:
        chosen_asset = session.query(Asset).filter(Asset.id == message.get("assetId")).first()
    if chosen_asset:
        subscribers_to_asset = subscribers_to_assets[chosen_asset.name]
        subscribers_to_asset.add(addr)
        subscribers_to_assets[chosen_asset.name] = subscribers_to_asset
        return {
            "action": "asset_history",
            "message": {
                "points": get_asset_history(chosen_asset),
            },
        }


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
        with Session() as session:
            session.add_all(
                [
                    Point(value=new_ratios["EURUSD"],
                          asset=session.query(Asset).filter(Asset.name == "EURUSD").first()),
                    Point(value=new_ratios["USDJPY"],
                          asset=session.query(Asset).filter(Asset.name == "USDJPY").first()),
                    Point(value=new_ratios["GBPUSD"],
                          asset=session.query(Asset).filter(Asset.name == "GBPUSD").first()),
                    Point(value=new_ratios["AUDUSD"],
                          asset=session.query(Asset).filter(Asset.name == "AUDUSD").first()),
                    Point(value=new_ratios["USDCAD"],
                          asset=session.query(Asset).filter(Asset.name == "USDCAD").first()),
                ]
            )
            session.commit()
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
