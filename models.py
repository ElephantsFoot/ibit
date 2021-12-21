from sqlalchemy import Column, Integer, String, ForeignKey, Float, DateTime, func
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Asset(Base):
    __tablename__ = 'assets'
    id = Column(Integer, primary_key=True)
    name = Column(String)

    def __repr__(self):
        return "<Asset(name='%s')>" % self.name


class Point(Base):
    __tablename__ = 'points'
    id = Column(Integer, primary_key=True)
    created_on = Column(DateTime, server_default=func.now())
    value = Column(Float, nullable=False)
    asset_id = Column(Integer, ForeignKey('assets.id'))

    asset = relationship("Asset")

    def __repr__(self):
        return "<Point(value='%s')>" % self.value


# Asset.points = relationship("Point", order_by=Point.timestamp, back_populates="asset")
