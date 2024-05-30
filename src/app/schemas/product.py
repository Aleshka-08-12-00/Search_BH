from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class ABlockElementSearch(Base):
    __tablename__ = 'a_block_element_search'

    code = Column(String(255))
    name = Column(String(255))
    barcode = Column(String(255))
    createdAt = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updatedAt = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    tsvector_name = Column(String)
    blockElementId = Column(Integer, primary_key=True)
    producerId = Column(Integer, default=0)
    rowHash = Column(String, nullable=False, default='')
