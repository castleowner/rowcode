from sqlalchemy import create_engine,text
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert, JSONB
from sqlalchemy import create_engine, Column, MetaData, Table, Date, DateTime, Integer, String, Float, Boolean, DateTime

import pandas as pd

# Base 클래스 선언
Base = declarative_base()

from sqlalchemy import create_engine,text
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert, JSONB
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, Numeric, DateTime, UniqueConstraint

from telegrambot import TelegramBotSingleton
from sqlalchemy.exc import IntegrityError, NoResultFound

# KisKStockItem 클래스를 기반으로 테이블 매핑
class KorMaster(Base):
    __tablename__ = 'kor_master'
    
    symbol = Column(String, primary_key=True )
    data = Column(JSONB)

class Watcher(Base):
    __tablename__ = 'watcher'
    
    issue_date = Column(Date, primary_key=True )
    symbol = Column(String)
    memo = Column(String)

class StockDay(Base):
    __tablename__ = 'kor_day'

    stck_bsop_date = Column(DateTime, primary_key=True)
    symbol = Column(String, primary_key=True)
    stck_clpr = Column(Integer)
    stck_oprc = Column(Integer)
    stck_hgpr = Column(Integer)
    stck_lwpr = Column(Integer)
    acml_vol = Column(Integer)
    acml_tr_pbmn = Column(Integer)
    flng_cls_code = Column(String)
    prtt_rate = Column(Float)
    mod_yn = Column(Boolean)
    prdy_vrss_sign = Column(String)
    prdy_vrss = Column(Integer)
    revl_issu_reas = Column(String)
    data = Column(JSONB)
    
    # stck_bsop_date와 symbol을 유니크하게 묶음
    __table_args__ = (
        UniqueConstraint('stck_bsop_date', 'symbol', name='kor_day_pk'),
    )

class DBM:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DBM, cls).__new__(cls)
            cls._instance.setup_connection()
        return cls._instance

    def setup_connection(self):
        self.engine = create_engine("postgresql://juju:anstn1229!@localhost:5432/stock_timescale")
        self.Session = sessionmaker(bind=self.engine)

    def get_session(self):
        return self.Session()
    
    def get_engine(self):
        return self.engine
    
    def upsert_stockitem(self, json_data ):
        print( json_data )
     
    def data( self, sym, fromDT = '2010-01-01' ):
        engine = self.get_engine()
        
        existing_data = engine.execute("SELECT * FROM kor_day WHERE symbol = '%s' and stck_bsop_date > '%s' order by stck_bsop_date" % ( sym, fromDT ) )
        
        df = pd.DataFrame(existing_data, columns=existing_data.keys())
        
        df.rename(columns={
            'stck_bsop_date': 'Date',
            'stck_clpr': 'Close',
            'stck_oprc': 'Open',
            'stck_hgpr': 'High',
            'stck_lwpr': 'Low',
            'acml_vol': 'Volume'
        }, inplace=True)
        df.Date = pd.to_datetime(df.Date)
        df.set_index('Date', inplace=True)
        
        return df

    def get_symbols(self):
     
        engine = self.get_engine()
        existing_data = engine.execute("SELECT symbol FROM kor_master" )
        df = pd.DataFrame(existing_data, columns=existing_data.keys())
        return df;

    def get_symbols_by_top(self, count):
     
        engine = self.get_engine()
        existing_data = engine.execute( 'SELECT symbol FROM kor_day ORDER BY stck_bsop_date DESC, acml_tr_pbmn DESC LIMIT %d' % count )
        df = pd.DataFrame(existing_data, columns=existing_data.keys())
        return df;

    def get_watcher(self):
     
        engine = self.get_engine()
        existing_data = engine.execute("SELECT symbol FROM watcher" )
        df = pd.DataFrame(existing_data, columns=existing_data.keys())
        return df;

    # def test_connection(self):
    #     try:
    #         connection = self.engine.connect()
    #         connection.close()
    #         print("Database connection test successful.")
    #     except Exception as e:
    #         print(f"Database connection test failed: {e}")

# if __name__ == "__main__":
#     db_manager = DBM()
#     session1 = db_manager.get_session()
#     session2 = db_manager.get_session()

#     db_manager.test_connection()


# 모델 정의
# Base = declarative_base()

# class StockDaily(Base):
#     __tablename__ = 'kor_day'

#     id = Column(Integer, primary_key=True)
#     stck_bsop_date = Column(DateTime)
#     stck_clpr = Column(Integer)
#     stck_oprc = Column(Integer)
#     stck_hgpr = Column(Integer)
#     stck_lwpr = Column(Integer)
#     acml_vol = Column(Integer)
#     acml_tr_pbmn = Column(Integer)
#     flng_cls_code = Column(String)
#     prtt_rate = Column(Float)
#     mod_yn = Column(Boolean)
#     prdy_vrss_sign = Column(String)
#     prdy_vrss = Column(Integer)
#     revl_issu_reas = Column(String)
#     data = Column(JSONB)
# if __name__ == "__main__":

#     session = DBM().get_session()

#     # 테이블 생성
#     Base.metadata.create_all(DBM().get_engine())
