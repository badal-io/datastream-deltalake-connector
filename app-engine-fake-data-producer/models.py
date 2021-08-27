from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, \
    ForeignKey, event
from sqlalchemy.sql import func
from sqlalchemy.orm import scoped_session, sessionmaker, backref, relation
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy
import os
import logging


# from werkzeug import cached_property, http_date
#
# from flask import url_for, Markup

db_name = os.getenv("MYSQL_DB","demo")
db_user = os.getenv("MYSQL_USER", "root")
db_pass = os.getenv("MYSQL_PASS")
cloud_sql_connection_name = os.environ["CLOUD_SQL_CONNECTION_NAME"]
mysql_charset = "utf8mb4"

engine = create_engine(
    sqlalchemy.engine.url.URL(
        drivername="mysql+pymysql",
        username=db_user,  # e.g. "my-database-user"
        password=db_pass,  # e.g. "my-database-password"
        database=db_name,  # e.g. "my-database-name"
        query={
            "unix_socket": "{}/{}".format(
                "/cloudsql",
                cloud_sql_connection_name)  # i.e "<PROJECT-NAME>:<INSTANCE-REGION>:<INSTANCE-NAME>"
        }
    )
)

db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))


Base = declarative_base()
Base.query = db_session.query_property()

def init_db():
    Base.metadata.create_all(bind=engine)


class Basic():
    @classmethod
    def random(cls):
        return cls.query.order_by(func.random()).first()

    def create(self):
        db_session.add(self)
        db_session.commit()

    def save(self):
        db_session.add(self)
        db_session.commit()

    def delete(self):
        db_session.delete(self)
        db_session.commit()


class Voter(Base, Basic):
    __tablename__ = 'inventory.voters'
    id = Column(String(9), primary_key=True)
    name = Column(String(50))
    address = Column(String(100))
    gender = Column(String(50))


class Poll(Base, Basic):
    __tablename__ = 'inventory.polls'
    id = Column('id', Integer, primary_key=True, autoincrement=True)
    voter_id = Column(String(9))
    answer = Column(String(50))
    created_at = Column(DateTime(), default=datetime.now)

