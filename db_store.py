#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : akumanowski
# Created Date: 25/05/2023
# version ='1.0'
# ---------------------------------------------------------------------------
"""
Модуль консольного приложения DataStation, реализующий чтение работу с базой данных
"""
# ---------------------------------------------------------------------------
import datetime
import os
from typing import List

from dotenv import load_dotenv
from sqlalchemy import Column, Integer, String, Date
from sqlalchemy import create_engine, func, Engine
from sqlalchemy.orm import DeclarativeBase, Session

# ---------------------------------------------------------------------------
load_dotenv()

DATABASE_NAME = os.getenv('DATABASE_NAME')


# создаем базовый класс для моделей
class Base(DeclarativeBase):
    pass


# создаем модель, объекты которой будут храниться в бд
class DataStream(Base):
    __tablename__ = "datastream"
    id = Column(Integer, autoincrement=True, unique=True, primary_key=True, index=True)
    sid = Column(String, nullable=False, default=0)
    company = Column(String(10), nullable=False, default='company1')
    date = Column(Date, default=datetime.datetime.now())
    fact_qliq_1 = Column(Integer, nullable=False, default=0)
    fact_qliq_2 = Column(Integer, nullable=False, default=0)
    fact_qoil_1 = Column(Integer, nullable=False, default=0)
    fact_qoil_2 = Column(Integer, nullable=False, default=0)
    forecast_qliq_1 = Column(Integer, nullable=False, default=0)
    forecast_qliq_2 = Column(Integer, nullable=False, default=0)
    forecast_qoil_1 = Column(Integer, nullable=False, default=0)
    forecast_qoil_2 = Column(Integer, nullable=False, default=0)


# создаем класс хранилище данных
class DataStorage:
    def __init__(self):
        # строка подключения
        self.sqlite_database = f'sqlite:///{DATABASE_NAME}'
        # создаем движок SqlAlchemy
        self.engine = create_engine(self.sqlite_database)

    def save(self, data_pack: List):
        # создаем таблицы
        Base.metadata.create_all(bind=self.engine)

        with Session(autoflush=False, bind=self.engine) as db:
            # добавляем объекты, заполненные данными из xlsx таблицы
            for row in data_pack:
                record = DataStream(
                    sid=row['sid'], company=row['company'],
                    fact_qliq_1=row['fact_qliq_1'], fact_qliq_2=row['fact_qliq_2'],
                    fact_qoil_1=row['fact_qoil_1'], fact_qoil_2=row['fact_qoil_2'],
                    forecast_qliq_1=row['forecast_qliq_1'], forecast_qliq_2=row['forecast_qliq_2'],
                    forecast_qoil_1=row['forecast_qoil_1'], forecast_qoil_2=row['forecast_qoil_2'],
                    date=row['date']
                )
                db.add(record)
            db.commit()


# создаем класс, выполняющий запрос к бд
class ReportByDates:
    def __init__(self, engine: Engine):
        self.engine = engine

    def __str__(self) -> str:
        with Session(bind=self.engine) as db:
            query_result = db.query(
                DataStream.date,
                func.sum(DataStream.fact_qliq_1).label('fact_qliq_1'),
                func.sum(DataStream.fact_qliq_2).label('fact_qliq_2'),
                func.sum(DataStream.fact_qoil_1).label('fact_qoil_1'),
                func.sum(DataStream.fact_qoil_2).label('fact_qoil_2'),
                func.sum(DataStream.forecast_qliq_1).label('forecast_qliq_1'),
                func.sum(DataStream.forecast_qliq_2).label('forecast_qliq_2'),
                func.sum(DataStream.forecast_qoil_1).label('forecast_qoil_1'),
                func.sum(DataStream.forecast_qoil_2).label('forecast_qoil_2')
            ).group_by(DataStream.date).order_by(DataStream.date).all()

        str_to_print = [
            '+------------+-------------------------------+-------------------------------+',
            '|            |              Fact             |           Forecast            |',
            '|            |---------------+---------------+---------------+---------------+',
            '|    Date    |      Qliq     |      Qoil     |      Qliq     |      Qoil     |',
            '|            |-------+-------+-------+-------+-------+-------+-------+-------+',
            '|            | Data1 | Data2 | Data1 | Data2 | Data1 | Data2 | Data1 | Data2 |',
            '+------------+-------+-------+-------+-------+-------+-------+-------+-------+'
        ]
        for i in range(len(query_result)):
            record = query_result[i]
            str_to_print.append(
                f'| {record.date:%d.%m.%Y} | {record.fact_qliq_1:^5} '
                f'| {record.fact_qliq_2:^5} | {record.fact_qoil_1:^5} | {record.fact_qoil_2:^5} '
                f'| {record.forecast_qliq_1:^5} | {record.forecast_qliq_2:^5} '
                f'| {record.forecast_qoil_1:^5} | {record.forecast_qoil_2:^5} |'
            )
            str_to_print.append(
                '+------------+-------+-------+-------+-------+-------+-------+-------+-------+'
            )
        return '\n'.join(str_to_print)
