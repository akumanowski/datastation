#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : akumanowski
# Created Date: 25/05/2023
# version ='1.0'
# ---------------------------------------------------------------------------
"""
DataStation - консольное приложение, предназначенное для получения и хранения данных,
а также формирования отчетов. Данные импортируются из xlsx-файлов, хранятся в базе
SQLite. Отчеты генерируются в текстовом формате.
"""
# ---------------------------------------------------------------------------
import datetime

import click

from db_store import DataStorage, ReportByDates
from xlsx_import import DataReader


# ---------------------------------------------------------------------------
@click.command()
@click.help_option('--help', '-h', help='Показать это сообщение и выйти')
@click.option('--load', '-l', is_flag=True, help='Загрузить данные из файла')
@click.option('--filename', '-f', default='data_packet.xlsx', show_default=True, help='Файл для загрузки')
@click.option('--date', '-d', default=datetime.date.today(), show_default=True, help='Дата загружаемых данных')
@click.option('--report', '-r', is_flag=True, help='Вывести отчет')
def datastation(load, filename, report, date):
    """
        DataStation — Программа для получения и хранения данных, формирования отчетов на основе этих данных.
    """

    if load:
        try:
            reader = DataReader(filename=filename, date=datetime.date.fromisoformat(date))
            click.echo(f'Чтение данных из файла {filename} завершено.')
            data_storage = DataStorage()
            click.echo(f'База данных подготовлена для загрузки.')
            data_storage.save(data_pack=reader.read_xlsx())
            click.echo(f'Загрузка данных, актуальных на дату {date} выполнена.')
        except Exception as err:
            click.echo(
                f'Произошла ошибка "{err}" при загрузке из файла {filename} данных, актуальных на {date}.',
                err=True,
                color=True
            )
    if report:
        try:
            data_storage = DataStorage()
            click.echo(f'База данных подготовлена для формирования отчета.')
            report = ReportByDates(engine=data_storage.engine)
            click.echo(f'Отчет сформирован:')
            click.echo(report)
        except Exception as err:
            click.echo(
                f'Произошла ошибка "{err}" при подготовке отчета.',
                err=True,
                color=True
            )


if __name__ == '__main__':
    datastation()
