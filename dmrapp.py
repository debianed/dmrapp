#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import base64
import datetime
import hashlib
import io
import json
import os
import pandas
import pathlib
import pytz
import soundfile

from calendar import monthrange
from dateutil import parser, tz
from fastapi import Request
from fastapi.responses import RedirectResponse
from nicegui import app, ui, Client
from sqlalchemy import create_engine, exc, text
from starlette.middleware.base import BaseHTTPMiddleware
from threading import Timer
from typing import Optional, Union

# Прероутер для авторизации
class AuthMiddleware(BaseHTTPMiddleware):
    unrestricted_page_routes = {'/login'}
    async def dispatch(self, request: Request, call_next):
        if not app.storage.user.get('authenticated', False):
            if request.url.path in Client.page_routes.values() and request.url.path not in self.unrestricted_page_routes:
                app.storage.user['referrer_path'] = request.url.path
                return RedirectResponse('/login')
        return await call_next(request)

# Роутер страниц приложения
class UIPage(ui.page):
    def __init__(self) -> None:
        super().__init__('')

    def add(self, path: str, func):
        self._path = path
        self.__call__(func)

# Источник звука для плеера
class SndSource():
    def __init__(self, fpath: str) -> None:
        self._basedir = os.path.dirname(os.path.abspath(__file__))
        self.set_source(fpath)

    @property
    def source(self):
        return self._fpath

    @property
    def uri(self):
        return self._uri

    def set_source(self, fpath: str) -> str:
        def remove_old_media_routes(path: str) -> None:
            app.routes[:] = [r for r in app.routes if not getattr(r, 'path', None).startswith(path)]

        if fpath == '':
            self._fpath = ''
            self._uri = ''
        else:
            self._fpath = fpath.replace('/','\\')

            user = hashlib.sha256(app.storage.browser['id'].encode()).hexdigest()[:32]
            fl = str(pathlib.Path(self._fpath).stem)

            remove_old_media_routes(f'/media/{user}/')
            self._uri = app.add_media_file(local_file=self._fpath, url_path=f'/media/{user}/{fl}')

# Основное приложение
class DMRApp():
    #region Поля
    _sdata = pandas.DataFrame([])
    _zdata = pandas.DataFrame([])
    _sndfile = SndSource('')
    _ui = ui
    #region

    #region Конструктор
    def __init__(self, pgconnstr: str, myconnstr: str, users: dict, recdir: str) -> None:
        # Инициализация
        app.add_middleware(AuthMiddleware)

        # Настраиваем маршруты
        self._router = UIPage()
        self._router.add('/', self._uipg_main)
        self._router.add('/login', self._uipg_login)

        # Заполняем поля
        self._users = users
        self._recdir = recdir
        self._role = "user"

        # Подключаемся к БД
        self._pgsql = create_engine(pgconnstr, pool_pre_ping=True).connect()
        self._mysql = create_engine(myconnstr, pool_pre_ping=True).connect()

        self._repeater(300, self._db_mysql_keepalive)
    #endregion

    #region Основные методы
    # Запуск приложения
    def start(self) -> None:
        radio = '''
            <svg height="800px" width="800px" version="1.1" id="Layer_1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" viewBox="0 0 512.001 512.001" xml:space="preserve">
                <circle style="fill:#FFD2BE;" cx="255.996" cy="146.87" r="146.87"/>
                <circle style="fill:#FFAF91;" cx="255.996" cy="146.87" r="95.61"/>
                <circle style="fill:#FF8269;" cx="255.996" cy="146.87" r="47.805"/>
                <circle style="fill:#FF5050;" cx="255.996" cy="146.87" r="26.075"/>
                <path d="M206.833,204.523c2.085,0,4.171-0.795,5.762-2.387c3.182-3.182,3.182-8.341,0-11.523c-23.934-23.935-23.934-62.879,0-86.813
                    c3.182-3.182,3.182-8.341,0-11.523c-3.182-3.182-8.342-3.182-11.523,0c-30.289,30.289-30.289,79.571,0,109.86
                    C202.662,203.728,204.747,204.523,206.833,204.523z"/>
                <path d="M299.407,202.136c1.591,1.591,3.677,2.387,5.762,2.387c2.085,0,4.171-0.795,5.762-2.387
                    c30.289-30.289,30.289-79.571,0-109.86c-3.182-3.182-8.342-3.182-11.523,0c-3.182,3.182-3.182,8.341,0,11.523
                    c23.934,23.935,23.934,62.879,0,86.813C296.224,193.796,296.224,198.954,299.407,202.136z"/>
                <path d="M342.044,241.4c2.085,0,4.171-0.795,5.762-2.387c36.782-36.782,48.07-91.76,28.758-140.067
                    c-1.67-4.179-6.41-6.212-10.591-4.541c-4.179,1.671-6.212,6.412-4.541,10.591c16.888,42.245,7.016,90.327-25.15,122.493
                    c-3.182,3.182-3.182,8.342,0,11.523C337.873,240.604,339.959,241.4,342.044,241.4z"/>
                <path d="M345.208,76.955c1.608,2.037,3.992,3.099,6.4,3.099c1.768,0,3.549-0.573,5.046-1.754c3.531-2.789,4.134-7.914,1.344-11.445
                    c-3.15-3.988-6.579-7.842-10.191-11.455c-3.182-3.182-8.342-3.181-11.523,0c-3.182,3.183-3.181,8.342,0.001,11.524
                    C339.448,70.09,342.451,73.464,345.208,76.955z"/>
                <path d="M132.696,23.902c3.182-3.182,3.182-8.342,0-11.523c-3.182-3.182-8.342-3.182-11.523,0
                    c-74.345,74.343-74.345,195.311-0.001,269.655c1.591,1.591,3.677,2.387,5.762,2.387s4.171-0.795,5.762-2.387
                    c3.182-3.182,3.182-8.341,0-11.523C64.705,202.521,64.705,91.893,132.696,23.902z"/>
                <path d="M390.829,12.378c-3.182-3.182-8.342-3.182-11.523,0c-3.182,3.182-3.182,8.341,0,11.523c67.99,67.99,67.99,178.619,0,246.609
                    c-3.182,3.182-3.182,8.342,0,11.523c1.591,1.591,3.677,2.387,5.762,2.387c2.085,0,4.171-0.795,5.762-2.387
                    C465.173,207.69,465.173,86.722,390.829,12.378z"/>
                <path d="M150.568,189.418c-16.888-42.245-7.016-90.327,25.15-122.493c3.182-3.182,3.182-8.342,0-11.523
                    c-3.181-3.182-8.341-3.182-11.523,0c-36.782,36.782-48.07,91.76-28.758,140.067c1.273,3.187,4.334,5.126,7.568,5.126
                    c1.006,0,2.031-0.188,3.023-0.585C150.206,198.339,152.239,193.597,150.568,189.418z"/>
                <path d="M166.793,217.458c-2.789-3.532-7.913-4.134-11.445-1.345c-3.531,2.789-4.134,7.914-1.344,11.445
                    c3.15,3.987,6.579,7.842,10.191,11.455c1.592,1.591,3.677,2.387,5.762,2.387s4.171-0.795,5.763-2.387
                    c3.182-3.182,3.181-8.342,0-11.523C172.553,224.324,169.55,220.949,166.793,217.458z"/>
                <path d="M273.083,176.843c10.236-5.922,17.141-16.985,17.141-29.637c0-1.96-0.167-3.925-0.498-5.844
                    c-0.765-4.434-4.977-7.411-9.415-6.645c-4.435,0.765-7.41,4.979-6.646,9.414c0.174,1.006,0.262,2.04,0.262,3.075
                    c0,9.885-8.042,17.927-17.927,17.927s-17.927-8.042-17.927-17.927S246.115,129.28,256,129.28c0.889,0,1.781,0.065,2.651,0.193
                    c4.452,0.658,8.594-2.418,9.251-6.871c0.657-4.452-2.418-8.594-6.871-9.251c-1.656-0.244-3.35-0.368-5.033-0.368
                    c-18.871,0-34.224,15.353-34.224,34.224c0,12.652,6.906,23.714,17.141,29.637l-95.039,324.72c-0.982,3.356,0.287,6.964,3.153,8.968
                    c1.404,0.98,3.036,1.47,4.667,1.47c1.7,0,3.401-0.531,4.837-1.591l99.465-73.366l99.465,73.366c1.436,1.059,3.137,1.591,4.837,1.591
                    c1.632,0,3.265-0.489,4.667-1.47c2.866-2.003,4.135-5.612,3.153-8.968L273.083,176.843z M222.55,290.786l19.714,14.526
                    l-30.555,22.514L222.55,290.786z M204.154,353.637l56.295-41.482c0.138-0.09,0.267-0.197,0.401-0.296l28.6-21.073l10.841,37.04
                    l-13.408-9.88c-3.623-2.669-8.724-1.897-11.394,1.726c-2.669,3.623-1.897,8.724,1.726,11.394l30.63,22.569l5.997,20.49L256,416.791
                    l-57.843-42.666L204.154,353.637z M256.001,181.43c0.48,0,0.957-0.017,1.433-0.037l27.143,92.74l-28.576,21.056l-28.576-21.056
                    l27.143-92.74C255.043,181.414,255.52,181.43,256.001,181.43z M166.306,482.952l26.976-92.17l48.99,36.136L166.306,482.952z
                    M269.728,426.917l48.99-36.136l26.976,92.17L269.728,426.917z"/>
            </svg>
        '''
        ui.run(port=2606, title='Радиосвязь DMR', favicon=radio, language='ru', storage_secret='secret', reload=False, show=False)

    # Остановка приложения
    def stop(self) -> None:
        app.shutdown()

    # Статистика по радиосвязи
    def getstat(self, year: int, month: int) -> pandas.DataFrame:
        data = self._db_mysql_get_stat(year, month)
        data.insert(1, 'gid', data['senderid'])
        data['gid'] = data['gid'].map(self._db_pgsql_get_groups())
        data['sender'] = data['sender'].map(self._db_pgsql_get_users())
        data = data.rename(columns={'senderid': 'ID радиостанции', 'gid': 'Группа', 'sender': 'Должность', 'sum': 'Общее время', 'len': 'Количество сеансов', 'avg': 'Среднее время'})
        data = self._filter_recs(data)
        return data

    def getdetail(self, rid: int, year: int, month: int) -> pandas.DataFrame:
        data = self._db_mysql_get_detail(rid, year, month)
        data.insert(1, 'gid', data['senderid'])
        data['gid'] = data['gid'].map(self._db_pgsql_get_groups())
        data['sender'] = data['sender'].map(self._db_pgsql_get_users())
        data['duration'] = data['duration'].div(1000).round(2)
        data.insert(0, 'id', data.index + 1)
        data = data.rename(columns={'id': '#', 'senderid': 'ID радиостанции', 'gid': 'Группа', 'sender': 'Должность', 'starttime': 'Начат', 'duration': 'Длительность (c)', 'endtime': 'Завершен'})
        data = self._filter_recs(data)
        return data

    # Информация по звукозаписи
    def getzz(self, dt: str) -> pandas.DataFrame:
        rdt = datetime.datetime.strptime(dt, '%d.%m.%Y')
        data = self._db_pgsql_get_records(rdt.strftime('%Y-%m-%d %H:%M:%S'))
        return data
    #endregion

    #region Вспомогательные методы

    def _filter_recs(self, recs: pandas.DataFrame,) -> pandas.DataFrame:
        res = recs
        match self._role:
            case "manager":
                res = res[res['Группа'] != 'Связисты']
            case "user":
                res = res[res['Группа'] != 'Связисты']
                res = res[res['Группа'] != 'Административная']
        return res

    def _filter_groups(self, grps: list) -> list:
        res = grps
        match self._role:
            case "manager":
                res.remove('Связисты')
            case "user":
                res.remove('Связисты')
                res.remove('Административная')
        return res

    def _repeater(self, interval, function):
        Timer(interval, self._repeater, [interval, function]).start()
        function()

    # Возвращает номер недели в году
    def _get_week(self, year: int, month: int, day: int) -> int:
        return int(datetime.date(year, month, day).isocalendar()[1])

    # Переводит миллисекунды в часы, минуты, секунды
    def _msec2hms(self, msec: int) -> str:
        seconds = int((msec / 1000) % 60)
        minutes = int((msec / (1000 * 60)) % 60)
        hours = int((msec / (1000 * 60 * 60)) % 24)
        return f'{hours:02d}:{minutes:02d}:{seconds:02d}'

    # Возвращает среднее значение по времени свззи
    def _avg(self, s :int, l: int) -> str:
        return self._msec2hms(int(s/l)) if l > 0 else self._msec2hms(0)

    # Возвращает номер месяца по его имени
    def _month2num(self, name: str) -> int:
        mn = {'Январь': 1, 'Февраль': 2, 'Март': 3, 'Апрель': 4, 'Май': 5, 'Июнь': 6,
              'Июль': 7, 'Август': 8, 'Сентябрь': 9, 'Октябрь': 10, 'Ноябрь': 11, 'Декабрь': 12}
        return mn[name]

    # Возвращает имя месяца по его номеру
    def _num2month(self, num: int) -> str:
        mn = {1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь',
              7: 'Июль', 8: 'Август', 9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'}
        return mn[num]

    # Конвертирует время в UTC во время в GMT
    def _u2g(self, dt: datetime.datetime) -> datetime.datetime:
        dt = str(dt).split('.', 1)[0]

        from_tz = tz.gettz('UTC')
        to_tz = tz.gettz('GMT+7')

        utc = datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
        utc = utc.replace(tzinfo=from_tz)
        gmt = utc.astimezone(to_tz)
        return gmt

    # Конвертирует время в UTC во время в GMT (строковое представление)
    def _utc2gmt(self, dt: str) -> str:
        gmt = self._u2g(dt)
        return gmt.strftime('%d.%m.%Y %H:%M:%S')

    # Конвертирует время в GMT во время в UTC
    def _gmt2utc(self, dt: str) -> str:
        tmp = parser.parse(dt)
        dtmp = tmp.replace(tzinfo=pytz.utc) + tmp.tzinfo._offset
        return dtmp.strftime('%Y-%m-%d %H:%M:%S')

    # Возвращает текущую дату для календаря
    def _today(self) -> str:
        return datetime.datetime.now().strftime('%d.%m.%Y')

    # День, ранее которого нельзя выбрать дату в календаре
    def _minday(self) -> str:
        dt = self._db_pgsql_get_mindate()
        gmt = self._u2g(dt)
        return gmt.strftime('%Y/%m/%d')

    # День, позднеее которого нельзя выбрать дату в календаре
    def _maxday(self) -> str:
        #gmt = self._u2g(datetime.datetime.now())
        gmt = datetime.datetime.now()
        return gmt.strftime('%Y/%m/%d')

    # Устанавливает источник воспроизведения
    def _set_au_source(self, src: str) -> None:
        self._au_player._handle_source_change(src)
        app.storage.user['media'] = src
    #endregion

    #region Работа с БД
    #
    def _db_mysql_keepalive(self):
        try:
            self._mysql.execute(text('select 1;'))
        except exc.DBAPIError as err:
            pass
    #
    def _db_check_connection(self, connection):
        try:
            connection.execute(text('select 1;'))
        except exc.DBAPIError as err:
            connection.connect()

    # Получает список радиостанций с их ID
    def _db_pgsql_get_users(self) -> dict:
        self._db_check_connection(self._pgsql)
        users = self._pgsql.execute(text('select name, abonentid from abonents;'))
        users = pandas.DataFrame(users).to_dict(orient='records')
        users_dict = {int(e['abonentid']):e['name'] for e in users if e['abonentid'].isdigit() }
        return users_dict

    # Получает список радиостанций с их группами
    def _db_pgsql_get_groups(self) -> dict:
        self._db_check_connection(self._pgsql)
        groups = self._pgsql.execute(text('select abonents.abonentid, groups.groupname from abonent_group, abonents, groups where abonent_group.ab_id = abonents.ab_id and abonent_group.group_id = groups.groupid;'))
        groups = pandas.DataFrame(groups).to_dict(orient='records')
        groups_dict = {int(e['abonentid']):e['groupname'] for e in groups if e['abonentid'].isdigit() }
        return groups_dict

    # Возвращает список записей на определенную дату
    def _db_pgsql_get_records(self, dt: str) -> pandas.DataFrame:
        self._db_check_connection(self._pgsql)
        dtstart = self._gmt2utc(f'{dt} 00:00:00 GMT+7')
        dtend =  self._gmt2utc(f'{dt} 23:59:59 GMT+7')
        rec = self._pgsql.execute(text(f'select id, caller, abonents.name, datetimestart, datetimeend from sessions, abonents where (datetimestart between \'{dtstart}\' and \'{dtend}\') and abonents.abonentid = caller;'))
        rec = pandas.DataFrame(rec)
        if len(rec) > 0:
            rec['datetimestart'] = rec.apply(lambda row: self._utc2gmt(row['datetimestart']), axis=1)
            rec['datetimeend'] = rec.apply(lambda row: self._utc2gmt(row['datetimeend']), axis=1)
        return rec

    # Возвращает минимальное значение даты, на которую есть записи
    def _db_pgsql_get_mindate(self) -> str:
        self._db_check_connection(self._pgsql)
        data = self._pgsql.execute(text('select min(datetimestart) from sessions;'))
        data = data.fetchone()
        return data[0]

    # Возвращает путь к аудиозаписи в локальной файловой системе
    def _db_pgsql_get_record_path(self, id: str) -> str:
        self._db_check_connection(self._pgsql)
        data = self._pgsql.execute(text(f'select filepath from sessions where id = \'{str(id)}\';'))
        data = data.fetchone()
        return data[0]

    # Возвращает список имен радиогрупп
    def _db_pgsql_get_group_names(self) -> list:
        self._db_check_connection(self._pgsql)
        data = self._pgsql.execute(text(f'select groupname from groups;'))
        data = list(*zip(*data.fetchall()))
        data.sort()
        data.append('Все группы')
        return data

    # Функция возвращает список лет, информация за которые имеется в БД
    def _db_mysql_get_years(self) -> list:
        self._db_check_connection(self._mysql)
        tnames = self._mysql.execute(text('show tables like \'rptbiz%\';'))
        tnames = list(zip(*tnames.fetchall()))
        if len(tnames) > 0:
            years = [int(tname[6:10]) for tname in tnames[0]]
            return pandas.Series(years).drop_duplicates().to_list()
        else:
            return []

    # Функция возвращает список месяцев за указанный год, в которых есть сеансы связи
    def _db_mysql_get_months(self, year: int, intres: bool = True) -> list:
        self._db_check_connection(self._mysql)
        res = []
        tnames = self._mysql.execute(text(f'show tables like \'rptbiz{str(year)}%\';'))
        tnames = list(zip(*tnames.fetchall()))
        if len(tnames) > 0:
            for tname in tnames[0]:
                months = self._mysql.execute(text(f'select month(starttime) from {tname}  group by month(starttime);'))
                months = list(zip(*months.fetchall()))
                if len(months) > 0:
                    for month in months[0]:
                        res.append(month)
        res = pandas.Series(res).drop_duplicates().to_list()
        return res if intres else [self._num2month(itm) for itm in res]

    # Функция возвращает статистическую информацию по радиосвязи за определенный месяц года
    def _db_mysql_get_stat(self, year: int, month: int) -> pandas.DataFrame:
        self._db_check_connection(self._mysql)
        days = monthrange(year, month)[1]
        fweek = self._get_week(year, month, 1)
        lweek = self._get_week(year, month, days)
        fdm = datetime.datetime(year, month, 1, 0, 0, 0)
        ldm = datetime.datetime(year, month, days, 23, 59, 59)

        tmpl = f'rptbiz{str(year)}'
        seltmpl = ''

        for m in range(fweek, lweek + 1):
            seltmpl += f'select senderid, starttime, duration from {tmpl}{m:02d} where (`starttime` between \'{fdm.strftime("%Y-%m-%d %H:%M:%S")}\' and \'{ldm.strftime("%Y-%m-%d %H:%M:%S")}\') and (`senderid` between 1000 and 9999) and `calltype`=1 union '
        seltmpl = f'{seltmpl[0:-7]};'

        data = pandas.DataFrame(self._mysql.execute(text(seltmpl)))
        data = pandas.DataFrame.pivot_table(data[['senderid', 'duration']], index=['senderid'], aggfunc=['sum',len])
        data = data.reset_index().droplevel(1, axis=1).sort_values(['len'], ascending=False)

        data['avg'] = data.apply(lambda row: self._avg(row['sum'],row['len']), axis=1)
        data['sum'] = data.apply(lambda row: self._msec2hms(row['sum']), axis=1)

        data.insert(1, 'sender', data['senderid'])

        return data.reset_index(drop=True)

    # Функция возвращает статистическую информацию по радиосвязи за определенный месяц года
    def _db_mysql_get_detail(self, rid: int, year: int, month: int) -> pandas.DataFrame:
        self._db_check_connection(self._mysql)
        days = monthrange(year, month)[1]
        fweek = self._get_week(year, month, 1)
        lweek = self._get_week(year, month, days)
        fdm = datetime.datetime(year, month, 1, 0, 0, 0)
        ldm = datetime.datetime(year, month, days, 23, 59, 59)

        tmpl = f'rptbiz{str(year)}'
        seltmpl = ''

        for m in range(fweek, lweek + 1):
            seltmpl += f'select senderid, starttime, duration, endtime from {tmpl}{m:02d} where (`starttime` between \'{fdm.strftime("%Y-%m-%d %H:%M:%S")}\' and \'{ldm.strftime("%Y-%m-%d %H:%M:%S")}\') and (`senderid` = {rid}) and `calltype`=1 union '
        seltmpl = f'{seltmpl[0:-7]};'

        data = pandas.DataFrame(self._mysql.execute(text(seltmpl)))
        data.insert(1, 'sender', data['senderid'])
        return data.reset_index(drop=True)

    #endregion

    #region Обработчики событий
    # Изменение месяца
    def _change_month(self) -> None:
        self._sdata = self.getstat(int(self._sl_year.value), self._month2num(self._sl_month.value))
        self._cont_t1.remove(self._tb_data)
        with self._cont_t1:
            self._tb_data = ui.table.from_pandas(self._sdata).classes('w-full')
            self._tb_data.add_slot('body-cell', r"""
                <q-td :props="props" @dblclick="$parent.$emit('cell_dblclick', props)">
                    {{ props.value }}
                </q-td>
                """)
            self._tb_data.on('cell_dblclick', lambda msg: self.detail(msg.args.get('row')))
        res = None

    # Изменение года
    def _change_year(self) -> None:
        months = self._db_mysql_get_months(self._sl_year.value, False)
        self._sl_month.options = months
        self._sl_month.value = months[-1]

    # Изменение группы
    def _change_group(self) -> None:
        res = self._sdata

        if self._sl_group.value != 'Все группы':
            res = res[res['Группа'] == self._sl_group.value]

        self._cont_t1.remove(self._tb_data)
        with self._cont_t1:
            self._tb_data = ui.table.from_pandas(res).classes('w-full')
            self._tb_data.add_slot('body-cell', r"""
                <q-td :props="props" @dblclick="$parent.$emit('cell_dblclick', props)">
                    {{ props.value }}
                </q-td>
                """)
            self._tb_data.on('cell_dblclick', lambda msg: self.detail(msg.args.get('row')))
        res = None

    # Загрузка статистики
    def _download_data(self) -> None:
        alph = list(map(chr, range(ord('A'), ord('Z')+1)))
        name = f'{str(self._sl_year.value)}-{self._sl_month.value}'
        fl = io.BytesIO()
        ew = pandas.ExcelWriter(fl)

        res = self._sdata

        if self._sl_group.value != 'Все группы':
            res = res[res['Группа'] == self._sl_group.value]

        res.to_excel(ew, sheet_name=name, index=False)

        for column in res:
            column_width = max(res[column].astype(str).map(len).max(), len(column)) + 3
            col_idx = res.columns.get_loc(column)
            ew.sheets[name].column_dimensions[alph[col_idx]].width = column_width
        ew.close()
        fl.seek(0, 0)
        ui.download(fl.read(), f'{name}.xlsx')

    # Изменение даты
    def _change_date(self) -> None:
        self._mn_date.close()
        self._zdata = self.getzz(self._in_date.value)
        self._cont_t2.remove(self._tb_zdata)
        with self._cont_t2:
            self._tb_zdata = ui.table.from_pandas(self._zdata, selection='single', on_select=self._row_select).classes('w-full')
        if len(self._zdata) > 0:
            for i in range(0, 5):
                self._tb_zdata.columns[i]['label'] = ['ID Такт ПРО', 'ID радиостанции', 'Должность', 'Начало сеанса', 'Конец сеанса'][i]

    # Выбор записи в таблице звукозаписи
    def _row_select(self, e) -> None:
        if e.selection != []:
            fpath = self._db_pgsql_get_record_path(e.selection[0]['id'])
            self._sndfile.set_source(os.path.join(self._recdir, fpath))
            self._set_au_source(self._sndfile.uri)
        else:
            self._set_au_source('')

    # Кнопка Воспроизвести
    def _play(self) -> None:
        self._au_player._handle_source_change(app.storage.user['media'])
        self._au_player.play()

    # Загрузка аудиозаписи
    def _download_zdata(self) -> None:
        fl = io.BytesIO()
        data, samplerate = soundfile.read(self._sndfile.source)
        soundfile.write(fl, data, samplerate, format='WAV')
        fl.seek(0, 0)
        fname = str(pathlib.Path(self._sndfile.source).stem)
        ui.download(fl.read(), f'{fname}.wav')
    #endregion

    def detail(self, row: dict) -> None:
        rid = row.get('ID радиостанции')
        self._dialog.clear()
        with  self._dialog, ui.card().style('width: 90%; height:90%; max-width: none;'):
            with ui.row().classes('w-full items-end justify-end'):
                ui.button('Закрыть', on_click=self.close_dlg)
            with ui.scroll_area().classes('w-full h-full'):
                ui.table.from_pandas(self.getdetail(int(rid), int(self._sl_year.value), self._month2num(self._sl_month.value))).classes('w-full')
            self._dialog.open()

    def close_dlg(self) -> None:
        self._dialog.close()
        self._dialog.clear()
        with  self._dialog, ui.card().style('width: 90%; height:90%; max-width: none;'):
            with ui.row().classes('w-full items-end justify-end'):
                ui.button('Закрыть', on_click= self._dialog.close)

    #region Обработчики редиректов UI
    # Основная страница приложения
    def _uipg_main(self) -> None:
        # Обновляем полномочия пользователя
        self._role = self._users.get(app.storage.user.get('username')).get("role")

        # Готовим начальные данные
        years = self._db_mysql_get_years()
        months = self._db_mysql_get_months(years[-1], False)
        groups = self._filter_groups(self._db_pgsql_get_group_names())
        self._sdata = self.getstat(years[-1], self._month2num(months[-1]))

        # Кнопка выхода
        with ui.row().classes('w-full items-end justify-end'):
            ui.label(app.storage.user.get('username')).classes('h-8')
            ui.button(on_click=lambda: (app.storage.user.clear(), ui.navigate.to('/login')), icon='logout').classes('h-6')

            # Панели
            with ui.tabs().classes('w-full') as self._tabs:
                self._tab1 = ui.tab('Статистика')
                if self._role != "user":
                    self._tab2 = ui.tab('Звукозапись')

        with ui.tab_panels(self._tabs, value=self._tab1).classes('w-full'):
            with ui.tab_panel(self._tab1):
                self._cont_t1 = ui.row().classes('w-full items-end')
            if self._role != "user":
                with ui.tab_panel(self._tab2):
                    self._cont_t2 = ui.row().classes('w-full items-end')

        with self._cont_t1:
            self._sl_month = ui.select(months, on_change=self._change_month, value=months[-1])
            self._sl_year = ui.select(years, on_change=self._change_year, value=years[-1])
            self._sl_group = ui.select(groups, on_change=self._change_group, value=groups[-1])
            self._bt_download = ui.button('Скачать', on_click=self._download_data, icon='download')
            ui.separator()
            self._tb_data = ui.table.from_pandas(self._sdata).classes('w-full')
            self._tb_data.add_slot('body-cell', r"""
                <q-td :props="props" @dblclick="$parent.$emit('cell_dblclick', props)">
                    {{ props.value }}
                </q-td>
                """)
            self._tb_data.on('cell_dblclick', lambda msg: self.detail(msg.args.get('row')))

            with ui.dialog() as self._dialog, ui.card().classes('w-90'):
                ui.button('Закрыть', on_click=self._dialog.close)

        self._sl_month.classes('w-[23%] h-11 mx-auto')
        self._sl_year.classes('w-[23%] h-11 mx-auto')
        self._sl_group.classes('w-[23%] h-11 mx-auto')
        self._bt_download.classes('w-[23%] h-9 flex-auto')

        if self._role == "user":
            return

        with self._cont_t2:
            self._in_date = ui.input('Дата', value=self._today(), on_change=self._change_date)
            with self._in_date as date:
                with date.add_slot('append'):
                    ui.icon('edit_calendar').on('click', lambda: menu.open()).classes('cursor-pointer')
                self._mn_date = ui.menu()
                self._mn_date.props(remove='auto-close')
                with self._mn_date as menu:
                    ui.date(mask='DD.MM.YYYY').props(add='no-unset').props(f''':options="date => date >= '{self._minday()}' && date <= '{self._maxday()}'"''').bind_value(self._in_date)

            self._au_player = ui.audio('', autoplay=True).classes('hidden')
            self._bt_zplay = ui.button('Воспроизвести', on_click=self._play, icon='arrow_right')
            self._bt_zdownload = ui.button('Скачать', on_click=self._download_zdata, icon='download')
            ui.separator()
            self._tb_zdata = ui.table.from_pandas(self._zdata, selection='single', on_select=self._row_select).classes('w-full')
            self._change_date()

        self._in_date.classes('w-[30%] h-11 mx-auto')
        self._bt_zplay.classes('w-[30%] h-9 flex-auto')
        self._bt_zdownload.classes('w-[30%] h-9 flex-auto')
        self._set_au_source('')

    # Страница входа
    def _uipg_login(self) -> Optional[RedirectResponse]:
        def try_login() -> None:
            if self._users.get(username.value).get("pass") == password.value:
                app.storage.user.update({'username': username.value, 'authenticated': True})
                self._role = self._users.get(username.value).get("role")
                ui.navigate.to(app.storage.user.get('referrer_path', '/'))
            else:
                ui.notify('Неверное имя пользователя или пароль', color='negative')

        if app.storage.user.get('authenticated', False):
            return RedirectResponse('/')
        with ui.card().classes('absolute-center'):
            username = ui.input('Логин').on('keydown.enter', try_login)
            password = ui.input('Пароль', password=True, password_toggle_button=True).on('keydown.enter', try_login)
            ui.button('Войти', on_click=try_login)
        return None
    #endregion

def main():
    config = {}
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dmrapp.json'), 'r', encoding='utf-8') as file:
        config = json.load(file)

    App = DMRApp("postgresql+psycopg2://postgres:" + config['dbpass'] + "@127.0.0.1:5432/postgres", "mysql+mysqldb://root:" + config['dbpass'] + "@127.0.0.1:3306/xpt_db", config['users'], config['recdir'])
    App.start()

if __name__ == "__main__":
    main()