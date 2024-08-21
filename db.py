import asyncio
import datetime
import logging
import traceback

import asyncpg
import requests

from asyncpg_utility import NamedParameterQuery, NamedParameterConnection

from config import DSN

LOGGER = logging.getLogger(__name__)


def get_proxies():
    url = 'http://ipArch.local:5558/'
    r = requests.get(url)
    return r.json()


async def get_account(username=None):
    conn = await asyncpg.connect(DSN)
    if username is None:
        query = 'SELECT * FROM passes.accounts'
    else:
        query = 'SELECT * FROM passes.accounts WHERE login = {{USERNAME}}'
    my_named_parameter_query = NamedParameterQuery(query)
    my_named_parameter_conn = NamedParameterConnection(conn, my_named_parameter_query)
    if username is None:
        results = await my_named_parameter_conn.fetch()
    else:
        results = await my_named_parameter_conn.fetch(username=username)
    for result in results:
        print(result)
    return results


async def get_last_pass():
    print('try to get last pass')
    query = 'select "number" from passes.passes p order by "number" desc limit 1'
    conn = await asyncpg.connect(DSN)
    my_named_parameter_query = NamedParameterQuery(query)
    my_named_parameter_conn = NamedParameterConnection(conn, my_named_parameter_query)
    results = await my_named_parameter_conn.fetch()
    try:
        return int(results[0]['number'])
    except:
        traceback.print_exc()
        return None


async def set_account(username, password, cookie_value):
    conn = await asyncpg.connect(DSN)
    with open('sql/insert_account.sql') as f:
        query = f.read()
    my_named_parameter_query = NamedParameterQuery(query)
    my_named_parameter_conn = NamedParameterConnection(conn, my_named_parameter_query)

    results = await my_named_parameter_conn.execute(username=username, password=password, cookie=cookie_value)
    #print(results)
    return results


async def set_pass(pass_dict: dict):
    conn = await asyncpg.connect(DSN)
    with open('sql/insert_pass.sql') as f:
        query = f.read()
    my_named_parameter_query = NamedParameterQuery(query)
    my_named_parameter_conn = NamedParameterConnection(conn, my_named_parameter_query)
    series = str(pass_dict['seriesAndNumber']).split(' ')[0].strip()
    number = str(pass_dict['seriesAndNumber']).split(' ')[1].strip()
    if pass_dict['statusCode'] == 'Active':
        status = True
    else:
        status = False
    arguments = {
        'series': series,
        'number': number,
        'td': pass_dict['passTimeOfDay'],
        'status': status,
        'vin': pass_dict['vin'],
        'reg': pass_dict['regNum'],
        'start': datetime.datetime.strptime(pass_dict['startDate'], '%Y-%m-%dT%H:%M:%SZ'),
        'finish': datetime.datetime.strptime(pass_dict['finishDate'], '%Y-%m-%dT%H:%M:%SZ')
    }
    results = await my_named_parameter_conn.execute(**arguments)
    #print(results)
    return results


async def test_get():
    conn = await asyncpg.connect(DSN)
    query = 'SELECT * FROM tg.history WHERE path = {{PATH}} and "function" like {{FUNCTION}} order by asctime desc limit 50'
    my_named_parameter_query = NamedParameterQuery(query)
    my_named_parameter_conn = NamedParameterConnection(conn, my_named_parameter_query)
    results = await my_named_parameter_conn.fetch(path='Сервис.ЕГТС.Эмулятор', function='send_%')
    for result in results:
        print(result)
    return results


if __name__ == '__main__':
    asyncio.run(get_account('nixncom@gmail.com'))