from random import randint, choice
from faker import Faker
from time import sleep
from flask import Flask

from multiprocessing import Process, Value

import os
import pymysql
from models import User, Order, init_db
import logging
POSSIBLE_ACTIONS = ("insert_user", "update_user", "insert_order", "delete_user", "delete_order")

app = Flask(__name__)


def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)

@app.route('/')
def main():
    db_name = os.getenv("MYSQL_DB","demo")
    db_user = os.getenv("MYSQL_USER", "root")
    db_pass = os.getenv("MYSQL_PASS")
    cloud_sql_connection_name = os.environ["CLOUD_SQL_CONNECTION_NAME"]

    unix_socket = '/cloudsql/{}'.format(cloud_sql_connection_name)
    cnx = pymysql.connect(user=db_user, password=db_pass,
                              unix_socket=unix_socket, db=db_name)

    with cnx.cursor() as cursor:
        cursor.execute('SELECT NOW() as now;')
        result = cursor.fetchall()
        current_time = result[0][0]
    cnx.close()

    return str(current_time)

def execution_loop():
    print('Init db')
    init_db()

    while True:
        action = choice(POSSIBLE_ACTIONS)
        print('Action: '  + action)  # will not print anything
        if action == "insert_user":
            insert_user()
        elif action == "update_user":
            update_user()
        elif action == "insert_order":
            add_order()
        elif action == "delete_user" and choice(POSSIBLE_ACTIONS) == "delete_user":
            delete_user()
        elif action == "delete_order" and choice(POSSIBLE_ACTIONS) == "delete_order":
            delete_order()
        sleep(0.5)

def random_gender():
    return choice(('m','f','n'))

def insert_user():
    faker = Faker()
    User(
        id=str(random_with_N_digits(9)),
        name=faker.name(),
        address=faker.address(),
        gender=random_gender(),
    ).create()

def update_user():
    user = User.random()
    if user is not None:
        if choice((True, False)):
            user.gender = random_gender()
        user.save()

def delete_user():
    user = User.random()
    if user is not None:
        user.delete()

def add_order():
    order = Order.random()
    if order is not None:
        Order(
            user_id=User.id,
            product=choice(("Table", "Chair", "Book", "Laptop", "Keyboard"))
        ).create()

def delete_order():
    order = Order.random()
    if order is not None:
        order.delete()

if __name__ == '__main__':
    p = Process(target=execution_loop)
    p.start()
    app.run(host='127.0.0.1', port=8080, debug=True)
    p.join()
