from random import randint, choice
from faker import Faker
from time import sleep
from flask import Flask

from multiprocessing import Process, Value

import os
import pymysql
from models import Voter, Poll, init_db
import logging
POSSIBLE_ACTIONS = ("insert", "update", "answer_poll", "delete_voter", "delete_poll")

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
        if action == "insert":
            insert_fake_voter()
        elif action == "update":
            update_random_voter()
        elif action == "answer_poll":
            answer_random_poll()
        elif action == "delete_voter" and choice(POSSIBLE_ACTIONS) == "delete_voter":
            delete_random_voter()
        elif action == "delete_poll" and choice(POSSIBLE_ACTIONS) == "delete_poll":
            delete_random_poll()
        sleep(0.5)

def random_gender():
    return choice(('m','f','t','n'))

def insert_fake_voter():
    faker = Faker()
    Voter(
        id=str(random_with_N_digits(9)),
        name=faker.name(),
        address=faker.address(),
        gender=random_gender(),
    ).create()

def update_random_voter():
    voter = Voter.random()
    if voter is not None:
        if choice((True, False)):
            voter.gender = random_gender()
        voter.save()

def delete_random_voter():
    voter = Voter.random()
    if voter is not None:
        voter.delete()

def answer_random_poll():
    voter = Voter.random()
    if voter is not None:
        Poll(
            voter_id=voter.id,
            answer=choice(("Bibi", "Gantz", "Benet", "Emet", "Meshutefet", "Shas"))
        ).create()

def delete_random_poll():
    poll = Poll.random()
    if poll is not None:
        poll.delete()

if __name__ == '__main__':
    p = Process(target=execution_loop)
    p.start()
    app.run(host='127.0.0.1', port=8080, debug=True)
    p.join()
