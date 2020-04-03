import zmq
import time
import random
import sqlite3
import argparse
from collections import deque

def current_time():
    return int(time.time())

def next_task(cursor, ntasks):
    try:
        cursor.execute('BEGIN EXCLUSIVE TRANSACTION')
        cursor.execute('''SELECT TaskID, Command, Status
            FROM (
                SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY TaskID
                    ORDER BY Timestamp DESC) rn
                FROM tasks JOIN history USING (TaskID)
            )
            WHERE rn = 1 AND Status = "not started"
            LIMIT ?''', (ntasks,))
        rows = cursor.fetchall()
        print(len(rows))
        if len(rows) == 0:
            cursor.execute('COMMIT')
            conn.commit()
            return None
        else:
            for row in rows:
                cursor.execute('''INSERT INTO history
                    VALUES (?, "started", ?)''',
                    (row['TaskID'], current_time()))
            cursor.execute('COMMIT')
            conn.commit()
            return rows
    except sqlite3.Error:
        conn.commit()
        return None

def finish_task(cursor, task_id):
    cursor.execute('''INSERT INTO history
        VALUES (?, "finished", ?)''',
        (task_id, current_time()))
    conn.commit()

def error_task(cursor, task_id):
    cursor.execute('''INSERT INTO history
        VALUES (?, "error", ?)''',
        (task_id, current_time()))
    conn.commit()

def resubmit_task(cursor, task_id):
    cursor.execute('''INSERT INTO history
        VALUES (?, "not started", ?)''',
        (task_id, current_time()))
    conn.commit()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('database')
    parser.add_argument('timelimit_min', type=float)
    args = parser.parse_args()
    timelimit_min = args.timelimit_min
    timelimit_sec = 60 * args.timelimit_min
    start_time = time.time()
    finish_time = start_time + timelimit_sec

    context = zmq.Context()
    producer = context.socket(zmq.REP)
    producer.bind('tcp://*:52837')
    print('bound')
    cache = deque()
    with sqlite3.connect(args.database, isolation_level=None) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        while True:
            request = producer.recv()
            worker_done = request[-4:] == b'DONE'
            if worker_done:
                finished_id = int(request[:-4])
            else:
                finished_id = int(request)
            print('received request')
            if finished_id < 0:
                error_task(cursor, -1 * finished_id)
            elif finished_id > 0:
                finish_task(cursor, finished_id)
            else:
                pass
            if worker_done or time.time() > finish_time:
                producer.send_multipart([b'0', b'DONE'])
            else:
                if len(cache) < 10:
                    new_tasks = next_task(cursor, 100)
                    cache.extend(new_tasks)
                    if len(cache) > 0:
                        task = cache.popleft()
                    else:
                        task = None
                else:
                    task = cache.popleft()
                if task is None:
                    producer.send_multipart([b'0', b'DONE'])
                else:
                    producer.send_multipart([b'%d' % task['TaskID'],
                        task['Command'].encode()])
                print('sent job command')
            if time.time() > finish_time:
                # Return remaining cache items to database
                for task in cache:
                    resubmit_task(cursor, task['TaskID'])
