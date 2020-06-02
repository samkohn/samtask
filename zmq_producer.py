import zmq
import time
import random
import sqlite3
import argparse
from collections import deque
import logging
logging.basicConfig(level=logging.DEBUG)

def current_time():
    return int(time.time())

def retry(func):
    def newfunc(*args, **kwargs):
        error = None
        for attempt_number in range(10):
            try:
                logging.debug('Calling %s: attempt %d', func, attempt_number)
                x = func(*args, **kwargs)
                break
            except sqlite3.OperationalError as e:
                time.sleep(30*random.random())
                error = e
        else:  # no-break
            raise error
        return x
    return newfunc

@retry
def next_task(cursor, ntasks):
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

@retry
def finish_tasks(cursor, task_ids):
    if len(task_ids) == 0:
        return
    values_statement = ', '.join(['(?, "finished", ?)' for _ in task_ids])
    timestamp = current_time()
    substitution = []
    for task_id in task_ids:
        substitution.append(task_id)
        substitution.append(timestamp)
    cursor.execute('''INSERT INTO history
        VALUES ''' + values_statement,
        tuple(substitution))
    conn.commit()

@retry
def error_task(cursor, task_id):
    cursor.execute('''INSERT INTO history
        VALUES (?, "error", ?)''',
        (task_id, current_time()))
    conn.commit()

@retry
def resubmit_tasks(cursor, task_ids):
    if len(task_ids) == 0:
        return
    values_statement = ', '.join(['(?, "not started", ?)' for _ in task_ids])
    timestamp = current_time()
    substitution = []
    for task_id in task_ids:
        substitution.append(task_id)
        substitution.append(timestamp)
    cursor.execute('''INSERT INTO history
        VALUES ''' + values_statement,
        tuple(substitution))
    conn.commit()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('database')
    parser.add_argument('open_time_min', type=float)
    parser.add_argument('maxtimelimit', type=float)
    parser.add_argument('address')
    parser.add_argument('--cachesize', type=int, default=100)
    args = parser.parse_args()
    timelimit_min = args.open_time_min
    timelimit_sec = 60 * timelimit_min
    start_time = time.time()
    finish_time = start_time + timelimit_sec
    die_time = start_time + 60 * args.maxtimelimit

    context = zmq.Context()
    producer = context.socket(zmq.REP)
    producer.setsockopt(zmq.RCVTIMEO, 10000)
    producer.bind(args.address)
    print('bound')
    cache = deque()
    finished_cache = []
    db_is_done = False
    with sqlite3.connect(args.database, isolation_level=None) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            while time.time() < die_time:
                try:
                    request = producer.recv()
                except zmq.error.Again:
                    continue
                worker_done = request[-4:] == b'DONE'
                if worker_done:
                    finished_id = int(request[:-4])
                else:
                    finished_id = int(request)
                print('received request', request)
                if finished_id < 0:
                    error_task(cursor, -1 * finished_id)
                elif finished_id > 0:
                    finished_cache.append(finished_id)
                    if len(finished_cache) > args.cachesize:
                        start_time = time.time()
                        finish_tasks(cursor, finished_cache)
                        logging.debug('%s finished finish_task in %.03f', args.address,
                                time.time() - start_time)
                        finished_cache = []
                else:
                    pass
                if worker_done or time.time() > finish_time:
                    logging.debug('sending DONE')
                    producer.send_multipart([b'0', b'DONE'])
                else:
                    if len(cache) < 10:
                        if not db_is_done:
                            start_time = time.time()
                            logging.debug("%s entering next_task", args.address)
                            new_tasks = next_task(cursor, args.cachesize)
                            logging.debug("%s finished next_task in %.03f",
                                    args.address, time.time() - start_time)
                            if new_tasks is not None:
                                cache.extend(new_tasks)
                            else:
                                db_is_done = True
                        if len(cache) > 0:
                            task = cache.popleft()
                        else:
                            task = None
                    else:
                        task = cache.popleft()
                    if task is None:
                        producer.send_multipart([b'0', b'DONE'])
                        logging.debug('sending DONE')
                    else:
                        logging.debug('sending task')
                        producer.send_multipart([b'%d' % task['TaskID'],
                            task['Command'].encode()])
                    print('sent job command')
                if time.time() > finish_time:
                    # Return remaining cache items to database
                    resubmit_tasks(cursor, [task['TaskID'] for task in cache])
                    # Log finished tasks as finished
                    finish_tasks(cursor, finished_cache)
        except KeyboardInterrupt:
            resubmit_tasks(cursor, [task['TaskID'] for task in cache])
            finish_tasks(cursor, finished_cache)
