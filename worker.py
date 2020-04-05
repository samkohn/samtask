from __future__ import print_function
import zmq
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('address')
parser.add_argument('--outdir')
parser.add_argument('--max-tasks', type=int, default=-1)
args = parser.parse_args()

if args.outdir is not None:
    os.chdir(args.outdir)

forever = args.max_tasks < 0

context = zmq.Context()
worker = context.socket(zmq.REQ)
worker.setsockopt(zmq.RCVTIMEO, 300000)
worker.connect(args.address)
print('connected')

ntasks_recv = 0
task_id = 0
try:
    while forever or ntasks_recv < args.max_tasks:
        worker.send(str(task_id).encode())
        print('sent command')
        task_id, line = worker.recv_multipart()
        task_id = int(task_id)
        print('received command')
        if line == b'DONE':
            print('received DONE')
            break
        ntasks_recv += 1
        print(line)
        task_id = int(task_id)
        try:
            result = os.system(line)
            if result != 0:
                task_id *= -1
        except:
            task_id *= -1
    else:  # no-break
        worker.send(str(task_id).encode() + b'DONE')
        worker.recv()
finally:
    print('closing worker socket')
    worker.close(linger=0)


print('received %d tasks' % ntasks_recv)
