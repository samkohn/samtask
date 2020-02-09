import os
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'VERSION')) as f:
    version = f.read()

setup(
        name='samtask',
        version=version,
        description='A system for tracking and processing tasks in a '
        'batch environment',
        url='https://github.com/samkohn/samtask',
        author='Sam Kohn',
        author_email='skohn@lbl.gov',
        scripts=[
            'add_tasks.py',
            'find_tasks.py',
            'get_history.py',
            'get_task.py',
            'init_tasks.py',
            'update_status.py',
            'worker.py',
            'zmq_producer.py'],

)
