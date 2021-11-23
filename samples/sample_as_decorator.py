import time
import random
import logging
from pyloadlimiter import LoadLimiter

logging.basicConfig(format='%(asctime)s %(threadName)s [%(name)s %(levelname)s] %(message)s', level=logging.DEBUG)

limiter = LoadLimiter(name='TestQueue80in20', maxload=80, period=20)

@limiter()
def do_things():
    logging.info('doing things!')

@limiter(load=5)
def do_expensive():
    logging.info('doing expensive things!')

@limiter(load=15)
def do_really_expensive():
    logging.info('doing REALLY expensive things!')

@limiter(load=15, wait=False)
def do_really_expensive_no_wait():
    logging.info('doing REALLY expensive things!')

for i in range(0, 50):
    r = random.randint(1, 10) 
    if r <= 2:
        try:
            do_really_expensive_no_wait()
        except Exception as e:
            logging.error('error calling do_really_expensive_no_wait: ', exc_info=1)
    elif r <= 4:
        do_really_expensive()
    elif r <= 6:
        do_expensive()
    else:
        do_things()

    sleep_factor = 1.0
    time.sleep(sleep_factor * random.randint(0, 1000)/1000)
