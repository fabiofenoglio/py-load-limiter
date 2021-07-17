import time
import random
import logging
from load_limiter import LoadLimiter

logging.basicConfig(format='%(asctime)s %(threadName)s [%(name)s %(levelname)s] %(message)s', level=logging.DEBUG)

limiter = LoadLimiter(name='TestQueue80in20', maxload=80, period=20)
demo_start = time.time()
demo_requested = 0
demo_produced = 0

def print_stats():
    demo_duration = time.time() - demo_start
    logging.info('total duration: {} sec'.format(demo_duration))
    logging.info('total requested: {} ( {}/sec )'.format(demo_requested, demo_requested/demo_duration))
    logging.info('total produced: {} ( {}/sec )'.format(demo_produced, demo_produced/demo_duration))

for i in range(0, 200):
    load_factor = 1.05
    load = load_factor * random.randint(1000, 3000)/1000
    if i > 200:
        load = 0
    demo_requested += load

    logging.info('submitting {}'.format(load))
    with limiter.waiting(load):
        demo_produced += load
        logging.info('task allowed for {}'.format(load))

    sleep_factor = 1.0
    time.sleep(sleep_factor * random.randint(0, 1000)/1000)

print_stats()
