import time
import random
import logging
from pyloadlimiter import LoadLimiter

logging.basicConfig(format='%(asctime)s %(threadName)s [%(name)s %(levelname)s] %(message)s', level=logging.DEBUG)

limiter = LoadLimiter(name='TestQueue80in20', maxload=80, period=20)

demo_start = time.time()
demo_requested = 0
demo_produced = 0

load_factor = 1.20
sleep_factor = 1.0

for i in range(0, 100):
    load = load_factor * random.randint(1000, 3000)/1000

    demo_requested += load

    logging.info('submitting {}'.format(load))
    with limiter.waiting(load):
        demo_produced += load
        logging.info('task allowed for {}'.format(load))

    time.sleep(sleep_factor * random.randint(0, 1000)/1000)

demo_duration = time.time() - demo_start
logging.info('*' * 80)
logging.info('total duration: {:.0f} sec'.format(demo_duration))
logging.info('total requested: {:.2f} ( {:.2f}/sec )'.format(demo_requested, demo_requested/demo_duration))
logging.info('total produced: {:.2f} ( {:.2f}/sec )'.format(demo_produced, demo_produced/demo_duration))
