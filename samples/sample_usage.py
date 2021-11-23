import time
import random
import logging
from py_load_limiter import LoadLimiter

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

    v1 = limiter.submit(load)

    if v1.accepted:
        demo_produced += load

    elif v1.retry_in is not None:
        logging.info('load of {} was rejected and can be resubmitted in {} secs'.format(load, v1.retry_in))
        time.sleep(v1.retry_in)
        logging.info('resubmitting load of {} after waiting'.format(load))
        demo_requested += load
        v2 = limiter.submit(load=load)
        if v2.accepted:
            demo_produced += load

    else:
        logging.info('load of {} was rejected with no indications on the required delay before resubmitting'.format(load))

    time.sleep(sleep_factor * random.randint(0, 1000)/1000)

demo_duration = time.time() - demo_start
logging.info('*' * 80)
logging.info('total duration: {:.0f} sec'.format(demo_duration))
logging.info('total requested: {:.2f} ( {:.2f}/sec )'.format(demo_requested, demo_requested/demo_duration))
logging.info('total produced: {:.2f} ( {:.2f}/sec )'.format(demo_produced, demo_produced/demo_duration))
