import time
import random
import logging
from pyloadlimiter import LoadLimiter, FileSystemLoadLimiterStorageAdapter

logging.basicConfig(format='%(asctime)s %(threadName)s [%(name)s %(levelname)s] %(message)s', level=logging.DEBUG)

storage_adapter = FileSystemLoadLimiterStorageAdapter()

def build_limiter():
    return LoadLimiter(
        name='TestQueue80in20', 
        maxload=80, 
        period=20, 
        storage_adapter=storage_adapter
    )

limiter = build_limiter()

demo_start = time.time()
demo_requested = 0
demo_produced = 0

load_factor = 1.20
sleep_factor = 1.0

for i in range(0, 100):
    load = load_factor * random.randint(1000, 3000)/1000

    demo_requested += load

    # test persistence every 20 iterations
    if i > 0 and i % 20 == 0:
        # store to disk
        logging.info('*' * 80)

        limiter.flush()

        # create a fresh instance of the limiter 
        # and restore its status from the storage adapter
        limiter = build_limiter()
        limiter.restore()
        
        logging.info('*' * 80)

    v1 = limiter.submit(load)

    if v1.accepted:
        demo_produced += load
    else:
        logging.info('load of {} was rejected'.format(load))

    time.sleep(sleep_factor * random.randint(0, 1000)/1000)

demo_duration = time.time() - demo_start
logging.info('*' * 80)
logging.info('total duration: {:.0f} sec'.format(demo_duration))
logging.info('total requested: {:.2f} ( {:.2f}/sec )'.format(demo_requested, demo_requested/demo_duration))
logging.info('total produced: {:.2f} ( {:.2f}/sec )'.format(demo_produced, demo_produced/demo_duration))
