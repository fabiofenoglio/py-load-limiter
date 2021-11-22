import time
import logging
from load_limiter import LoadLimiter
from datetime import datetime, timedelta
from matplotlib import pyplot
from matplotlib.animation import FuncAnimation
from random import randrange, randint

logging.basicConfig(format='%(asctime)s %(threadName)s [%(name)s %(levelname)s] %(message)s', level=logging.DEBUG)
logging.getLogger("matplotlib").setLevel(logging.INFO)

limiter = LoadLimiter(
    name='TestQueue100in20', 
    maxload=100, 
    period=20,
    penalty_factor=0.0,
    request_overhead_penalty_factor=0.0
)

load_meter = LoadLimiter(maxload=99999999, period=20)

# start with some load
limiter.submit(20)
load_meter.submit(20)

x_data, y_data1a, y_data1b, y_data2a, y_data2b = [], [], [], [], []
y_data3a, y_data3b = [], []

figure, (ax1, ax2, ax3) = pyplot.subplots(3)
figure.suptitle('Requested vs Server load')

line1a, = ax1.plot(x_data, y_data1a, 'bo')
line1b, = ax1.plot(x_data, y_data1b, 'rx')
line2a, = ax2.plot(x_data, y_data2a, '-')
line2b, = ax2.plot(x_data, y_data2b, '-')

ax3.set_ylim(0, 10.0)
line3a, = ax3.plot(x_data, y_data3a, '-')
line3b, = ax3.plot(x_data, y_data3b, '-')

ax2.axhline(y = 100.0, color = 'r', linestyle = 'dotted')
ax3.axhline(y = 5.0, color = 'r', linestyle = 'dotted')

counter = [0]
wait_until = [None]
started_at = [datetime.now()]
total_requested = [0]
total_served = [0]


def update(frame):
    
    load_factor = 1.33
    #load = load_factor * randint(1000, 3500)/1000
    load = 3
    now = datetime.now()

    waiting_until = wait_until[0]
    if waiting_until is not None and now <= waiting_until:
        logging.info('still waiting ...')
        return line1a,

    total_requested[0] += load

    load_meter.submit(load=load)

    v1 = limiter.submit(load=load)
    if v1.accepted:
        served = load
    else:
        logging.info('load of {} was rejected. asked to wait {}s.'.format(load, v1.retry_in))
        #wait_until[0] = datetime.now() + timedelta(seconds=v1.retry_in)
        #logging.info('will resume at {}', wait_until[0])
        served = 0

    total_served[0] += served
    # x_data.append(now)
    x_data.append(frame)

    y_data1a.append(load if v1.accepted else None)
    y_data1b.append(load if not v1.accepted else None)

    y_data2a.append(limiter.window_total)
    y_data2b.append(load_meter.window_total)

    total_elapsed = (datetime.now() - started_at[0]).total_seconds()
    avg_served = total_served[0] / total_elapsed
    avg_requested = total_requested[0] / total_elapsed

    y_data3a.append(avg_served)
    y_data3b.append(avg_requested)

    line1a.set_data(x_data, y_data1a)
    line1b.set_data(x_data, y_data1b)
    line2a.set_data(x_data, y_data2a)
    line2b.set_data(x_data, y_data2b)
    line3a.set_data(x_data, y_data3a)
    line3b.set_data(x_data, y_data3b)

    ax1.relim()
    ax2.relim()
    ax3.relim()

    ax1.autoscale_view()
    ax2.autoscale_view()
    ax3.autoscale_view()
    
    return line1a,

animation = FuncAnimation(figure, update, interval=500)

pyplot.show()
