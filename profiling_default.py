import time
import logging
from load_limiter import LoadLimiter
from datetime import datetime
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

# start with some load
limiter.submit(20)

x_data, y_data, y_data2 = [], [], []

figure, (ax1, ax2) = pyplot.subplots(2)
figure.suptitle('Requested vs Server load')

# figure = pyplot.figure()
line = ax1.bar(x_data, y_data)
#line2 = ax1.bar(x_data, y_data2)
line.show()

ax1.xaxis_date()

y_data3 = []
line3, = ax2.plot_date(x_data, y_data3, '-')

def update(frame):
    load_factor = 1.33
    load = load_factor * randint(1000, 3500)/1000
    now = datetime.now()

    v1 = limiter.submit(load=load)
    if v1.accepted:
        served = load
    else:
        served = 0
        logging.info('load of {} was rejected. asked to wait {}s but won\'t.'.format(load, v1.retry_in))

    x_data.append(now)
    y_data.append(load)
    y_data2.append(served)

    y_data3.append(limiter.window_total)

    #line.set_data(x_data, y_data)
    #line2.set_data(x_data, y_data2)

    for rect, y in zip(line, y_data):
        rect.set_height(y)

    line3.set_data(x_data, y_data3)

    #figure.gca().relim()
    ax1.relim()
    ax2.relim()
    #figure.gca().autoscale_view()
    ax1.autoscale_view()
    ax2.autoscale_view()
    return line,

animation = FuncAnimation(figure, update, interval=500)

pyplot.show()
