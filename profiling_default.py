import logging
import time
import threading
from load_limiter import LoadLimiter
from datetime import datetime, timedelta
from matplotlib import pyplot
from matplotlib.animation import FuncAnimation
from random import gauss
import unicodedata
import re

from load_limiter.load_limiter import CompositeLoadLimiter

logging.basicConfig(format='%(asctime)s %(threadName)s [%(name)s %(levelname)s] %(message)s', level=logging.DEBUG)
logging.getLogger("matplotlib").setLevel(logging.INFO)

DEFAULT_MAX_LOAD = 100
DEFAULT_TIME_PERIOD = 10

class LoadSubmitter():
    def __init__(self,
        limiter: LoadLimiter,
        load_factor = 0.80,
        average_request_interval = 300,
        delay_compliance_factor = 1.0,
    ):
        self.limiter = limiter
        self.load_factor = load_factor
        self.delay_compliance_factor = delay_compliance_factor
        self.avg_request_interval_ms = average_request_interval

        adl = limiter.maxload / limiter.period
        alpr = (adl * average_request_interval)

        self.avg_load_per_request = load_factor * alpr

class ProfilingTestBed():
    def __init__(self,
        name: str,
        limiter: LoadLimiter, 
        submitter: LoadSubmitter
    ):
        self.name = name
        self.limiter = limiter
        self.submitter = submitter

class Profiler():
    def __init__(self,
        testbed: ProfilingTestBed
    ):
        limiter = testbed.limiter
        submitter = testbed.submitter
        self.testbed = testbed
        self.name = testbed.name
           
        self.limiter = limiter
        self.submitter = submitter
        self.logger = logging.getLogger("profiler")

        self.warmup_load_factor = 0.0
        self.data_tick_interval_ms = 20
        self.apply_tick_correction = False
        self.max_points = (5 * limiter.period * 1000 / submitter.avg_request_interval_ms)
        self.total_points = self.max_points * 2

        # configure another LoadLimiter instance with much bigger load. 
        # We will use it just to measure how much the raw requested load would be without trimming
        meter_logger = logging.getLogger("meter")
        meter_logger.setLevel(logging.INFO)
        self.load_meter = LoadLimiter(maxload=limiter.maxload * 100, period=limiter.period, logger=meter_logger)

        self.data_lock = threading.Lock()
        self.run = [True]
        self.counter = [0]
        self.wait_until = [None]
        self.started_at = [datetime.now()]
        self.total_requested = [0]
        self.total_served = [0]
        self.latest_data_update = [datetime.now()]
        self.data_update_tick_correction_factor = [1.00]
        self.next_load_cap = [None]
        self.x_data = []
        self.y_data1a, self.y_data1b = [], [] 
        self.y_data2a, self.y_data2b = [], []
        self.y_data3a, self.y_data3b = [], []

    def is_running(self):
        return self.run[0]

    def start(self) -> threading.Thread:
        # start with some load
        initial_load = self.limiter.maxload * self.warmup_load_factor
        if initial_load > 0:
            self.limiter.distribute(initial_load)
            self.load_meter.distribute(initial_load)
            
        def data_updater_thread_handler():
            while self.run[0]:
                with self.data_lock:
                    self.data_updater()
                time.sleep(self.data_update_tick_correction_factor[0] * self.data_tick_interval_ms / 1000.0)

        self.thread = threading.Thread(target = data_updater_thread_handler, args = ())
        self.thread.start()

        return self.thread

    def stop(self):
        self.logger.info('profiler stopping')
        self.run[0] = False
        self.thread.join()
        self.logger.info('profiler stopped')
        
    def data_updater(self):
        if not self.run[0]:
            return

        submitter = self.submitter
        if self.next_load_cap[0] is not None:
            load = self.next_load_cap[0]
            self.next_load_cap[0] = None
            self.logger.info('resubmitting previously rejected load of %.2f', load)
        else:
            load = gauss(submitter.avg_load_per_request, (submitter.avg_load_per_request / 3.33))/1000
            if load < 0.1:
                self.logger.warning('applied trimming to requested load ( <= 0 )')
                load = 0.1

        now = datetime.now()

        if self.apply_tick_correction:
            cf = self.data_update_tick_correction_factor[0]
            milliseconds_since_last_data_update = (now - self.latest_data_update[0]).total_seconds() * 1000
            self.logger.info("ELAPSED: %.0f ms, cf %.2f", milliseconds_since_last_data_update, cf)
            self.latest_data_update[0] = datetime.now()

            if milliseconds_since_last_data_update > self.data_tick_interval_ms:
                cf -= 0.01
                if cf < 0.70:
                    cf = 0.70
                    self.logger.warning('correction factor is negatively overflowing')
            else:
                cf += 0.01
                if cf > 1.10:
                    cf = 1.10
                    self.logger.warning('correction factor is positively overflowing')
            self.data_update_tick_correction_factor[0] = cf

        waiting_until = self.wait_until[0]
        if waiting_until is not None and now <= waiting_until:
            return

        total_elapsed = (now - self.started_at[0]).total_seconds()
        total_elapsed_trimmed = total_elapsed
        if total_elapsed_trimmed < 1:
            total_elapsed_trimmed = 1

        data_index = self.counter[0]
        self.counter[0] += 1

        self.total_requested[0] += load

        self.load_meter.submit(load=load)

        v1 = self.limiter.submit(load=load)
        if v1.accepted:
            served = load
        else:
            self.logger.info('load of {} was rejected. asked to wait {}s.'.format(load, v1.retry_in))
            served = 0

        wait_ms = gauss(submitter.avg_request_interval_ms, submitter.avg_request_interval_ms / 2.5)
        if submitter.delay_compliance_factor is not None and not v1.accepted and v1.retry_in is not None and v1.retry_in > 0:
            # add portion of the requested delay
            wait_ms_for_compliance = v1.retry_in * 1000 * submitter.delay_compliance_factor
            self.next_load_cap[0] = load
            if wait_ms_for_compliance > wait_ms:
                wait_ms = wait_ms_for_compliance
                self.logger.info('waiting %.0f ms to comply with delay request', wait_ms)

        if wait_ms < self.data_tick_interval_ms:
            self.logger.warning('applied trimming to wait time ( <= 0 )')
            wait_ms = self.data_tick_interval_ms

        self.wait_until[0] = now + timedelta(milliseconds=wait_ms)

        self.total_served[0] += served

        self.x_data.append(total_elapsed)

        self.y_data1a.append(load if v1.accepted else None)
        self.y_data1b.append(load if not v1.accepted else None)

        self.y_data2a.append(self.limiter.window_total)
        self.y_data2b.append(self.load_meter.window_total)

        avg_served = self.total_served[0] / total_elapsed_trimmed
        avg_requested = self.total_requested[0] / total_elapsed_trimmed

        self.y_data3a.append(avg_served)
        self.y_data3b.append(avg_requested)

        if data_index > self.max_points:
            self.x_data = self.x_data[1:]
            self.y_data1a = self.y_data1a[1:]
            self.y_data1b = self.y_data1b[1:]
            self.y_data2a = self.y_data2a[1:]
            self.y_data2b = self.y_data2b[1:]
            self.y_data3a = self.y_data3a[1:]
            self.y_data3b = self.y_data3b[1:]

        if data_index >= self.total_points:
            self.logger.info('reached total points, terminating')
            self.run[0] = False

class ProfileGraphPrinter():
    def __init__(self, profiler: Profiler):
        self.testbed = profiler.testbed
        self.profiler = profiler
        self.limiter = profiler.limiter
        self.submitter = profiler.submitter

        self.animation_refresh_period = 5 * 1000
        self.logger = profiler.logger

        self.terminate = False
        self.terminated = False

    def start(self):

        figure, (ax1, ax2, ax3) = pyplot.subplots(3, figsize=(12, 6), sharex='all')

        figure.suptitle('Requested vs Served load - ' + self.testbed.name)

        ax1.xaxis.grid(True)
        ax2.xaxis.grid(True)
        ax3.xaxis.grid(True)

        ax1.set_xlabel('Time')
        ax1.set_ylabel('Load requests')

        ax2.set_xlabel('Time')
        ax2.set_ylabel('Total load in window')

        ax3.set_xlabel('Time')
        ax3.set_ylabel('Avg load')

        initial_x = []
        initial_y = []
        line1a, = ax1.plot(initial_x, initial_y, 'bo')
        line1b, = ax1.plot(initial_x, initial_y, 'rx')

        line2a, = ax2.plot(initial_x, initial_y, '-')
        line2b, = ax2.plot(initial_x, initial_y, color='orange', linestyle='dashed',)

        line3a, = ax3.plot(initial_x, initial_y, '-')
        line3b, = ax3.plot(initial_x, initial_y, color='orange', linestyle='dashed',)

        ax1.legend([line1a, line1b], ['accepted request', 'rejected request'], loc='upper left')
        ax1.set_ylim(0, self.submitter.avg_load_per_request * 2.50 / 1000)

        ax2.axhline(y = self.limiter.maxload, color = 'r', linestyle = 'dotted')
        ax2.legend([line2b, line2a], ['absolute requested load in window', 'absolute accepted load in window'], loc='upper left')
        ax2_max_y = self.limiter.maxload * (1.33 * self.submitter.load_factor)
        ax2.set_ylim(0, ax2_max_y)
        ax2.annotate("Max load", xy=(1, self.limiter.maxload / ax2_max_y), xycoords='axes fraction')

        self.avg_desired_load = self.limiter.maxload / self.limiter.period

        ax3.axhline(y = self.avg_desired_load, color = 'r', linestyle = 'dotted')
        ax3.legend([line3b, line3a], ['absolute requested load avg', 'absolute accepted load avg'], loc='lower left')
        ax3_max_y = self.avg_desired_load * (1.33 * self.submitter.load_factor)
        ax3.set_ylim(0, ax3_max_y)
        ax3.annotate("Max avg load", xy=(1, self.avg_desired_load / ax3_max_y), xycoords='axes fraction')

        def graph_updater(frame):
            with self.profiler.data_lock:        
                
                line1a.set_data(self.profiler.x_data, self.profiler.y_data1a)
                line1b.set_data(self.profiler.x_data, self.profiler.y_data1b)
                line2a.set_data(self.profiler.x_data, self.profiler.y_data2a)
                line2b.set_data(self.profiler.x_data, self.profiler.y_data2b)
                line3a.set_data(self.profiler.x_data, self.profiler.y_data3a)
                line3b.set_data(self.profiler.x_data, self.profiler.y_data3b)

            ax1.relim()
            ax2.relim()
            ax3.relim()

            ax1.autoscale_view()
            ax2.autoscale_view()
            ax3.autoscale_view()

            return line1a,

        self.animation = FuncAnimation(figure, graph_updater, interval=self.animation_refresh_period)

        pyplot.ion()
        pyplot.show()
        self.logger.info('waiting for profiler to terminate ...')
        while self.profiler.is_running():
            pyplot.pause(0.1)

        self.logger.info('detected profiler termination, terminating plotter')
        self.animation.event_source.stop()

        self.terminated = True

        graph_updater(None)

        figure.savefig('F:/UPAP/profiling-' + self.slugify(self.profiler.name) + '.jpeg', 
            bbox_inches='tight'
        )
        pyplot.close(figure)

        self.logger.info('plotter terminated')

    def slugify(self, value, allow_unicode=False):
        """
        Taken from https://github.com/django/django/blob/master/django/utils/text.py
        Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
        dashes to single dashes. Remove characters that aren't alphanumerics,
        underscores, or hyphens. Convert to lowercase. Also strip leading and
        trailing whitespace, dashes, and underscores.
        """
        value = str(value)
        if allow_unicode:
            value = unicodedata.normalize('NFKC', value)
        else:
            value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
        value = re.sub(r'[^\w\s-]', '', value.lower())
        return re.sub(r'[-\s]+', '-', value).strip('-_')

def do_profile(testbed: ProfilingTestBed):

    profiler = Profiler(testbed)

    graph_printer = ProfileGraphPrinter(profiler)

    profiler.start()

    graph_printer.start()

    profiler.stop()

def build_profile_default_limiter_nopenalty():
    return LoadLimiter(
        maxload=DEFAULT_MAX_LOAD, 
        period=DEFAULT_TIME_PERIOD,
        penalty_factor=0.0,
        request_overhead_penalty_factor=0.0
    )

def build_profile_default_limiter_smallpenalty():
    return LoadLimiter(
        maxload=DEFAULT_MAX_LOAD, 
        period=DEFAULT_TIME_PERIOD,
        penalty_factor=0.05,
        request_overhead_penalty_factor=0.05
    )

def build_profile_composite_limiter_nopenalty():
    return CompositeLoadLimiter(
        limiters = [
            LoadLimiter(
                maxload=100, 
                period=20,
                penalty_factor=0.0,
                request_overhead_penalty_factor=0.0
            ),
            LoadLimiter(
                maxload=20, 
                period=4,
                penalty_factor=0.0,
                request_overhead_penalty_factor=0.0
            ),
        ]
    )

def build_profile_90pc_nopenalty_delaycompliant():
    limiter = build_profile_default_limiter_nopenalty()

    submitter = LoadSubmitter(
        limiter = limiter,
        load_factor = 0.90,
        delay_compliance_factor = 1.20,
        average_request_interval = 300
    )

    return ProfilingTestBed(
        name='90% of load, no penalties, delay compliant',
        limiter=limiter, 
        submitter=submitter
    )

def build_profile_100pc_nopenalty_delaycompliant():
    limiter = build_profile_default_limiter_nopenalty()

    submitter = LoadSubmitter(
        limiter = limiter,
        load_factor = 1.00,
        delay_compliance_factor = 1.20,
        average_request_interval = 300
    )

    return ProfilingTestBed(
        name='100% of load, no penalties, delay compliant',
        limiter=limiter, 
        submitter=submitter
    )

def build_profile_130pc_nopenalty_delaycompliant():
    limiter = build_profile_default_limiter_nopenalty()

    submitter = LoadSubmitter(
        limiter = limiter,
        load_factor = 1.30,
        delay_compliance_factor = 1.20,
        average_request_interval = 300
    )

    return ProfilingTestBed(
        name='130% of load, no penalties, delay compliant',
        limiter=limiter, 
        submitter=submitter
    )

def build_profile_200pc_nopenalty_delayuncompliant():
    limiter = build_profile_default_limiter_nopenalty()

    submitter = LoadSubmitter(
        limiter = limiter,
        load_factor = 2.00,
        delay_compliance_factor = None,
        average_request_interval = 300
    )

    return ProfilingTestBed(
        name='200% of load, no penalties, not delay compliant',
        limiter=limiter, 
        submitter=submitter
    )

def build_profile_130pc_smallpenalty_delaycompliant():
    limiter = build_profile_default_limiter_smallpenalty()

    submitter = LoadSubmitter(
        limiter = limiter,
        load_factor = 1.30,
        delay_compliance_factor = 1.20,
        average_request_interval = 300
    )

    return ProfilingTestBed(
        name='130% of load, small penalties, delay compliant',
        limiter=limiter, 
        submitter=submitter
    )

def build_profile_150pc_composite_delayuncompliant():
    limiter = build_profile_composite_limiter_nopenalty()

    submitter = LoadSubmitter(
        limiter = limiter,
        load_factor = 2.00,
        delay_compliance_factor = None,
        average_request_interval = 100
    )

    return ProfilingTestBed(
        name='150% of load, composite limiter, not delay compliant',
        limiter=limiter, 
        submitter=submitter
    )

if __name__ == '__main__':

    testbeds_all =[
        build_profile_90pc_nopenalty_delaycompliant(),
        build_profile_100pc_nopenalty_delaycompliant(),
        build_profile_130pc_nopenalty_delaycompliant(),
        build_profile_200pc_nopenalty_delayuncompliant(),

        build_profile_130pc_smallpenalty_delaycompliant(),
        build_profile_150pc_composite_delayuncompliant(),
    ] 

    testbeds =[
        build_profile_150pc_composite_delayuncompliant(),
    ] 

    for testbed in testbeds:
        logging.info('running testbed ' + testbed.name)
        do_profile(testbed)