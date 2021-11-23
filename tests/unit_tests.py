import unittest
import datetime
from freezegun import freeze_time

from pyloadlimiter import LoadLimiter, LoadLimitExceeded, InMemoryLoadLimiterStorageAdapter

class TestLoadLimiter(unittest.TestCase):

    def deferred_fn(self, a, named1=None):
        if a == 0:
            raise ValueError('failure: a can\'t be 0')
        return [a, named1]

    def test_bad_params(self):
        for kwargs in [
            { 'maxload': 0 },
            { 'maxload': -2 },
            { 'period': 0 },
            { 'period': -2 },
            { 'fragmentation': 1.01 },
            { 'fragmentation': 0 },
            { 'fragmentation': -0.5 },
            { 'penalty_factor': -0.5 },
            { 'penalty_distribution_factor': -0.5 },
            { 'penalty_distribution_factor': 1.01 },
            { 'request_overhead_penalty_factor': -0.5 },
            { 'request_overhead_penalty_distribution_factor': -0.5 },
            { 'request_overhead_penalty_distribution_factor': 1.01 },
            { 'max_penalty_cap_factor': -0.5 }
        ]:
            with self.assertRaises(ValueError):
                LoadLimiter(**kwargs)

    def test_basic(self):

        start_time = datetime.datetime.now()
        
        with freeze_time(start_time):
            limiter = LoadLimiter(maxload=10, period=2)
            self.assertEqual(limiter.instant_load_factor(), 0.0)
            self.assertTrue(limiter.submit(3).accepted)
            self.assertTrue(limiter.submit(3).accepted)
            self.assertEqual(limiter.instant_load_factor(), 0.6)
            self.assertTrue(limiter.submit(4).accepted)
            self.assertEqual(limiter.instant_load_factor(), 1.0)
            r = limiter.submit(1)
            self.assertGreaterEqual(limiter.instant_load_factor(), 1.0)
            self.assertFalse(r.accepted)
            self.assertTrue(r.retry_in > 0 and r.retry_in <=2)

        with freeze_time(start_time) as frozen_datetime:
            limiter = LoadLimiter(maxload=10, period=1)

            self.assertTrue(limiter.submit(5).accepted)
            self.assertTrue(limiter.submit(5).accepted)
            frozen_datetime.tick(delta=datetime.timedelta(seconds=1))
            self.assertTrue(limiter.submit(1).accepted)

    def test_context_manager(self):
        start_time = datetime.datetime.now()
        
        with freeze_time(start_time):
            submitted = 0
            limiter = LoadLimiter(maxload=10, period=2)
            with limiter.waiting(5):
                submitted += 5
            with limiter.waiting(5):
                submitted += 5
            
            with self.assertRaises(LoadLimitExceeded):
                with limiter.attempting(5):
                    submitted += 5
            
            self.assertEqual(submitted, 10)

        with freeze_time(start_time) as frozen_datetime:
            limiter = LoadLimiter(maxload=10, period=1)
            submitted = 0
            self.assertEqual(limiter.instant_load_factor(), 0.0)
            with limiter.waiting(5):
                submitted += 5
            with limiter.waiting(5):
                submitted += 5
            self.assertEqual(limiter.instant_load_factor(), 1.0)
            frozen_datetime.tick(delta=datetime.timedelta(seconds=1))
            with limiter.waiting(5):
                submitted += 5
            self.assertEqual(submitted, 15)


    def test_as_decorator(self):
        start_time = datetime.datetime.now()
        
        with freeze_time(start_time):
            submitted = [0]
            limiter = LoadLimiter(maxload=10, period=2)
            @limiter(load=5, wait=False)
            def do_expensive():
                submitted[0] += 5

            do_expensive()
            do_expensive()
            
            self.assertEqual(limiter.instant_load_factor(), 1.0)
            with self.assertRaises(LoadLimitExceeded):
                do_expensive()
            
            self.assertEqual(submitted[0], 10)

        with freeze_time(start_time) as frozen_datetime:
            submitted = [0]
            limiter = LoadLimiter(maxload=10, period=2)
            @limiter(load=5, wait=False)
            def do_expensive():
                submitted[0] += 5

            do_expensive()
            do_expensive()
            self.assertEqual(limiter.instant_load_factor(), 1.0)
            frozen_datetime.tick(delta=datetime.timedelta(seconds=2))
            self.assertEqual(limiter.instant_load_factor(), 0.0)
            
            do_expensive()
            self.assertEqual(limiter.instant_load_factor(), 0.5)
            self.assertEqual(submitted[0], 15)

    def test_status_dump(self):
        limiter = LoadLimiter(maxload=10, period=2)
        start_time = datetime.datetime.now()
        
        def assert_serialization_identity():
            dump_before = limiter._dump_status()
            limiter._restore_from_status(dump_before)
            dump_after = limiter._dump_status()
            self.assertDictEqual(dump_before.__dict__, dump_after.__dict__)

        with freeze_time(start_time) as frozen_datetime:
            assert_serialization_identity()

            self.assertEqual(limiter.instant_load_factor(), 0.0)
            self.assertTrue(limiter.submit(1).accepted)
            assert_serialization_identity()
            self.assertTrue(limiter.submit(1).accepted)
            self.assertTrue(limiter.submit(2).accepted)
            assert_serialization_identity()

            frozen_datetime.tick(delta=datetime.timedelta(seconds=2))
            assert_serialization_identity()
            self.assertTrue(limiter.submit(1).accepted)
            assert_serialization_identity()

            frozen_datetime.tick(delta=datetime.timedelta(seconds=2))
            assert_serialization_identity()
            self.assertTrue(limiter.submit(3).accepted)
            assert_serialization_identity()
            self.assertTrue(limiter.submit(1).accepted)
            assert_serialization_identity()

            frozen_datetime.tick(delta=datetime.timedelta(seconds=5))
            assert_serialization_identity()
            self.assertTrue(limiter.submit(3).accepted)
            assert_serialization_identity()
            limiter.submit(3)
            limiter.submit(3)
            limiter.submit(3)
            assert_serialization_identity()
            limiter.submit(3)
            limiter.submit(3)
            self.assertFalse(limiter.submit(4).accepted)
            assert_serialization_identity()

            frozen_datetime.tick(delta=datetime.timedelta(seconds=8))
            assert_serialization_identity()
            self.assertTrue(limiter.submit(1).accepted)
            assert_serialization_identity()

    def test_storage_adapter(self):
        start_time = datetime.datetime.now()

        limiter = LoadLimiter(maxload=10, period=2, storage_adapter=InMemoryLoadLimiterStorageAdapter())

        def assert_serialization_identity():
            dump_before = limiter._dump_status()
            self.assertTrue(limiter.flush(force=True))

            self.assertTrue(limiter.restore())
            dump_after = limiter._dump_status()

            self.assertDictEqual(dump_before.__dict__, dump_after.__dict__)

        with freeze_time(start_time) as frozen_datetime:
            assert_serialization_identity()

            self.assertEqual(limiter.instant_load_factor(), 0.0)
            self.assertTrue(limiter.submit(1).accepted)
            assert_serialization_identity()
            self.assertTrue(limiter.submit(1).accepted)
            self.assertTrue(limiter.submit(2).accepted)
            assert_serialization_identity()

            frozen_datetime.tick(delta=datetime.timedelta(seconds=2))
            assert_serialization_identity()
            self.assertTrue(limiter.submit(1).accepted)
            assert_serialization_identity()

            frozen_datetime.tick(delta=datetime.timedelta(seconds=2))
            assert_serialization_identity()
            self.assertTrue(limiter.submit(3).accepted)
            assert_serialization_identity()
            self.assertTrue(limiter.submit(1).accepted)
            assert_serialization_identity()

            frozen_datetime.tick(delta=datetime.timedelta(seconds=5))
            assert_serialization_identity()
            self.assertTrue(limiter.submit(3).accepted)
            assert_serialization_identity()
            limiter.submit(3)
            limiter.submit(3)
            limiter.submit(3)
            assert_serialization_identity()
            limiter.submit(3)
            limiter.submit(3)
            self.assertFalse(limiter.submit(4).accepted)
            assert_serialization_identity()

            frozen_datetime.tick(delta=datetime.timedelta(seconds=8))
            assert_serialization_identity()
            self.assertTrue(limiter.submit(1).accepted)
            assert_serialization_identity()

if __name__ == '__main__':
    unittest.main()