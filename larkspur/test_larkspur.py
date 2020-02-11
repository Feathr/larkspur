import unittest
import redis
from bson import ObjectId
from .larkspur import BloomFilter, ScalableBloomFilter


class LarkspurTestCase(unittest.TestCase):

    def setUp(self):
        self.r = redis.StrictRedis(host='redis', db=3)
        self.r.flushdb()


class TestBloomFilter(LarkspurTestCase):

    def test_add(self):
        bf = BloomFilter(self.r, 'test', capacity=1000, error_rate=0.001)
        members = [str(ObjectId()) for x in range(1000)]
        nonmembers = [str(ObjectId()) for x in range(10)]
        for oid in members:
            bf.add(oid)
        margin = 0.002 * 1000
        assert(all([oid in bf for oid in members]))
        assert(len([
            result for result in
            [oid in bf for oid in nonmembers]
            if result]) <= margin)

        def add_too_many():
            # we have to add a couple too many because the count updates
            # with the same false positive rate
            bf.add(nonmembers[0])
            bf.add(nonmembers[1])
            bf.add(nonmembers[2])
            bf.add(nonmembers[3])
        self.assertRaises(IndexError, add_too_many)

    def test_bulk_add(self):
        bf = BloomFilter(self.r, 'test', capacity=1000, error_rate=0.001)
        members = [str(ObjectId()) for x in range(1000)]
        nonmembers = [str(ObjectId()) for x in range(10)]
        bf.bulk_add(members)
        margin = 0.002 * 1000
        assert(all([oid in bf for oid in members]))
        assert(len([
            result for result in
            [oid in bf for oid in nonmembers]
            if result]) <= margin)

    def test_flush(self):
        bf = BloomFilter(self.r, 'test', capacity=1000, error_rate=0.001)
        members = [str(ObjectId()) for x in range(1000)]
        bf.bulk_add(members)
        assert bf.count >= 990
        bf.flush()
        assert bf.count == 0
        assert(all([oid not in bf for oid in members]))


class TestScalableBloomFilter(LarkspurTestCase):
    def test_add(self):
        sbf = ScalableBloomFilter(self.r, 'test', initial_capacity=1000, error_rate=0.001)
        members = [str(ObjectId()) for x in range(6000)]
        nonmembers = [str(ObjectId()) for x in range(10)]
        for oid in members:
            sbf.add(oid)
        margin = 0.002 * 6000
        assert(all([oid in sbf for oid in members]))
        assert(len([
            result for result in
            [oid in sbf for oid in nonmembers]
            if result]) <= margin)

    def test_bulk_add(self):
        sbf = ScalableBloomFilter(self.r, 'test', initial_capacity=1000, error_rate=0.001)
        members = [str(ObjectId()) for x in range(6000)]
        nonmembers = [str(ObjectId()) for x in range(10)]
        sbf.bulk_add(members)
        margin = 0.002 * 6000
        assert(all([oid in sbf for oid in members]))
        assert(len([
            result for result in
            [oid in sbf for oid in nonmembers]
            if result]) <= margin)

    def test_count(self):
        sbf = ScalableBloomFilter(self.r, 'test', initial_capacity=1000, error_rate=0.001)
        members = [str(ObjectId()) for x in range(10000)]
        for oid in members:
            sbf.add(oid)
        assert sbf.count >= 9980

    def test_clear(self):
        sbf = ScalableBloomFilter(self.r, 'test', initial_capacity=1000, error_rate=0.001)
        members = [str(ObjectId()) for x in range(6000)]
        sbf.bulk_add(members)
        assert sbf.count >= 5988
        sbf.flush()
        assert sbf.count == 0
        assert(all([oid not in sbf for oid in members]))

    def test_existing_filter(self):
        sbf = ScalableBloomFilter(self.r, 'test', initial_capacity=1000, error_rate=0.001)
        members = [str(ObjectId()) for x in range(3000)]
        sbf.bulk_add(members)
        sbf2 = ScalableBloomFilter(self.r, 'test')
        assert sbf2.count == sbf.count
        assert sbf2.error_rate == sbf.error_rate
        assert sbf2.capacity == sbf.capacity
