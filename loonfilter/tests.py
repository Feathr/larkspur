import redis
from bson import ObjectId
from loon import BloomFilter

if __name__ == '__main__':
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    bf = BloomFilter(r, 'test:objectids', capacity=100000)
    objectids = [str(ObjectId()) for i in range(0, 10000)]
    bf.bulk_add(objectids)
    print(all([objectid in bf for objectid in objectids]))
    print(str(ObjectId()) in bf)
