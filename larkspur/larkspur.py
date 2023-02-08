import math
import hashlib
from struct import pack, unpack
from typing import Union


def deserialize_hm(hm):
    # kinda like a schema
    out = {}
    for key, value in hm.items():
        decoded_key = key.decode()
        if decoded_key in ['error_rate', 'ratio']:
            parsed = float(value.decode())
        else:
            parsed = int(value.decode())
        out[key.decode()] = parsed
    return out


def make_hashes(num_slices, num_bits):
    # we're going to hash the input by putting into a cryptographic hash function.
    # From that hash, we need to be able to derive integer indexes in a redis bitfield
    # of a known size. That means we need to carefully choose the hash function so that
    # it's digest size is appropriate to the size of the bitfield given a packed
    # representation of its output.

    # we need to choose the right function so we get enough bits to get enough
    # indices for the number of slices
    # choose packing format based on the size of the bitfield slices
    # see: https://docs.python.org/3/library/struct.html#format-characters

    # Based on the size of the bitfield slice we need to index into, we need to
    # choose a representation of each index that is big enough to actually represent
    # any index in the slice.
    if num_bits >= (1 << 31):
        format_code = 'Q'  # unsigned long (8 bytes)
        chunk_size = 8
    elif num_bits >= (1 << 15):
        format_code = 'I'  # unsigned int (4 bytes)
        chunk_size = 4
    else:
        format_code = 'H'  # unsigned short (2 bytes)
        chunk_size = 2

    # Choose hash algorithm that produces a digest big enough
    # to represent an index for each packed chunk.
    total_hash_bits = 8 * num_slices * chunk_size
    if total_hash_bits > 384:
        hashfn = hashlib.sha512
    elif total_hash_bits > 256:
        hashfn = hashlib.sha384
    elif total_hash_bits > 160:
        hashfn = hashlib.sha256
    elif total_hash_bits > 128:
        hashfn = hashlib.sha1
    else:
        hashfn = hashlib.md5

    # format for how the bitfield indices will be unpacked from
    # the hash. example:
    # 'IIIIIIII' (8 unsigned ints (4 bytes each))
    pack_format = format_code * (hashfn().digest_size // chunk_size)

    # if the number of slices does not go into the pack format evenly
    # add a salt to make any indices for additional slices
    num_salts, extra = divmod(num_slices, len(pack_format))
    if extra:
        num_salts += 1

    # Make a uniquely but deterministically salted hash function for each slice
    # Each hash function uses its index as an initial string. This way they don't
    # all produce the same hash for the same input and also can be reproduced
    # each time the BloomFilter object is instantiated.
    salts = tuple(
        hashfn(
            hashfn(pack('I', i)).digest()
        ) for i in range(0, num_salts)
    )

    def hasher(key):
        if isinstance(key, str):
            key = key.encode('utf8')
        else:
            key = str(key).encode('utf8')
        i = 0
        # This is the core of how the Bloom filter works. We hash the input key using each of
        # the presalted hash functions. Using the pack format, we unpack the hash into integers
        # in the range of the size of our bitfield in redis. We will use the integers and index
        # locations in that bitfield, setting the bits at each of those locations to 1 to indicate
        # that some input key hashed to that bit in the past.
        for salt in salts:
            h = salt.copy()
            h.update(key)
            # yield an index for each slice by unpacking the indices
            # from the hash digest
            for index in unpack(pack_format, h.digest()):
                yield index % num_bits
                i += 1
                if i >= num_slices:
                    return

    return hasher, hashfn


class BloomFilter:

    def __init__(self, connection, name, capacity, error_rate=0.001):
        if not (0 < error_rate < 1):
            raise ValueError(
                'error_rate must be float value between 0 and 1, exclusive.'
            )
        if not capacity > 0:
            raise ValueError('capacity must be greater than 0.')

        num_slices = int(math.ceil(math.log(1.0 / error_rate, 2)))
        bits_per_slice = int(
            math.ceil(
                (capacity * abs(math.log(error_rate))) / (num_slices * (math.log(2) ** 2))
            )
        )
        self.connection = connection
        self.name = name
        self.meta_name = f'bfmeta:{name}'
        meta = deserialize_hm(self.connection.hgetall(self.meta_name)) or {}
        self.error_rate = meta.get('error_rate') or error_rate
        self.num_slices = meta.get('num_slices') or num_slices
        self.bits_per_slice = meta.get('bits_per_slice') or bits_per_slice
        self.capacity = meta.get('capacity') or capacity
        self.num_bits = meta.get('num_bits') or num_slices * bits_per_slice
        self.count = meta.get('count') or 0
        if self.num_bits > 1 << 32:
            raise ValueError('capacity too large or error rate too low to store in redis')
        if not self.connection.exists(self.meta_name):
            self._create_meta()
        self.hasher, hashfn = make_hashes(self.num_slices, self.bits_per_slice)

    def _create_meta(self):
        self.connection.hmset(self.meta_name, {
            'error_rate': self.error_rate,
            'num_slices': self.num_slices,
            'bits_per_slice': self.bits_per_slice,
            'capacity': self.capacity,
            'num_bits': self.num_bits,
            'count': self.count
        })

    def __contains__(self, key):
        indexes = self.hasher(key)
        offset = 0

        pipe = self.connection.pipeline()
        for index in indexes:
            pipe.getbit(self.name, offset + index)
            offset += self.bits_per_slice
        res = pipe.execute()
        return all(res)

    def add(self, key, skip_check=False):
        if self.count > self.capacity:
            raise IndexError('BloomFilter is at capacity')
        indexes = self.hasher(key)
        offset = 0

        pipe = self.connection.pipeline()
        for index in indexes:
            pipe.setbit(self.name, offset + index, 1)
            offset += self.bits_per_slice
        res = pipe.execute()

        already_present = all(res)
        if not already_present:
            self.count = self.connection.hincrby(self.meta_name, 'count', 1)
        return already_present

    def bulk_add(self, keys):
        if self.count > self.capacity:
            raise IndexError('BloomFilter is at capacity')
        pipe = self.connection.pipeline()
        for key in keys:
            offset = 0
            indexes = self.hasher(key)
            for index in indexes:
                pipe.setbit(self.name, offset + index, 1)
                offset += self.bits_per_slice
        res = pipe.execute()
        buf = []
        bulk_increment = 0
        for val in res:
            buf.append(val)
            if len(buf) == self.num_slices:
                if not all(buf):
                    bulk_increment += 1
                buf = []
        self.count = self.connection.hincrby(self.meta_name, 'count', bulk_increment)

    def flush(self, pipe=None):
        execute = False
        if not pipe:
            pipe = self.connection.pipeline()
            execute = True
        pipe.delete(self.name)
        pipe.delete(self.meta_name)

        if execute:
            pipe.execute()
        self.count = 0

    def expire(self, time, pipe=None):
        execute = False
        if not pipe:
            pipe = self.connection.pipeline()
            execute = True

        pipe.expire(self.name, time)
        pipe.expire(self.meta_name, time)

        if execute:
            pipe.execute()


class ScalableBloomFilter:
    SMALL_SET_GROWTH = 2
    LARGE_SET_GROWTH = 4

    def __init__(
        self,
        connection,
        name,
        initial_capacity=1000,
        error_rate=0.001,
        scale=LARGE_SET_GROWTH,
        ratio=0.9
    ):
        self.name = name
        self.meta_name = f'sbfmeta:{name}'
        self.connection = connection
        meta = deserialize_hm(self.connection.hgetall(self.meta_name))
        self.error_rate = meta.get('error_rate') or error_rate
        self.scale = meta.get('scale') or scale
        self.ratio = meta.get('ratio') or ratio
        self.initial_capacity = meta.get('initial_capacity') or initial_capacity
        if not self.connection.exists(self.meta_name):
            self._create_meta()
        filter_names = sorted(list(self.connection.smembers(self.name)))
        self.filters = [
            BloomFilter(connection, fn.decode('utf8'), self.initial_capacity)
            for fn in filter_names
        ]

    def _create_meta(self):
        self.connection.hmset(self.meta_name, {
            'error_rate': self.error_rate,
            'scale': self.scale,
            'ratio': self.ratio,
            'initial_capacity': self.initial_capacity,
        })

    def _get_next_filter(self):
        if not self.filters:
            bf_name = f'{self.name}:bf0'
            bf = BloomFilter(
                self.connection,
                bf_name,
                capacity=self.initial_capacity,
                error_rate=self.error_rate
            )
            self.filters.append(bf)
            self.connection.sadd(self.name, bf.name)
        else:
            bf = self.filters[-1]
            if bf.count >= bf.capacity:
                bf_name = f'{self.name}:bf{len(self.filters)}'
                bf = BloomFilter(
                    self.connection,
                    bf_name,
                    capacity=min(bf.capacity * self.scale, 1000000000),
                    error_rate=max(bf.error_rate * self.ratio, 0.000001),
                )
                self.filters.append(bf)
                self.connection.sadd(self.name, bf.name)
        return bf

    def __contains__(self, key):
        for f in reversed(self.filters):
            if key in f:
                return True
        return False

    def add(self, key: Union[bytes, str]) -> bool:
        """Adds the item key into the bloom filter.

        Args:
            key: The item to be added.

        Returns:
            True if key exists and False if it not and it was added
        """
        # Check to see if any filters already contain the key.
        # This check is necessary because self._get_next_filter will return the latest 
        # BloomFilter which may not contain this key, but the key may still exist
        # in an earlier BloomFilter if initial_capacity was exceeded.
        if self.__contains__(key):
            return True

        bf = self._get_next_filter()
        return bf.add(key)

    def bulk_add(self, keys):
        index = 0
        while index < len(keys):
            bf = self._get_next_filter()
            chunk_size = min(bf.capacity - bf.count, len(keys))
            chunk = keys[index:index + chunk_size]
            bf.bulk_add(chunk)
            index += chunk_size

    def flush(self):
        pipe = self.connection.pipeline()
        pipe.delete(self.name)
        pipe.delete(self.meta_name)
        for bf in self.filters:
            bf.flush(pipe)
        pipe.execute()
        self.filters = []
        self._create_meta()

    def expire(self, time):
        pipe = self.connection.pipeline()
        pipe.expire(self.name, time)
        pipe.expire(self.meta_name, time)
        for bf in self.filters:
            bf.expire(time, pipe)
        pipe.execute()

    @property
    def capacity(self):
        return sum([bf.capacity for bf in self.filters])

    @property
    def count(self):
        return sum([bf.count for bf in self.filters])
