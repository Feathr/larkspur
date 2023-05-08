import math
import hashlib
from struct import pack, unpack
from typing import List, Optional, Union
from redis.client import Pipeline, Redis


KeyType = Union[bytes, str, int, float]


def deserialize_hm(hm) -> dict:
    # kinda like a schema
    out = {}
    for key, value in hm.items():
        decoded_key = key.decode()
        if decoded_key in ['error_rate', 'ratio', 'threshold_scale']:
            parsed = float(value.decode())
        else:
            parsed = int(value.decode())
        out[key.decode()] = parsed
    return out


def make_hashes(num_slices: int, num_bits: int):
    """Makes hashes. Determine the hash function for each slice.

    Args:
        num_slices: number of slices
        num_bits: number of bits for each slice
    """
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

    def hasher(key: KeyType):
        """Hashes the input item key.
        """
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

    def __init__(self, connection: Redis, name: str, capacity: int, error_rate: Optional[float] = 0.001) -> None:
        """
        Initialize a new BloomFilter.

        Parameters
        ----------
        connection : Redis client connection
            The Redis client connection.
        name : str
            A bloom filter name.
        capacity : int
            A initial capacity of the bloom filter. Capacity must be greater than 0.
        error_rate : float, optional
            A initial error_rate of the bloom filter. Error_rate must be between 0 and 1. Default is 0.001.
        """
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

    def __contains__(self, key: KeyType) -> bool:
        """Checks if bloom filter contains an item by checking if all bits of the hashed item are 1.
        Contains false positive results.

        Args:
            key: the item needed to be checked

        Returns:
            True if bloom filter contains the item. Contains false positive results.
            False if bloom filter does not contain the item.
        """
        indexes = self.hasher(key)
        offset = 0

        pipe = self.connection.pipeline()
        for index in indexes:
            pipe.getbit(self.name, offset + index)
            offset += self.bits_per_slice
        res = pipe.execute()
        return all(res)

    def add(self, key: KeyType):
        """Adds an item key into bloom filter.
        Raises IndexError if the current count is larger than its capacity.
        Hashes the item key and adds into the redis bitfield.
        Increments count by 1 if any of the bit stored was 0 and returns false. Otherwise returns true.

        Args:
            key: An item need to be added into bloom filter.

        Returns:
            A boolean represents if count was incremented.
            Increments count by 1 if any of the bit stored was 0 and returns false. Otherwise returns true.
        """
        if self.count > self.capacity:
            raise IndexError(f'BloomFilter is at capacity. Count: {self.count}. Capacity: {self.capacity} ')
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

    def bulk_add(self, keys: List[KeyType]):
        """Adds items in chunk into bloom filter.
        Raises IndexError if the current count is larger than its capacity.
        For each item, hashes the item key and adds into redis bitfield.
        Increment count by 1 if not all the bits were set to 1 by other item.

        Args:
            keys: An list of items
        """
        if self.count > self.capacity:
            raise IndexError(f'BloomFilter is at capacity. Count: {self.count}. Capacity: {self.capacity}. ')
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

    def flush(self, pipe: Optional[Pipeline]) -> None:
        """Deletes one or more keys specified by names.
        Sets count as 0.
        """
        execute = False
        if not pipe:
            pipe = self.connection.pipeline()
            execute = True
        pipe.delete(self.name)
        pipe.delete(self.meta_name)

        if execute:
            pipe.execute()
        self.count = 0

    def expire(self, time: int, pipe: Optional[Pipeline]) -> None:
        """Sets an expire flag on key name for time seconds.
        """
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
        connection: Redis,
        name: str,
        initial_capacity: Optional[int] = 1000,
        error_rate: Optional[float] = 0.001,
        scale: Optional[int] = LARGE_SET_GROWTH,
        ratio: Optional[float] = 0.9,
        threshold_scale: Optional[float] = 0.9
    ):
        """
        Initialize a new ScalableBloomFilter.

        Parameters
        ----------
        connection : Redis Connection
            The Redis Connection.
        name : str
            A bloom filter name.
        initial_capacity : int, optional
            A initial capacity of the bloom filter, default is 1000.
        error_rate : float, optional
            A initial error_rate of the bloom filter, default is 0.001.
        scale: int, optional
            A scale factor for scaling up capacity. default is 4.
        ratio: float, optional
            A scale factor for scaling up error_rate. default is 0.9.
        threshold_scale: float, optional
            A threshold factor for capacity. Capacity multiplies with threshold_scale 
            to set count threshold for scaling up. Default is 0.9.  

        """
        self.name = name
        self.meta_name = f'sbfmeta:{name}'
        self.connection = connection
        meta = deserialize_hm(self.connection.hgetall(self.meta_name))
        self.error_rate = meta.get('error_rate') or error_rate
        self.scale = meta.get('scale') or scale
        self.ratio = meta.get('ratio') or ratio
        self.threshold_scale = meta.get('threshold_scale') or threshold_scale
        self.initial_capacity = meta.get('initial_capacity') or initial_capacity
        if not self.connection.exists(self.meta_name):
            self._create_meta()
        filter_names = list(self.connection.smembers(self.name))
        # Sort the bloomfilters according to their bf# as they are in format NAME:bf0
        filter_names.sort(
            key=lambda bf_name: int(bf_name.decode("utf-8").split(":")[-1][2:])
        )
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
            'threshold_scale': self.threshold_scale
        })

    def _get_next_filter(self) -> BloomFilter:
        """Creates a new bloom filter with scale factors. Scales capacity and error_rate.
        Appends to filters and returns the newly created bloom filter.
        Capacity scales up with scale, with a maximum of 1000000000.
        Error_rate scales up with ratio, with a minimum of 0.000001.

        Returns:
            The newly created BloomFilter object.
        """
        if not self.filters:
            # initialize a new BloomFilter with the initial capacity and error_rate
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
            # check count against the capacity threshold to reduce race conditions
            if bf.count >= bf.capacity * self.threshold_scale:
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

    def __contains__(self, key: KeyType) -> bool:
        """Checks if any bloom filter contains the item key.
        False positive is possible.

        Args:
            key: the item needed to be checked

        Returns:
            True if any bloom filter contains the item. Otherwise returns false.
        """
        for f in reversed(self.filters):
            if key in f:
                return True
        return False

    def add(self, key: KeyType) -> bool:
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
        if key in self:
            return True

        bf = self._get_next_filter()
        return bf.add(key)

    def bulk_add(self, keys: List[KeyType]):
        """Checks current count against capacist, then scales up when needed. 
        Adds a chunk of items into bloom filter.

        Args:
            keys: a list of items.
        """
        index = 0
        while index < len(keys):
            # Race conditions may occur
            bf = self._get_next_filter()
            chunk_size = min(bf.capacity - bf.count, len(keys))
            chunk = keys[index:index + chunk_size]
            bf.bulk_add(chunk)
            index += chunk_size

    def flush(self) -> None:
        """Deletes one or more keys specified by names.
        Deletes all filters.
        """
        pipe = self.connection.pipeline()
        pipe.delete(self.name)
        pipe.delete(self.meta_name)
        for bf in self.filters:
            bf.flush(pipe)
        pipe.execute()
        self.filters = []
        self._create_meta()

    def expire(self, time: int) -> None:
        """Sets an expire flag on key name for time seconds.
        """
        pipe = self.connection.pipeline()
        pipe.expire(self.name, time)
        pipe.expire(self.meta_name, time)
        for bf in self.filters:
            bf.expire(time, pipe)
        pipe.execute()

    @property
    def capacity(self) -> int:
        return sum([bf.capacity for bf in self.filters])

    @property
    def count(self) -> int:
        return sum([bf.count for bf in self.filters])
