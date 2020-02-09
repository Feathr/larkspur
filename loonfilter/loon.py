import math
import hashlib
from struct import pack, unpack


def make_hashes(num_slices, num_bits):
    # choose packing format based on the size of the bitfield
    if num_bits >= (1 << 31):
        format_code = 'Q'
        chunk_size = 8
    elif num_bits >= (1 << 15):
        format_code = 'I'
        chunk_size = 4
    else:
        format_code = 'H'
        chunk_size = 2

    # choose hash algorithm based on the number of slices and the
    # packing format
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

    # format for how the bitfield indices will be packed into
    # the hash. example:
    # 'IIIIIIII' (8 unsigned ints (4 bits each))
    pack_format = format_code * (hashfn().digest_size // chunk_size)

    # if the number of slices does not go into the pack format evenly
    # add a salt to make any indices for additional slices
    num_salts, extra = divmod(num_slices, len(pack_format))
    if extra:
        num_salts += 1

    # make a hash for each salt (often there will be just one salt)
    # seed each hash with a hash derived from the salt
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
                (capacity * abs(math.log(error_rate))) /
                (num_slices * (math.log(2) ** 2))
            )
        )
        self.connection = connection
        self.name = name
        self.meta_name = f'bfmeta:{name}'
        meta = self.connection.hgetall(self.meta_name)
        self.error_rate = float(meta.get(b'error_rate').decode()) or error_rate
        self.num_slices = int(meta.get(b'num_slices').decode()) or num_slices
        self.bits_per_slice = int(meta.get(b'bits_per_slice').decode()) or bits_per_slice
        self.capacity = int(meta.get(b'capacity').decode()) or capacity
        self.num_bits = int(meta.get(b'num_bits').decode()) or num_slices * bits_per_slice
        self.count = int(meta.get(b'count').decode()) or 0
        if not self.connection.exists(self.meta_name):
            self.connection.hmset(self.meta_name, {
                b'error_rate': self.error_rate,
                b'num_slices': self.num_slices,
                b'bits_per_slice': self.bits_per_slice,
                b'capacity': self.capacity,
                b'num_bits': self.num_bits,
                b'count': self.count
            })
        self.hasher, hashfn = make_hashes(self.num_slices, self.bits_per_slice)

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
        indexes = self.hasher(key)
        if self.count > self.capacity:
            raise IndexError('BloomFilter is at capacity')
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
        if self.count != 0:
            raise IndexError('BloomFilter is not empty.')
        pipe = self.connection.pipeline()
        for key in keys:
            offset = 0
            indexes = self.hasher(key)
            for index in indexes:
                pipe.setbit(self.name, offset + index, 1)
        self.count = pipe.hset(self.meta_name, 'count', len(set(keys)))
        pipe.execute()
