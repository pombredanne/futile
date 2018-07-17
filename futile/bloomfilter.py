class BloomFilter:

    def put(self, )

class BloomFilter2(object):
    def __init__(self, bit_num=None, hash_num=None, item_num=None, bits=None):
        self.bit_num = bit_num
        self.hash_num = hash_num
        self.item_num = item_num if item_num else 0
        self._bits = BitVector(size=bit_num, bits=bits)

    def gen_offsets(self, key: Union[str, bytes]):
        key = ensure_bytes(key)
        h = md5.new()
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        else:
            key = str(key)
        h.update(key)
        a, b = struct.unpack('QQ', h.digest())
        for i in range(self.hash_num):
            yield (a + i * b) % self.bit_num

    def add(self, key):
        ''' Adds a key to this bloom filter. If the key already exists in this
            filter it will return True. Otherwise False. '''
        dup = True
        for i in self.gen_offsets(key): 
            if dup and not self._bits.has_bit(i):
                dup = False
            self._bits.set_bit(i)
        if not dup:
            self.item_num += 1
        return dup

    def might_contain(self, key):
        for i in self.gen_offsets(key):
            if not self._bits.has_bit(i):
                return False
        return True

    def __len__(self):
        return self.item_num

    def bits(self):
        return self._bits.to_binary()

    def bits_size(self):
        return len(self._bits.to_binary())

