import math
from bitarray import bitarray
from pymmh3 import hash
from src.utils.config import debug

class BloomFilter(object):
      def __init__(self, items_count):
          self.items_count = items_count
          self.false_positive_probability = 0.02 # The bigger this number, the bigger the bloom filter size
          self.hash_count = 3
          self.size = self.get_size()
          self.bit_array = bitarray(self.size)
          self.bit_array.setall(0)
          print("DEBUG: Bloom filter size: ", self.size) if debug else None
          
      def add(self, item):
          for i in range(self.hash_count):
              digest = hash(item, i) % self.size # in order to get a single integer from the hash
              self.bit_array[digest] = True
              
      def get_size(self):
          m = -(self.items_count * math.log(self.false_positive_probability))/(math.log(2)**2) # derived from the theoretical maximum false positive rate for a Bloom filter
          return int(m)

      def contains(self, item):
          for i in range(self.hash_count):
              digest = hash(item, i) % self.size
              if self.bit_array[digest] == False:
                  return False
          return True
