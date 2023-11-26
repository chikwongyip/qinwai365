# coding: utf-8
from pandas import Series

# obj = Series([4, 5, 6, 78])
# print(obj.index)

# obj = Series([1, 23, 4, 90, "aa"], index=['a', 'b', 'v', 'c', 'd'])
# print(obj)

sdata = {'happy': 20, 'bear': 10}
obj = Series(sdata)
print(obj)
