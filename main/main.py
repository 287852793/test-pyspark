import os

from pyspark import SparkConf
from pyspark.context import SparkContext

if __name__ == '__main__':
    os.environ['JAVA_HOME'] = 'E:/tools/Java/jdk8'
    os.environ['HADOOP_HOME'] = 'E:/tools/hadoop-3.1.0'
    os.environ['PYSPARK_PYTHON'] = 'E:/tools/anaconda3/envs/pyspark/python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'E:/tools/anaconda3/envs/pyspark/python.exe'

    conf = SparkConf().setMaster("local[*]").setAppName("pangyafei's job")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # ======转换算子测试======

    # 文件读取单词统计
    # fileRdd = sc.textFile("../data/words.txt")
    # rsRdd = fileRdd.filter(lambda x: len(x) > 0).flatMap(lambda line: line.strip().split(" ")).map(
    #     lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    # rsRdd.take(3)
    # rsRdd.saveAsTextFile("../data/result/wordcount")

    # map 算子测试
    # list1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    # rdd = sc.parallelize(list1, 4)
    # rs = rdd.map(lambda x: x ** 3)
    # rs.foreach(lambda x: print(x))
    '''
    1
    8
    343
    512
    729
    1000
    27
    64
    125
    216
    '''

    # flatmap 算子测试
    # list1 = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    # print(list1)
    # rdd = sc.parallelize(list1).flatMap(lambda x: x)
    # rdd.foreach(lambda x: print(x))
    '''
    [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    '''

    # filter 算子测试
    # list1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    # rdd = sc.parallelize(list1, 3)
    # rs = rdd.filter(lambda x: x % 3 == 0)
    # rs.foreach(lambda x: print(x))
    '''
    6
    3
    9
    '''

    # union 算子测试
    # list1 = [1, 2, 3]
    # list2 = [11, 12, 13]
    # rdd1 = sc.parallelize(list1)
    # rdd2 = sc.parallelize(list2)
    # rdd = rdd1.union(rdd2)
    # rdd.foreach(lambda x: print(x))
    '''
    2
    1
    3
    11
    12
    13
    '''

    # join 算子测试，必须是 k,v 类型的数据
    # list1 = [('周杰伦', 43), ('蔡依林', 41), ('陈奕迅', 47), ('刘亦菲', 37)]
    # list2 = [('周杰伦', '七里香'), ('蔡依林', '日不落'), ('陈奕迅', '浮夸'), ('王蓉', '小鸡小鸡')]
    # rdd1 = sc.parallelize(list1)
    # rdd2 = sc.parallelize(list2)
    # rdd = rdd1.join(rdd2)
    # rdd.foreach(lambda x: print(x))
    '''
    ('周杰伦', (43, '七里香'))
    ('蔡依林', (41, '日不落'))
    ('陈奕迅', (47, '浮夸'))
    '''

    # fullOuterJoin 算子测试，必须是 k,v 类型的数据
    # list1 = [('周杰伦', 43), ('蔡依林', 41), ('陈奕迅', 47), ('刘亦菲', 37)]
    # list2 = [('周杰伦', '七里香'), ('蔡依林', '日不落'), ('陈奕迅', '浮夸'), ('王蓉', '小鸡小鸡')]
    # rdd1 = sc.parallelize(list1)
    # rdd2 = sc.parallelize(list2)
    # rdd = rdd1.fullOuterJoin(rdd2)
    # rdd.foreach(lambda x: print(x))
    '''
    ('蔡依林', (41, '日不落'))
    ('刘亦菲', (37, None))
    ('王蓉', (None, '小鸡小鸡'))
    ('周杰伦', (43, '七里香'))
    ('陈奕迅', (47, '浮夸'))
    '''

    # leftOuterJoin 算子测试，必须是 k,v 类型的数据
    # list1 = [('周杰伦', 43), ('蔡依林', 41), ('陈奕迅', 47), ('刘亦菲', 37)]
    # list2 = [('周杰伦', '七里香'), ('蔡依林', '日不落'), ('陈奕迅', '浮夸'), ('王蓉', '小鸡小鸡')]
    # rdd1 = sc.parallelize(list1)
    # rdd2 = sc.parallelize(list2)
    # rdd = rdd1.leftOuterJoin(rdd2)
    # rdd.foreach(lambda x: print(x))
    '''
    ('蔡依林', (41, '日不落'))
    ('陈奕迅', (47, '浮夸'))
    ('周杰伦', (43, '七里香'))
    ('刘亦菲', (37, None))
    '''

    # rightOuterJoin 算子测试，必须是 k,v 类型的数据
    list1 = [('周杰伦', 43), ('蔡依林', 41), ('陈奕迅', 47), ('刘亦菲', 37)]
    list2 = [('周杰伦', '七里香'), ('蔡依林', '日不落'), ('陈奕迅', '浮夸'), ('王蓉', '小鸡小鸡')]
    rdd1 = sc.parallelize(list1)
    rdd2 = sc.parallelize(list2)
    rdd = rdd1.rightOuterJoin(rdd2)
    rdd.foreach(lambda x: print(x))
    '''
    ('蔡依林', (41, '日不落'))
    ('周杰伦', (43, '七里香'))
    ('陈奕迅', (47, '浮夸'))
    ('王蓉', (None, '小鸡小鸡'))
    '''

    # distinct 算子测试
    # list1 = [1, 2, 3, 4, 1, 2, 3, 5]
    # rdd = sc.parallelize(list1)
    # print(rdd.count())
    # print(rdd.distinct().count())
    '''
    8
    5
    '''

    # groupByKey 算子测试 (KV类型RDD）
    # list1 = [("蛋糕", 3), ("巧克力", 14), ("糖果", 9), ("蛋糕", 8)]
    # rdd = sc.parallelize(list1)
    # rs = rdd.groupByKey()
    # def printkv(item):
    #     k, v = item
    #     print(f"key: {k}, value: ", *v)
    # rs.foreach(lambda x: printkv(x))
    '''
    key: 蛋糕, value:  3 8
    key: 糖果, value:  9
    key: 巧克力, value:  14
    '''

    # reduceByKey 算子测试 （尽量使用 reduceByKey 代替 groupByKey & map）
    # list1 = [("蛋糕", 3), ("巧克力", 14), ("糖果", 9), ("蛋糕", 8)]
    # rdd = sc.parallelize(list1)
    # rs = rdd.reduceByKey(lambda a, b: a + b)
    # rs.foreach(lambda x: print(x))
    '''
    ('蛋糕', 11)
    ('糖果', 9)
    ('巧克力', 14)
    '''

    # sortBy 算子测试 （全局排序）
    # list1 = [11, 12, 13, 34, 35, 26, 67, 48, 9, 10]
    # rdd = sc.parallelize(list1, 3)
    # rs = rdd.sortBy(lambda x: x, ascending=True)
    # rs.foreach(lambda x: print(x))  # 这里打印无序是因为存在分区
    # print(rs.collect())  # 这里肯定是有序
    '''
    35
    48
    67
    13
    26
    34
    9
    10
    11
    12
    [9, 10, 11, 12, 13, 26, 34, 35, 48, 67]
    '''

    # sortByKey 算子测试 (试用 kv，只能按 k 排序）
    # list1 = [("蛋糕", 3), ("巧克力", 14), ("糖果", 9), ("蛋糕", 8)]
    # rdd = sc.parallelize(list1, 1)
    # rs = rdd.sortByKey()
    # rs.foreach(lambda x: print(x))
    '''
    ('巧克力', 14)
    ('糖果', 9)
    ('蛋糕', 3)
    ('蛋糕', 8)
    '''

    # repartition & coalesce 算子测试
    # list1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    # rdd = sc.parallelize(list1, 1)
    # print(rdd.getNumPartitions())
    # rs = rdd.repartition(4)
    # print(rs.getNumPartitions())
    # rs2 = rs.coalesce(2)
    # print(rs2.getNumPartitions())
    '''
    1
    4
    2
    '''

    # keys & values 算子测试
    # list1 = [("蛋糕", 3), ("巧克力", 14), ("糖果", 9), ("蛋糕", 8)]
    # rdd = sc.parallelize(list1)
    # rdd.keys().foreach(lambda x: print(x))
    # rdd.values().foreach(lambda x: print(x))
    '''
    糖果
    巧克力
    蛋糕
    蛋糕
    9
    14
    3
    8
    '''

    # mapValues 算子测试i
    # list1 = [("蛋糕", 3), ("巧克力", 14), ("糖果", 9), ("蛋糕", 8)]
    # rdd = sc.parallelize(list1)
    # rdd.mapValues(lambda x: x + 1).foreach(lambda x: print(x))
    '''
    ('糖果', 10)
    ('巧克力', 15)
    ('蛋糕', 4)
    ('蛋糕', 9)
    '''

    # ======触发算子使用======

    # collect 收集算子，返回一个 list，会将数据合并到一个分区，如果 rdd 过大则不适用
    # list1 = [("蛋糕", 3), ("巧克力", 14), ("糖果", 9), ("蛋糕", 8)]
    # rdd = sc.parallelize(list1)
    # rs = rdd.collect()
    # print(rs)
    '''
    [('蛋糕', 3), ('巧克力', 14), ('糖果', 9), ('蛋糕', 8)]
    '''

    # collectAsMap 算子测试，返回一个 dict
    # list1 = [("蛋糕", 3), ("巧克力", 14), ("糖果", 9), ("蛋糕", 8)]
    # rdd = sc.parallelize(list1)
    # rs = rdd.collectAsMap()
    # print(rs)
    '''
    {'蛋糕': 8, '巧克力': 14, '糖果': 9}
    '''

    # reduce 累加算子

    # foreach

    # top & takeOrdered 算子测试，涉及合并排序，仅适合小数据量
    # list1 = [11, 12, 13, 34, 35, 26, 67, 48, 9, 10]
    # rdd = sc.parallelize(list1, 3)
    # rs = rdd.top(5)
    # print(rs)
    # rs1 = rdd.takeOrdered(5)
    # print(rs1)
    '''
    [67, 48, 35, 34, 26]
    [9, 10, 11, 12, 13]
    '''



    sc.stop()
