p94 working with numbers

#### Chapter 5 基本结构化操作

##### 5.1 概念

数据框（DataFrame）: 一系列记录（表中的行），数据类型为**行**（Row）与多个列(Columns)，数据集（Dataset）中的单个记录。DataFrame是元素均为Row的DataSet。

模式（Schemas）：来自数据库系统的概念，定义数据每一列的名字和类型。可以自己定义也可以直接导入数据，由数据决定。

分区（Partitioning）：数据框的分区定义了数据框的布局以及数据集在集群上的物理分布。

分区模式（Partitioning Schema）：定义了数据分布的方式。

结构类型（StructType）：由几个域（fields）组成，结构域（StructFields）包含了数据列名、数据类型和是否存在空值信息（name,type,nullable）。

转换（Spark transformation）：修改数据框的计划，由于lazy机制，需要先设置数据的转换计划再一起执行。有narrow transforamtion和wide transformation等多种方式。

e.g. :memo:增删行列、行列转换、根据列值进行行排序

表达式（Expressions）：列（Columns）也是表达式；表达式是对数据框上一个或多个记录进行的一系列变换。spark对一系列变换进行逻辑分词，优化逻辑计划（logical plan）之后再生成物理计划进行执行。

##### 5.2 函数

###### 载入

df = spark.read.format("-csv/json-").load("-dir-")  读入数据

可使用.option("header","true")

###### 模式

df.printSchema() 打印df的数据类型（模式）

myManualSchema = StructType([StructField("xxx",StringType(),True),...])手动设置模式

df.columns 获取列名

myDf = spark.createDataFrame([myRow], myManualSchema) 利用手动生成的行与模式生成数据框

###### 表达式

expr("") 输入表达式交给spark生成计算逻辑

###### 行

df.first() 获取第一行

myRow = Row() 初始化一行数据

myRow[i] 行内第i个数据

case::video_camera:多行数据生成

```python
newRows = [Row(), Row()...] 生成多行数据

parallelizedRows = spark.sparkContext.parallelize(newRows) 多行数据排列

newDF = spark.createDataFrame(parallelizedRows, schema)
```

df.union(newDF).where()... 数据框df与数据框newDF合并，使用where筛选

df.filter(col("count") < 2) 或者 df.where("count < 2").show()过滤行记录，多个筛选条件可以分别加入，spark会在最后统一执行，因此多个AND关系的纳排条件存在时可以连续使用.where。

df.select().distinct().count() 计数不重复的记录

df.randomSplit([portion1, portion2...], seed) 随机分开样本，根据比例（portion）划分，保存列表。

case::video_camera: 随机抽样

```python
seed = 5 # 随机种子
withReplacement = False # 是否替换
fraction = 0.5 # 抽样比例
df.sample(withReplacement, fraction, seed).count() #抽样函数
```

df.take(N) 取出N行

###### 列

df.withColumn("列名",lit(1)) 插入一列，.lit()函数可以加入一列数字1。

e.g. :memo:

```python
df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)
```

df.withColumnRenamed("列名1","列名2").columns 修改列名

df.drop("").columns 删除列

df.withColumn("count2", col("count").cast("long")) 使用cast进行类型转换

###### 表

df.createOrReplaceTempView("tableName") 将dataframe转换为与sparkSession同寿命的表，用于查询

df.select("").show() SQL表查询语句，select中可以写“列名”或者expr("列名")或者col("列名")或者column("列名")都是等价的。

SELECT... FROM... LIMIT... sql查询语句

e.g. :memo:

```python
df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
```

等价于

```sql
SELECT DEST_COUNTRY_NAME AS destination FROM dfTable LIMIT 2
```

等价于在pyspark中使用.alias("别名")

等价于

```python
df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)
```

df.selectExpr("表达式1","表达式2"...) 表达式可以为聚合函数，

也可以用来给列改名字。当列名有空格以及其他特殊符号时，使用``引用。

e.g.:memo:

```python
df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
```

df.orderBy() 可以填入expr或者col().desc()

df.sortWithinPartitions("") 在分区内排序

df.limit() 显示特定有限数量的记录

###### 分区

df.rdd.getNumPartitions() 返回分区数量

df.repartition() 重新分配分区，可以选定分区数量、选定列名从而根据列进行重分区

df.collect() 将数据收集到驱动，将RDD数据类型转化为数组存放，一次collect一次shuffle，数据在本地运行

df.toLocalIterator() 逐个分区地将数据收集到驱动



##### 5.3 pyspark库

from pyspark.sql.types import StructField, StructType, StringType, LongType 手动设置模式

from pyspark.sql import Row 从pyspark载入Row

set spark.sql.caseSensitive true 设置sql是否区分大小写，默认不区分

from pyspark.sql.functions import desc, asc 载入排序工具

#### Chapter 6 使用不同类型的数据

##### 6.1 概念

布尔型：spark会将布尔型数据的执行逻辑在最后一起执行，因此可以将and逻辑操作串联在一起。

##### 6.2 函数

###### 数据类型

lit() 将数据转化为spark类型

e.g.:memo:filter定义

```python
priceFilter = col("xxx") > 600
# instr 返回N以后字符串s2中第一次出现s1的索引
descripFilter = instr(df.Description, "POSTAGE") >= 1
# filter定义后可以在后面直接用逻辑关系连接
```

###### 字符处理

instr('s1', 's2', N) 返回N以后字符串s2中第一次出现s1的索引

.isin() 存在某字符，返回布尔值

col("xx").eqNullSafe() 对一整列查看是否全为空，对空值安全的equal运算

###### 数值类型



##### 6.3 pyspark库













