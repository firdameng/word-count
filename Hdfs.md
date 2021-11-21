# Hdfs

## 查看hdfs文件在磁盘上路径和内容

> 参考：https://blog.csdn.net/weixin_43114954/article/details/115571939?utm_term=hdfs%E6%95%B0%E6%8D%AE%E5%AD%98%E5%82%A8%E5%9C%A8%E5%93%AA&utm_medium=distribute.pc_aggpage_search_result.none-task-blog-2~all~sobaiduweb~default-2-115571939&spm=3001.4430

例如 hdfs 目录下的文件l路径：

`/test/hdfs/user_visit_action.txt`

![Snipaste_2021-10-30_20-18-24](E:\IdeaProjects\word-count\img\Snipaste_2021-10-30_20-18-24.png)

![image-20211030203312171](.\img\image-20211030203312171.png)

由于文件小于128M,所以用一个block存储即可，然后再hadoop1,hadoop2,hadoop3都有备份。



首先得知道hdfs文件存储路径，在hadoop官网文档中默认配置模块找到hdfs-default.xml( 文件存储相关的配置)

http://hadoop.apache.org/docs/r2.9.2/

![Snipaste_2021-10-30_20-25-33](E:\IdeaProjects\word-count\img\Snipaste_2021-10-30_20-25-33.png)

![image-20211030202704559](.\img\image-20211030202704559.png)



而`hadoop.tmp.dir`属性 在 hdfs配置 `core-site.xml` 中已经配置了。

![image-20211030202922388](.\img\image-20211030202922388.png)

因此，hdfs 目录文件 实际磁盘存储位置为 

`/opt/cwc/servers/hadoop-2.9.2/data/tmp/dfs/data`

![image-20211030210625031](.\img\image-20211030210625031.png)

![image-20211030210732380](.\img\image-20211030210732380.png)

最终 blk_1073741861 存储的是  `/test/hdfs/user_visit_action.txt`文件内容。

即最终存储位置在 `/opt/cwc/servers/hadoop-2.9.2/data/tmp/dfs/data/current/BP-662313219-192.168.150.11-1635568953553/current/finalized/subdir0/subdir0/blk_1073741861` 



![image-20211030210119615](.\img\image-20211030210119615.png)

可以发现，block有  block pool的概念，然后最终磁盘存储文件名为：blk_{block Id}

并且在hadoop2,hadoop3 相同磁盘路径上，也找到了这个文件。

![image-20211030210732380](.\img\image-20211030210732380.png)



## block pool

Block pool(块池)就是属于单个命名空间的一组block(块)

Datanode中的数据结构都通过块池ID（BlockPoolID）索引，即datanode中的BlockMap，storage等都通过BPID索引。在HDFS中，所有的更新、回滚都是以Namenode和BlockPool为单元发生的。即同一HDFS Federation中不同的Namenode/BlockPool之间没有什么关系。Hadoop V0.23版本中Block Pool的管理功能依然放在了Namenode中，将来的版本中会将Block Pool的管理功能移动的新的功能节点中。





# Mapreduce

## map阶段

![image-20211101145114268](C:\Users\CaiWencheng\AppData\Roaming\Typora\typora-user-images\image-20211101145114268.png)

### 1. 对输入目录中文件进行分片

1. 读取数据组件InputFormat（默认TextInputFormat）会通过getSplits方法对输入目录中文件进行逻辑切片规划得到splits，有多少个split就对应启动多少个MapTask。split与block的对应关系默认是一对一。  

> - 支持输入目录中，存在多个文件
> - 一个分片默认128M,切分时判断文件大小是否> 128M * 1.1，是的话，切分，否则直接作为一个分片。 即129M的文件会作为1个分片处理

### 2. 自定义map解析记录

2. 将输入文件切分为splits之后，由RecordReader对象（默认LineRecordReader）进行读取，以\n作为分隔符，读取一行数据，返回<key，value>。Key表示每行首字符偏移值，value表示这一行文本内容。  

3. 读取split返回<key,value>，进入用户自己继承的Mapper类中，执行用户重写的map函数。RecordReader读取一行这里调用一次。  

`org.apache.hadoop.mapreduce.Mapper#run`

```java
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    try {
      while (context.nextKeyValue()) {
          // map函数为用户重写的mapper类中map方法
        map(context.getCurrentKey(), context.getCurrentValue(), context);
      }
    } finally {
      cleanup(context);
    }
  }
```

### 3. 对kv进行分区

4. map逻辑完之后，将map的每条结果通过context.write进行collect数据收集。在collect中，会先对其进行分区处理，默认使用HashPartitioner。  

org.apache.hadoop.mapred.MapTask.NewOutputCollector#write

```java
    public void write(K key, V value) throws IOException, InterruptedException {
        //会getPartition 计算分区
      collector.collect(key, value,
                        partitioner.getPartition(key, value, partitions));
    }
```



org.apache.hadoop.mapreduce.lib.partition.HashPartitioner#getPartition

默认使用HashPartitioner，应该有个前提是分区数大于1，

```java
  /** Use {@link Object#hashCode()} to partition. */
  public int getPartition(K key, V value,
                          int numReduceTasks) {
    return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  }
```

> 如果本地调试环境，则默认分区数为1，最后返回的分区为0
>
> ```java
> // 为这个作业配置 reduce 任务的数量。默认为1
> partitions = jobContext.getNumReduceTasks();
>       if (partitions > 1) {
>         partitioner = (org.apache.hadoop.mapreduce.Partitioner<K,V>)
>           ReflectionUtils.newInstance(jobContext.getPartitionerClass(), job);
>       } else {
>         partitioner = new org.apache.hadoop.mapreduce.Partitioner<K,V>() {
>           @Override
>           public int getPartition(K key, V value, int numPartitions) {
>             return partitions - 1;
>           }
>         };
>       }
> ```
>
> 



有个疑问： hadoop 中 job概念？？？



MapReduce提供Partitioner接口，它的作用就是根据key或value及reduce的数量来决定当前的这对输出数据最终应该交由哪个reduce task处理。

`org.apache.hadoop.mapreduce.Partitioner#getPartition`

默认对key hash后再以reduce task数量取模。默认的取模方式只是为了平均reduce的处理能力，如果用户自己对Partitioner有需求，可以订制并设置到job上。

### 4. map端 shuffle





#### 4.1 写入环形内存缓冲区

5. 接下来，会将数据写入内存，内存中这片区域叫做环形缓冲区，缓冲区的作用是批量收集map结
   果，减少磁盘IO的影响。我们的key/value对以及Partition的结果都会被写入缓冲区。当然写入之
   前，key与value值都会被序列化成字节数组。  

- 环形缓冲区其实是一个数组，数组中存放着key、value的序列化数据和key、value的元数据信
  息，包括partition、key的起始位置、value的起始位置以及value的长度。环形结构是一个抽象概
  念。

```java
// 环形缓冲区是一个字节数组
byte[] kvbuffer; 

// 元数据默认16个字节
private static final int NMETA = 4;            // num meta ints
private static final int METASIZE = NMETA * 4; // size in bytes

// 缓冲区默认100Mb
final int sortmb = job.getInt(JobContext.IO_SORT_MB, 100);

// buffers and accounting
int maxMemUsage = sortmb << 20;  // Mb转换为byte
maxMemUsage -= maxMemUsage % METASIZE;  // 去掉多余byte,保证数组长度是16整数倍，比如33-》32
kvbuffer = new byte[maxMemUsage];

//执行溢出写的阈值，默认超过缓冲区80%开始执行溢出写线程
final float spillper = job.getFloat(JobContext.MAP_SORT_SPILL_PERCENT, (float)0.8);
softLimit = (int)(kvbuffer.length * spillper);
bufferRemaining = softLimit;
```



包含内容：

（1） key、value的序列化数据

（2）partition、key的起始位置、value的起始位置以及value的长度。起什么作用？？？？

- 用于后面溢出写时按partion,key排序使用；
- 写入文件时，通过元数据，按分区，找到k,v的地址，得到k,v字节数据

> org.apache.hadoop.mapred.MapTask.MapOutputBuffer#sortAndSpill 详情在这里，

将数组抽象为一个环形结构之后，以equator为界，key/value顺时针存储，元数据逆时针存储。

![image-20211101173821881](C:\Users\CaiWencheng\AppData\Roaming\Typora\typora-user-images\image-20211101173821881.png)



![环形缓冲区](E:\IdeaProjects\word-count\img\环形缓冲区.png)



- 缓冲区是有大小限制，默认是100MB。当map task的输出结果很多时，就可能会撑爆内存，所以
  需要在一定条件下将缓冲区中的数据临时写入磁盘，然后重新利用这块缓冲区。这个从内存往磁盘
  写数据的过程被称为Spill，中文可译为溢写。这个溢写是由单独线程来完成，不影响往缓冲区写
  map结果的线程。溢写线程启动时不应该阻止map的结果输出，所以整个缓冲区有个溢写的比例
  spill.percent。这个比例默认是0.8，也就是当缓冲区的数据已经达到阈值（buffer size * spill
  percent = 100MB * 0.8 = 80MB），溢写线程启动，锁定这80MB的内存，执行溢写过程。Map
  task的输出结果还可以往剩下的20MB内存中写，互不影响。  

> 溢出写时，如何往剩余20% kvBuffer写数据？如果还是按照原来方向写，必定会出现碰头的情况。
>
> 因此：Map取kvbuffer中剩余空间的中间位置，用这个位置设置为新的分界点，bufindex指针移动到这个分界点，Kvindex移动到这个分界点的-16位置，然后两者就可以和谐地按照自己既定的轨迹放置数据了，当Spill完成，空间腾出之后，不需要做任何改动继续前进
>
> ![溢出写时写kvbuffer](E:\IdeaProjects\word-count\img\溢出写时写kvbuffer.png)

写入环形缓冲区完整方法

`org.apache.hadoop.mapred.MapTask.MapOutputBuffer#collect`

```java
// 同步关键字
public synchronized void collect(K key, V value, final int partition
                                     ) throws IOException {
      ...
      bufferRemaining -= METASIZE;
     // 如果剩余buffer不够写入，执行溢出写
      if (bufferRemaining <= 0) {
        // start spill if the thread is not running and the soft limit has been
        // reached ，加锁了
        spillLock.lock();
        try {
           // 这个do循环有啥意义？？？
          do {
            // 如果溢出写线程还在执行，其他线程不受影响，继续往buffer中写数据
            if (!spillInProgress) {
              final int kvbidx = 4 * kvindex;
              final int kvbend = 4 * kvend;
              // serialized, unspilled bytes always lie between kvindex and
              // bufindex, crossing the equator. Note that any void space
              // created by a reset must be included in "used" bytes
              final int bUsed = distanceTo(kvbidx, bufindex);
              final boolean bufsoftlimit = bUsed >= softLimit;
                
              if ((kvbend + METASIZE) % kvbuffer.length !=
                  equator - (equator % METASIZE)) {
                // spill finished, reclaim space
                // 主要是调整指针
                resetSpill();
                bufferRemaining = Math.min(
                    distanceTo(bufindex, kvbidx) - 2 * METASIZE,
                    softLimit - bUsed) - METASIZE;
                continue;
              } else if (bufsoftlimit && kvindex != kvend) {
                 // 1) 设置spillInProgress为true
                // 2）这里发送信号给spill线程开始溢出写，org.apache.hadoop.mapred.MapTask.MapOutputBuffer.SpillThread
                // 溢出写线程的run方法中 有个关键方法sortAndSpill，主要流程就是  排序sort--->合并combiner--->生成溢出写文件，和索引文件
                startSpill();
                final int avgRec = (int)
                  (mapOutputByteCounter.getCounter() /
                  mapOutputRecordCounter.getCounter());
                // leave at least half the split buffer for serialization data
                // ensure that kvindex >= bufindex
                final int distkvi = distanceTo(bufindex, kvbidx);
                final int newPos = (bufindex +
                  Math.max(2 * METASIZE - 1,
                          Math.min(distkvi / 2,
                                   distkvi / (METASIZE + avgRec) * METASIZE)))
                  % kvbuffer.length;
                setEquator(newPos);
                bufmark = bufindex = newPos;
                final int serBound = 4 * kvend;
                // bytes remaining before the lock must be held and limits
                // checked is the minimum of three arcs: the metadata space, the
                // serialization space, and the soft limit
                bufferRemaining = Math.min(
                    // metadata max
                    distanceTo(bufend, newPos),
                    Math.min(
                      // serialization max
                      distanceTo(newPos, serBound),
                      // soft limit
                      softLimit)) - 2 * METASIZE;
              }
            }
          } while (false);
        } finally {
          spillLock.unlock();
        }
      }
	 // 通知溢出写线程执行后，继续写数据到buffer中
      try {
        // serialize key bytes into buffer
        int keystart = bufindex;
        // 序列化key到buffer中时，会更新bufindex索引
        keySerializer.serialize(key);
        if (bufindex < keystart) {
          // wrapped the key; must make contiguous
          bb.shiftBufferedKey();
          keystart = 0;
        }
        // serialize value bytes into buffer
        final int valstart = bufindex;
        // 同理序列化value到buffer中时，会更新bufindex索引
        valSerializer.serialize(value);
        // It's possible for records to have zero length, i.e. the serializer
        // will perform no writes. To ensure that the boundary conditions are
        // checked and that the kvindex invariant is maintained, perform a
        // zero-length write into the buffer. The logic monitoring this could be
        // moved into collect, but this is cleaner and inexpensive. For now, it
        // is acceptable.
        bb.write(b0, 0, 0);

        // the record must be marked after the preceding write, as the metadata
        // for this record are not yet written
        int valend = bb.markRecord();

        mapOutputRecordCounter.increment(1);
        mapOutputByteCounter.increment(
            distanceTo(keystart, valend, bufvoid));

        // 写入统计信息（元数据），4个int类型数据，所以占16个字节
        kvmeta.put(kvindex + PARTITION, partition);
        kvmeta.put(kvindex + KEYSTART, keystart);
        kvmeta.put(kvindex + VALSTART, valstart);
        kvmeta.put(kvindex + VALLEN, distanceTo(valstart, valend));
        // 更新 kvindex
        kvindex = (kvindex - NMETA + kvmeta.capacity()) % kvmeta.capacity();
      } catch (MapBufferTooSmallException e) {
        LOG.info("Record too large for in-memory buffer: " + e.getMessage());
        spillSingleRecord(key, value, partition);
        mapOutputRecordCounter.increment(1);
        return;
      }
    }
```





`org.apache.hadoop.mapred.MapTask.MapOutputBuffer.Buffer#write(byte[], int, int)`

真正写入k,v到kvbuffer的方法，同时会更新 `bufindex`





#### 4.2 执行溢出写

所以环形缓冲区 至少有2个好处：

- 在写到磁盘文件之前，对maper的kv对，按key进行排序，合并combiner(有必要的话)

> 因为后面对多个 溢出写文件，进行归并，得到一个按key分组排序的 文件。

`org.apache.hadoop.mapred.MapTask.MapOutputBuffer#startSpill` 具体流程如下：

   包括： 排序sort--->合并combiner--->生成溢出写文件，和索引文件

6、当溢写线程启动后，需要对这80MB空间内的key做排序(Sort)。排序是MapReduce模型默认的行为!

> 先把Kvbuffer中的数据按照partition值和key两个关键字升序排序，移动的只是索引数据，排序结果是Kvmeta中数据按照partition为单位聚集在一起，同一partition内的按照key有序。

如果job设置过Combiner，那么现在就是使用Combiner的时候了。将有相同key的key/value对的
value加起来，减少溢写到磁盘的数据量。Combiner会优化MapReduce的中间结果，所以它在整
个模型中会多次使用。
那哪些场景才能使用Combiner呢？从这里分析，Combiner的输出是Reducer的输入，Combiner
绝不能改变最终的计算结果。Combiner只应该用于那种Reduce的输入key/value与输出key/value
类型完全一致，且不影响最终结果的场景。比如累加，最大值等。Combiner的使用一定得慎重，
如果用好，它对job执行效率有帮助，反之会影响reduce的最终结果。



> **所有的partition对应的数据都放在这个文件里，虽然是顺序存放的，但是怎么直接知道某个partition在这个文件中存放的起始位置呢？**
>
> 强大的索引又出场了。有一个三元组记录某个partition对应的数据在这个文件中的索引：起始位置、原始数据长度、压缩之后的数据长度，一个partition对应一个三元组。然后把这些索引信息存放在内存中，如果内存中放不下了，后续的索引信息就需要写到磁盘文件中了：
>
> ![溢出写文件](E:\IdeaProjects\word-count\img\溢出写文件.png)



`org.apache.hadoop.mapred.MapTask.MapOutputBuffer#sortAndSpill`

```java
private void sortAndSpill() throws IOException, ClassNotFoundException,
                                       InterruptedException {
      //approximate the length of the output file to be the length of the
      //buffer + header lengths for the partitions
      final long size = distanceTo(bufstart, bufend, bufvoid) +
                  partitions * APPROX_HEADER_LENGTH;
      FSDataOutputStream out = null;
      try {
        // 创建溢出写文件
        final SpillRecord spillRec = new SpillRecord(partitions);
        final Path filename =
            mapOutputFile.getSpillFileForWrite(numSpills, size);
        out = rfs.create(filename);
		
        final int mstart = kvend / NMETA;
        final int mend = 1 + // kvend is a valid record
          (kvstart >= kvend
          ? kvstart
          : kvmeta.capacity() + kvstart) / NMETA;
        sorter.sort(MapOutputBuffer.this, mstart, mend, reporter);
        int spindex = mstart;
        final IndexRecord rec = new IndexRecord();
        final InMemValBytes value = new InMemValBytes();
        for (int i = 0; i < partitions; ++i) {
          IFile.Writer<K, V> writer = null;
          try {
            long segmentStart = out.getPos();
            FSDataOutputStream partitionOut = CryptoUtils.wrapIfNecessary(job, out);
            writer = new Writer<K, V>(job, partitionOut, keyClass, valClass, codec,
                                      spilledRecordsCounter);
             // 未设置combine时
            if (combinerRunner == null) {
              // spill directly
              DataInputBuffer key = new DataInputBuffer();
               // 通过元数据，找到分区为i的kv,依次写入文件中
                // 实现该分区的kv,都集中在一起写入文件中。
              while (spindex < mend &&
                  kvmeta.get(offsetFor(spindex % maxRec) + PARTITION) == i) {
                final int kvoff = offsetFor(spindex % maxRec);
                int keystart = kvmeta.get(kvoff + KEYSTART);
                int valstart = kvmeta.get(kvoff + VALSTART);
                key.reset(kvbuffer, keystart, valstart - keystart);
                getVBytesForOffset(kvoff, value);
                writer.append(key, value);
                ++spindex;
              }
            } else {
              int spstart = spindex;
              while (spindex < mend &&
                  kvmeta.get(offsetFor(spindex % maxRec)
                            + PARTITION) == i) {
                ++spindex;
              }
              // Note: we would like to avoid the combiner if we've fewer
              // than some threshold of records for a partition
              if (spstart != spindex) {
                combineCollector.setWriter(writer);
                RawKeyValueIterator kvIter =
                  new MRResultIterator(spstart, spindex);
                combinerRunner.combine(kvIter, combineCollector);
              }
            }

            // close the writer
            writer.close();

            // record offsets
            rec.startOffset = segmentStart;
            rec.rawLength = writer.getRawLength() + CryptoUtils.cryptoPadding(job);
            rec.partLength = writer.getCompressedLength() + CryptoUtils.cryptoPadding(job);
            spillRec.putIndex(rec, i);

            writer = null;
          } finally {
            if (null != writer) writer.close();
          }
        }

        if (totalIndexCacheMemory >= indexCacheMemoryLimit) {
          // create spill index file
          Path indexFilename =
              mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
                  * MAP_OUTPUT_INDEX_RECORD_LENGTH);
          spillRec.writeToFile(indexFilename, job);
        } else {
          indexCacheList.add(spillRec);
          totalIndexCacheMemory +=
            spillRec.size() * MAP_OUTPUT_INDEX_RECORD_LENGTH;
        }
        LOG.info("Finished spill " + numSpills);
        ++numSpills;
      } finally {
        if (out != null) out.close();
      }
    }
```







#### 4.3 归并merge

7. 归并溢写文件：每次溢写会在磁盘上生成一个临时文件（写之前判断是否有combiner），如果
map的输出结果真的很大，有多次这样的溢写发生，磁盘上相应的就会有多个临时文件存在。当
整个数据处理结束之后开始对磁盘中的临时文件进行merge合并，因为最终的文件只有一个，写入
磁盘，并且为这个文件提供了一个索引文件，以记录每个reduce对应数据的偏移量。  

> 
>
> 归并多个溢出写文件：
>
> 当一个``map task``处理的数据很大，以至于超过缓冲区内存时，就会生成多个``spill``文件。
>
> 此时就需要对同一个``map``任务产生的多个``spill``文件进行归并生成最终的一个已分区且已排序的大文件。
>
> 配置属性``mapreduce.task.io.sort.factor``控制着一次最多能合并多少流，默认值是``10``。
>
> 这个过程包括排序和合并（可选），因为归并得到的文件内键值对有可能拥有相同的``key``，这``个过程如果``client``设置过``Combiner``，也会合并相同的``key值的键值对（根据上面提到的combine的调用时机可知）
>
> > 合并（Combine）和归并（Merge）的区别：
> >  两个键值对`<“a”,1>`和`<“a”,1>`，如果合并，会得到`<“a”,2>`，如果归并，会得到`<“a”,<1,1>>`
>
> 因此 如果指定了Combiner，可能在两个地方被调用： 
>
> 1.当为作业设置Combiner类后，缓存溢出线程将缓存存放到磁盘时，就会调用； 
>
> 2.缓存溢出的数量超过mapreduce.map.combine.minspills（默认3）时，在缓存溢出文件合并的时候会调用
>
> 归并示意图：
>
> ![归并多个溢出写文件](E:\IdeaProjects\word-count\img\归并多个溢出写文件.png)
>
> （1）merge过程创建一个叫file.out的文件和一个叫file.out.Index的文件用来存储最终的输出和索引。
>
> （2）按partition来进行合并输出。对于某个partition来说，从索引列表中查询这个partition对应的所有索引信息，每个对应一个段插入到段列表中。也就是这个partition对应一个段列表，记录所有的Spill文件中对应的这个partition那段数据的文件名、起始位置、长度等等。然后对这个partition对应的所有的segment进行合并，目标是合并成一个segment。
>
> （3）溢出写文件归并完毕后，``Map``将删除所有的临时溢出写文件，并告知``NodeManager``任务已完成，``只要其中一个``MapTask``完成，``ReduceTask``就开始复制它的输出（``Copy``阶段分区输出文件通过``http``的方式提供给``reducer``）

#### 4.4 压缩

写磁盘时压缩map端的输出，因为这样会让写磁盘的速度更快，节约磁盘空间，并减少传给reducer的数据量。默认情况下，输出是不压缩的(将mapreduce.map.output.compress设置为true即可启动）



https://hadoop.apache.org/docs/r2.9.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml

溢出写相关的一些配置

![image-20211101184402409](C:\Users\CaiWencheng\AppData\Roaming\Typora\typora-user-images\image-20211101184402409.png)



## reduce阶段

![reduce流程](E:\IdeaProjects\word-count\img\reduce流程.png)

org.apache.hadoop.mapred.ReduceTask#run

```java
// 明确划分了这3个阶段
if (isMapOrReduce()) {
      copyPhase = getProgress().addPhase("copy");
      sortPhase  = getProgress().addPhase("sort");
      reducePhase = getProgress().addPhase("reduce");
    }
```



### 复制文件



Reduce进程启动一些数据copy线程，通过HTTP方式请求MapTask所在的NodeManager以获取输出文件。 

NodeManager需要为分区文件运行reduce任务。并且reduce任务需要集群上若干个map任务的map输出作为其特殊的分区文件。而每个map任务的完成时间可能不同，**因此只要有一个任务完成，reduce task就开始复制其输出。**

 

reduce任务有少量复制线程，因此能够并行取得map输出。默认线程数为5，但这个默认值可以通过mapreduce.reduce.shuffle.parallelcopies属性进行设置。

 

【Reducer如何知道自己应该处理哪些数据呢？】 

因为Map端进行partition的时候，实际上就相当于指定了**每个Reducer要处理的数据(partition就对应了Reducer)**，所以Reducer在拷贝数据的时候只需拷贝与自己对应的partition中的数据即可。每个Reducer会处理一个或者多个partition。

 

【reducer如何知道要从哪台机器上去的map输出呢？】 

map任务完成后，它们会使用**心跳机制通知它们的application master**、因此对于指定作业，application master知道map输出和主机位置之间的映射关系。reducer中的一个线程定期询问master以便获取map输出主机的位置, 直到获得所有输出位置。

### 合并文件

这里的归并和 溢出写时 的归并作用不一样：

**溢出写归并： 针对一个maptask，产生多个溢出写文件时，为了合并成最终1个文件，所进行归并排序。**

**这里的归并：从属于某个reduce的 mappertask 复制输出文件后，针对多个maptask, 输出数据进行归并和排序。**

把复制到Reducer本地数据，全部进行合并，即把分散的数据合并成一个大的数据。再对合并后的数据排序。

![reduce合并文件](E:\IdeaProjects\word-count\img\reduce合并文件.png)

 Copy 过来的数据会先放入内存缓冲区中，这里的缓冲区大小要比 map 端的更为灵活，

**它基于 JVM 的 heap size 设置，因为 Shuffle 阶段 Reducer 不运行，所以应该把绝大部分的内存都给 Shuffle 用。**（这时shuffle阶段？？）

 

Copy过来的数据会先放入内存缓冲区中，如果内存缓冲区中能放得下这次数据的话就直接把数据写到内存中，**即内存到内存merge。**

Reduce要向每个Map去拖取数据，在内存中每个Map对应一块数据，**当内存缓存区中存储的Map数据占用空间达到一定程度的时候**，开始启动内存中merge，**把内存中的数据merge输出到磁盘上一个文件中，即内存到磁盘merge。**

与map端的溢写类似，在将buffer中多个map输出合并写入磁盘之前，如果设置了Combiner，则会化简压缩合并的map输出。

- Reduce的内存缓冲区可通过mapred.job.shuffle.input.buffer.percent配置，默认是JVM的heap size的70%。

- 内存到磁盘merge的启动门限可以通过mapred.job.shuffle.merge.percent配置，默认是66%。

 

当属于该reducer的map输出全部拷贝完成，则会在reducer上生成多个文件（如果拖取的所有map数据总量都没有内存缓冲区，则数据就只存在于内存中），这时开始执行合并操作，即**磁盘到磁盘merge。**（重要！！！！）

**Map的输出数据已经是有序的，Merge进行一次合并排序**，所谓Reduce端的sort过程就是这个合并的过程，采取的排序方法跟map阶段不同，因为每个map端传过来的数据是排好序的，因此众多排好序的map输出文件在reduce端进行合并时采用的是归并排序，针对键进行归并排序。一般Reduce是一边copy一边sort，即copy和sort两个阶段是重叠而不是完全分开的。**最终Reduce shuffle过程会输出一个整体有序的数据块。**

 

### 调用reduce方法

对排序后的键值对调用reduce方法。**键相等的键值对调用一次reduce方法**，每次调用会产生零个或者多个键值对。最后把这些输出的键值对写入到HDFS文件中。

> 参考 https://blog.csdn.net/weiyongle1996/article/details/74784031



## shuffle阶段

maptask的map方法之后，reductTask的reduce方法之前数据处理流程称之为 shuffle.

进一步细分为，map shuffle,何 reduce shuffle