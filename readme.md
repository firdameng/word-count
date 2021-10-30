# window环境下idea中运行hadoop程序

- jdk 1.8
- hadoop 2.8.3



## 1. 下载hadoop 2.8.3

http://archive.apache.org/dist/hadoop/core/hadoop-2.8.3/

- 设置HADOOP_HOM环境变量 为hadoop解压目录
- 追加path环境变量，`%HADOOP_HOM%\bin`

## 2. 下载 winutils 

只用取2.8.3版本即可

 winutils ： Windows binaries for Hadoop versions

https://github.com/steveloughran/winutils

- 替换安装hadoop的bin为下载winuils下面2.8.3的bin
- 将2.8.3的bin中hadoop.dll拷贝到C:\Windows\System32目录下

> 参考博客：https://www.jianshu.com/p/a65a95108620

## 3.编写mapper,reducer,driver即可



# 搭建hadoop集群

> 参考https://blog.csdn.net/sculpta/article/details/107850280

## 软件准备

- Centos 7
- JDK 1.8
- Hadoop 2.9.2

## 集群规划

![Snipaste_2021-10-30_15-16-52](.\img\Snipaste_2021-10-30_15-16-52.png)



## vmware虚拟机网络配置

1. 「编辑」->「虚拟网络编辑器」->「VMnet8」->「设置 NAT」，如下图

这里我是默认配置, **注意这里网关ip 为 192.168.150.2，后面在虚拟机里面设置静态ip时，保持同一网段，且网关为192.168.150.2**

![Snipaste_2021-10-30_15-19-17](.\img\Snipaste_2021-10-30_15-19-17.png)



修改了下范围

![Snipaste_2021-10-30_15-20-01](.\img\Snipaste_2021-10-30_15-20-01.png)

## 配置静态ip

![Snipaste_2021-10-30_15-28-00](.\img\Snipaste_2021-10-30_15-28-00.png)



```sh
systemctl restart network  # 重启网络
ip addr # 查看IP
```



## 关闭防火墙

```sh
systemctl stop firewalld.service
systemctl disable firewalld.service
systemctl status firewalld
```

## 修改和设置hostname

```sh
echo hadoop1 > /etc/hostname

vi /etc/sysconfig/network
# 写入下面内容
NETWORKING=yes # 使用网络
HOSTNAME=hadoop1 # 设置主机名
```

## 配置host

```sh
vi /etc/hosts

追加下面内容
192.168.150.11 hadoop1
192.168.150.22 hadoop2
192.168.150.33 hadoop3
```

## 创建软件安装目录

```sh
mkdir -p /opt/cwc/software --软件安装包存放目录
mkdir -p /opt/cwc/servers --软件安装目录
```

### 安装jdk,配置环境变量

```sh
vi /etc/profile

# JAVA_HOME
export JAVA_HOME=/opt/module/jdk
export PATH=$PATH:$JAVA_HOME/bin

source /etc/profile
java version
```



### 安装hadoop，配置环境变量

```sh
vi /etc/profile

# HADOOP_HOME
export HADOOP_HOME=/opt/module/hadoop
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$HADOOP_HOME/sbin

source /etc/profile
hadoop version
```



## 克隆虚拟机

### 克隆

先将 hadoop1 关机，「右键」->「管理」->「克隆」

1. 选择创建完整克隆
2. 修改虚拟机名称为 hadoop2

### 配置网络

1. 修改 /etc/sysconfig/network-scripts/ifcfg-ens32 文件中的 IPADDR 为 192.168.150.22，

2. 并在ifcfg-ens32 文件中删除 UUID

3. 修改 hostname 为 hadoop2

重复以上步骤，克隆一个 hadoop3 节点


## 配置ssh无密码登录

1. 生成公钥

```shell
ssh-keygen -t rsa
```

2. 分发公钥

```shell
ssh-copy-id hadoop1
ssh-copy-id hadoop2
ssh-copy-id hadoop3
```

在三个节点上重复以上命令

## 编写集群分发脚本 xsync



在/usr/local/bin这个目录下存放的脚本，root用户可以在系统任何地方直接执行。  

（1）在/usr/local/bin目录下创建文件rsync-script，文件内容如下：  

```sh
touch rsync-script
vim rsync-script

#!/bin/bash
#1 获取命令输入参数的个数，如果个数为0，直接退出命令
slave_array=(hadoop2 hadoop3)
paramnum=$#
if((paramnum==0)); then
echo no params;
exit;
fi
#2 根据传入参数获取文件名称
p1=$1
file_name=`basename $p1`
echo fname=$file_name
#3 获取输入参数的绝对路径
pdir=`cd -P $(dirname $p1); pwd`
echo pdir=$pdir
#4 获取用户名称
user=`whoami`
#5 循环执行rsync
for host in ${slave_array[@]}; do
echo ------------------- linux$host --------------
rsync -rvl $pdir/$file_name $user@$host:$pdir
done

```



修改权限：

```sh
chmod 777 rsync-script  
```



## 集群配置

### hdfs配置

#### 将JDK路径明确配置给HDFS（修改hadoop-env.sh）

```xml
export JAVA_HOME=/opt/cwc/servers/jdk1.8.0_231
```



####  指定NameNode节点以及数据存储目录（修改core-site.xml）

```xml
<!-- 指定HDFS中NameNode的地址 -->
<property>
<name>fs.defaultFS</name>
<value>hdfs://hadoop1:9000</value>
</property>
<!-- 指定Hadoop运行时产生文件的存储目录 -->
<property>
<name>hadoop.tmp.dir</name>
<value>/opt/cwc/servers/hadoop-2.9.2/data/tmp</value>
</property>
```

####  指定SecondaryNameNode节点（修改hdfs-site.xml）

```xml
<!-- 指定Hadoop辅助名称节点主机配置 -->
<property>
<name>dfs.namenode.secondary.http-address</name>
<value>hadoop3:50090</value>
</property>
<!--副本数量 -->
<property>
<name>dfs.replication</name>
<value>3</value>
</property>
```



#### 指定DataNode从节点（修改etc/hadoop/slaves文件，每个节点配置信息占一行）

```
hadoop1
hadoop2
hadoop3
```



### MapReduce集群配置

#### 将JDK路径明确配置给MapReduce（修改mapred-env.sh）

```sh
export JAVA_HOME=/opt/cwc/servers/jdk1.8.0_231
```



####  指定MapReduce计算框架运行Yarn资源调度框架（修改mapred-site.xml）

```xml
<!-- 指定MR运行在Yarn上 -->
<property>
<name>mapreduce.framework.name</name>
<value>yarn</value>
</property>
```



### Yarn集群配置

####  将JDK路径明确配置给Yarn（修改yarn-env.sh）

```sh
export JAVA_HOME=/opt/cwc/servers/jdk1.8.0_231
```



#### 指定ResourceManager老大节点所在计算机节点（修改yarn-site.xml）

```xml
<!-- 指定YARN的ResourceManager的地址 -->
<property>
<name>yarn.resourcemanager.hostname</name>
<value>hadoop3</value>
</property>
<!-- Reducer获取数据的方式 -->
<property>
<name>yarn.nodemanager.aux-services</name>
<value>mapreduce_shuffle</value>
</property>
```



#### 指定NodeManager节点（会通过slaves文件内容确定）  



### 分发配置

```sh
rsync-script /opt/cwc/servers/hadoop-2.9.2
```



## hadoop集群启动停止命令

### 整体启动停止

1. 整体启动

start-dfs.sh   hadoop1执行

start-yarn.sh    hadoop2执行

2. 整体停止

stop-yarn.sh    hadoop2执行

stop-dfs.sh   hadoop1执行

### 单个启动停止

1. 启动、停止hdfs

```sh
hadoop-daemon.sh start / stop namenode / datanode / secondarynamenode
```

2. 启动、停止yarn

```sh
yarn-daemon.sh start / stop resourcemanager / nodemanager
```



### web端查看hdfs

```sh
http://hadoop:50070
```

## hadoop集群测试

1. HDFS文件系统根目录下面创建一个wcinput文件夹  

```sh
hdfs dfs -mkdir /wcinput  
```

2. 在/root/目录下创建一个wc.txt文件(本地文件系统)  

```sh
touch wc.txt
vi wc.txt

#写入以下内容
hadoop mapreduce yarn
hdfs hadoop mapreduce
mapreduce yarn lagou
datanode
namenode datanode

```

3. 上传wc.txt到Hdfs目录/wcinput下  

```sh
hdfs dfs -put wc.txt /wcinput
```

4. 执行hadoop自带的wordcount jar

```sh
cd /opt/cwc/servers/hadoop-2.9.2

hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.2.jar wordcount /wcinput
/wcoutput
```

5. 查看结果

```sh
hdfs dfs -cat /wcoutput/part-r-00000
```

也可登录 hadoop1:50070 hdfs的web界面去查看。



## 配置历史服务器

在Yarn中运行的任务产生的日志数据不能查看，为了查看程序的历史运行情况，需要配置一下历史日志
服务器。具体配置步骤如下  

### 配置mapred-site.xml  

增加如下内容

```xml
<!-- 历史服务器端地址 -->
<property>
<name>mapreduce.jobhistory.address</name>
<value>hadoop1:10020</value>
</property>
<!-- 历史服务器web端地址 -->
<property>
<name>mapreduce.jobhistory.webapp.address</name>
<value>hadoop1:19888</value>
</property>
```

### 配置yarn-site.xml  

```xml
<!-- 日志聚集功能使能 -->
<property>
<name>yarn.log-aggregation-enable</name>
<value>true</value>
</property>
<!-- 日志保留时间设置7天 -->
<property>
<name>yarn.log-aggregation.retain-seconds</name>
<value>604800</value>
</property>
```



### 分发到其他节点

```sh
rsync-script mapred-site.xml
rsync-script yarn-site.xml  
```

3. 重启集群后，再启动历史服务器(在hadoop1上执行，因为在集群中hadoop1上配置为历史服务器)

```sh
mr-jobhistory-daemon.sh start historyserver
```

4. 查看JobHistory  

```sh
http://hadoop1:19888/jobhistory
```

