package com.example.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author CaiWencheng
 * @Date 2021-10-30 17:06
 */
public class HdfsClient {
    @Test
    public void testMkdirs() throws IOException, InterruptedException,
            URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        // configuration.set("fs.defaultFS", "hdfs://linux121:9000");
        // FileSystem fs = FileSystem.get(configuration);
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.150.11:9000"),
                configuration, "root");
        // 2 创建目录
        fs.mkdirs(new Path("/test/hdfs"));


        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testCopyFromLocalFile() throws IOException,
            InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.150.11:9000"),
                configuration, "root");
        // 2 上传文件
        fs.copyFromLocalFile(new Path("E:\\IdeaProjects\\word-count\\src\\main\\resources\\wc.txt"), new
                Path("/test/hdfs/lagou.txt"));
        // 3 关闭资源
        fs.close();
        System.out.println("end");
    }

    @Test
    public void testListStatus() throws IOException, InterruptedException,
            URISyntaxException{
// 1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.150.11:9000"),
                configuration, "root");
// 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
// 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }
// 3 关闭资源
        fs.close();
    }

    @Test
    public void testListFiles() throws IOException, InterruptedException,
            URISyntaxException{
// 1获取文件系统5 文件夹判断
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.150.11:9000"),
                configuration, "root");
// 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/test/hdfs/"),
                true);
        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();
// 输出详情
// 文件名称
            System.out.println(status.getPath().getName());
// 长度
            System.out.println(status.getLen());
// 权限
            System.out.println(status.getPermission());
// 分组
            System.out.println(status.getGroup());
// 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
// 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("-----------华丽的分割线----------");
        }
// 3 关闭资源
        fs.close();
    }


    @Test
    public void testUploadPacket() throws IOException, URISyntaxException, InterruptedException {
        // 1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.150.11:9000"),
                configuration, "root");
//1 准备读取本地文件的输入流
        final FileInputStream in = new FileInputStream(new
                File("E:\\IdeaProjects\\word-count\\src\\main\\resources\\user_visit_action.csv"));
//2 准备好写出数据到hdfs的输出流
        final FSDataOutputStream out = fs.create(new Path("/test/hdfs/user_visit_action.txt"), new
                Progressable() {
                    @Override
                    public void progress() { //这个progress方法就是每传输64KB（packet）就会执行一次，
                        System.out.println("&");
                    }
                });
//3 实现流拷贝
        IOUtils.copyBytes(in, out, configuration); //默认关闭流选项是true，所以会自动关闭
//4 关流 可以再次关闭也可以不关了
    }
}
