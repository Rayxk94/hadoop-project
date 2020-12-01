package com.xk.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

public class HDFSAPITest {

    FileSystem fileSystem = null;

    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        URI uri = new URI("hdfs://bigdatatest02:8020");
        fileSystem = FileSystem.get(uri, conf, "hdfs");
    }

    @After
    public void cleanUp() throws Exception {
        if (null != fileSystem) fileSystem.close();
    }

    @Test
    public void mkdir() throws Exception {
        boolean res = fileSystem.mkdirs(new Path("/demo"));
        System.out.println(res);
    }

    @Test
    public void copyFromLocalFile() throws Exception {
        // 文件地址
        Path src = new Path("E:\\workspace\\java\\hadoop-project\\hdfs-basic\\data\\demo.txt");
        // 目标地址
        Path dst = new Path("/demo/demo.txt");
        fileSystem.copyFromLocalFile(src, dst);
    }

    @Test
    public void copyFromLocalFileByIo() throws Exception {
        FSDataOutputStream outputStream = fileSystem.create(new Path("/demo/demo2.txt"), true);
        BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(new File("E:\\workspace\\java\\hadoop-project\\hdfs-basic\\data\\demo.txt")));
        IOUtils.copyBytes(inputStream, outputStream, 2048);
        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(inputStream);
    }

    @Test
    public void copyToLocalFile() throws Exception {
        // HDFS路径
        Path src = new Path("/demo/demo.txt");
        // 本地路径
        Path dst = new Path("E:\\workspace\\java\\hadoop-project\\hdfs-basic\\data\\demo1.txt");
        fileSystem.copyToLocalFile(src, dst);
    }

    @Test
    public void copyToLocalFileByIo() throws Exception {
        FSDataInputStream inputStream = fileSystem.open(new Path("/demo/demo1.txt"));
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(new File("E:\\workspace\\java\\hadoop-project\\hdfs-basic\\data\\demo2.txt")));
        IOUtils.copyBytes(inputStream, outputStream, 2048);
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(outputStream);
    }


    @Test
    public void reName() throws Exception {
        // 原文件路径
        Path src = new Path("/demo/demo.txt");
        // 新文件路径
        Path dst = new Path("/demo/demo1.txt");
        boolean res = fileSystem.rename(src, dst);
        System.out.println(res);
    }

    @Test
    public void listFiles() throws Exception {
        /**
         * Path f : 路径
         * boolean recursive ： 是否递归
         */
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/demo"), true);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            String path = file.getPath().toString().trim();
            String isdic = file.isDirectory() ? "文件夹" : "文件";
            String owner = file.getOwner();
            System.out.println(isdic + "\t" + path + "\t" + owner);
            // 得到该文件的副本地址
            BlockLocation[] blockLocations = file.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 得到该副本的存储地址
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }

        }
    }

    @Test
    public void delete() throws Exception {

        /**
         * Path f : 删除文件的路径
         * boolean recursive ： 是否递归
         */
        boolean res = fileSystem.delete(new Path("/demo"), true);
        System.out.println(res);
    }

}
