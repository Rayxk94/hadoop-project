package com.xk.bigdata.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.net.URI;

public class HDFSUtils {

    FileSystem fileSystem = null;

    /**
     * 文件系统初始化
     */
    public void stepUp() throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        URI uri = new URI(FinalCode.HDFS_URI);
        fileSystem = FileSystem.get(uri, conf, FinalCode.HDFS_USER_NAME);
    }

    /**
     * 关闭文件系统
     */
    public void cleanUp() throws Exception {
        if (null != fileSystem) {
            fileSystem.close();
        }
    }

    /**
     * 创建文件夹
     *
     * @param path : 需要创建的目录
     */
    public boolean mkdir(String path) throws Exception {
        return fileSystem.mkdirs(new Path(path));
    }

    /**
     * 从本地上传文件
     *
     * @param srcPath ： 文件地址
     * @param dstPath ： 目标地址
     */
    public void copyFromLocalFile(String srcPath, String dstPath) throws Exception {
        // 文件地址
        Path src = new Path(srcPath);
        // 目标地址
        Path dst = new Path(dstPath);
        fileSystem.copyFromLocalFile(src, dst);
    }

    /**
     * 从HDFS下载文件
     *
     * @param srcPath ： 文件地址
     * @param dstPath ： 目标地址
     */
    public void copyToLocalFile(String srcPath, String dstPath) throws Exception {
        // HDFS路径
        Path src = new Path(srcPath);
        // 本地路径
        Path dst = new Path(dstPath);
        fileSystem.copyToLocalFile(src, dst);
    }

    /**
     * 修改名称
     *
     * @param oldPath : 原路径
     * @param newPath ： 新的路径
     */
    public boolean reName(String oldPath, String newPath) throws Exception {
        // 原文件路径
        Path src = new Path(oldPath);
        // 新文件路径
        Path dst = new Path(newPath);
        return fileSystem.rename(src, dst);
    }

    /**
     * 文件列表
     *
     * @param pathString : 路径
     * @param recursive  ： 是否递归
     */
    public RemoteIterator<LocatedFileStatus> listFiles(String pathString, Boolean recursive) throws Exception {
        return fileSystem.listFiles(new Path(pathString), recursive);
    }

    /**
     * 文件列表
     *
     * @param pathString : 路径
     */
    public RemoteIterator<LocatedFileStatus> listFiles(String pathString) throws Exception {
        return fileSystem.listFiles(new Path(pathString), true);
    }

    /**
     * 删除文件或者文件夹
     *
     * @param pathString : 删除文件的路径
     * @param recursive  : 是否递归
     */
    public boolean delete(String pathString, boolean recursive) throws Exception {
        return fileSystem.delete(new Path(pathString), recursive);
    }

    /**
     * 删除文件
     *
     * @param pathString : 删除文件的路径
     */
    public boolean delete(String pathString) throws Exception {
        return fileSystem.delete(new Path(pathString), true);
    }

    /**
     * 判断路径是否存在
     *
     * @param pathString ： 路径
     */
    public boolean isExist(String pathString) throws Exception {
        return fileSystem.exists(new Path(pathString));
    }

}
