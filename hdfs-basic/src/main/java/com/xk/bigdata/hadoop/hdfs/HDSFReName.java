package com.xk.bigdata.hadoop.hdfs;

import com.xk.bigdata.hadoop.utils.HDFSUtils;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

public class HDSFReName {

    /**
     * 1. 通过路径读取下面层级文件的全部路径
     * 2. 修改文件路径
     * 3. 删除原文件夹路径
     */
    public static void reName(String pathString) {
        HDFSUtils hdfsUtils = new HDFSUtils();
        try {
            hdfsUtils.stepUp();
            RemoteIterator<LocatedFileStatus> files = hdfsUtils.listFiles(pathString);
            while (files.hasNext()) {
                LocatedFileStatus file = files.next();
                String filePath = file.getPath().toString();
                String newFilePath = filePath.substring(0, filePath.lastIndexOf("/")) + "-" + filePath.substring(filePath.lastIndexOf("/") + 1);
                boolean res = hdfsUtils.reName(filePath, newFilePath);
                System.out.println(res);
            }
            System.out.println(hdfsUtils.delete(pathString));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                hdfsUtils.cleanUp();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        String pathString = "/bigdata/hdfs-works/20211001";
        reName(pathString);
    }

}
