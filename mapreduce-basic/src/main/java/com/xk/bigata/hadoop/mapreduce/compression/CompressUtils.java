package com.xk.bigata.hadoop.mapreduce.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class CompressUtils {

    public static void main(String[] args) throws Exception {
        compress("mapreduce-basic/data/compression.data", "org.apache.hadoop.io.compress.BZip2Codec");
        decompression("mapreduce-basic/data/compression.data.bz2");
    }

    /**
     * 压缩文件
     *
     * @param fileName 文件名
     * @param codeC    压缩格式
     */
    public static void compress(String fileName, String codeC) throws Exception {
        FileInputStream fis = new FileInputStream(new File(fileName));
        Class<?> codecClass = Class.forName(codeC);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, new Configuration());
        FileOutputStream fos = new FileOutputStream(new File(fileName + codec.getDefaultExtension()));
        CompressionOutputStream cos = codec.createOutputStream(fos);
        IOUtils.copyBytes(fis, cos, 1024 * 1024 * 5);
        cos.close();
        fos.close();
        fis.close();
    }

    /**
     * 解压文件
     *
     * @param fileName
     * @throws Exception
     */
    public static void decompression(String fileName) throws Exception {
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(fileName));
        if (null == codec) {
            System.out.println("找不到codec：" + codec.getDefaultExtension());
            return;
        }
        CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(fileName)));
        FileOutputStream fos = new FileOutputStream(new File(fileName + ".bak"));
        IOUtils.copyBytes(cis, fos, 1024 * 1024 * 5);
        fos.close();
        cis.close();
    }
}