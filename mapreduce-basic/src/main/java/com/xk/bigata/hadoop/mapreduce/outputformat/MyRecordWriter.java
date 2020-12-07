package com.xk.bigata.hadoop.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MyRecordWriter extends RecordWriter<Text, IntWritable> {

    FileSystem fs = null;
    FSDataOutputStream baiduOut = null;
    FSDataOutputStream qqOut = null;

    public MyRecordWriter(TaskAttemptContext job) {
        try {
            fs = FileSystem.get(job.getConfiguration());
            baiduOut = fs.create(new Path("mapreduce-basic/out/www.baidu.com.log"));
            qqOut = fs.create(new Path("mapreduce-basic/out/www.qq.com.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, IntWritable value) throws IOException, InterruptedException {
        String domain = key.toString();
        if (domain.equals("www.baidu.com")) {
            baiduOut.write((key.toString() + "\t" + value.toString()).getBytes());
        } else if (domain.equals("www.qq.com")) {
            qqOut.write((key.toString() + "\t" + value.toString()).getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (null != baiduOut) {
            IOUtils.closeStream(baiduOut);
        }
        if (null != qqOut) {
            IOUtils.closeStream(qqOut);
        }
        if (null != fs) {
            fs.close();
        }
    }
}
