package com.xk.bigata.hadoop.mapreduce.compression;

import com.xk.bigata.hadoop.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CompressionDirver {

    public static void main(String[] args) throws Exception {
        String input = "mapreduce-basic/data/wc.txt.bz2";
        String output = "mapreduce-basic/out";

        // 1 创建 MapReduce job
        Configuration conf = new Configuration();
        // 开启Map端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);
        // 指定Map端压缩格式
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
        // 开启Reduce端输出压缩
        conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
        // 指定Reduce端压缩格式
        conf.setClass("mapreduce.output.fileoutputformat.compress.codec", BZip2Codec.class, CompressionCodec.class);
        Job job = Job.getInstance(conf);

        // 删除输出路径
        FileUtils.deleteFile(job.getConfiguration(), output);

        // 2 设置运行主类
        job.setJarByClass(CompressionDirver.class);

        // 3 设置Map和Reduce运行的类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 4 设置Map 输出的 KEY 和 VALUE 数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置Reduce 输出 KEY 和 VALUE 数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] spilts = value.toString().split(",");
            for (String word : spilts) {
                context.write(new Text(word), ONE);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

}
