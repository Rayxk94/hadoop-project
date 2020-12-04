package com.xk.bigata.hadoop.mapreduce.inputformat;

import com.xk.bigata.hadoop.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KeyValueTextInputFormatDriver {

    public static void main(String[] args) throws Exception {

        String input = "mapreduce-basic/data/wc.txt";
        String output = "mapreduce-basic/out";

        // 1 创建 MapReduce job
        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, ",");
        Job job = Job.getInstance(conf);

        // 删除输出路径
        FileUtils.deleteFile(job.getConfiguration(), output);

        // 2 设置运行主类
        job.setJarByClass(KeyValueTextInputFormatDriver.class);

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
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

    public static class MyMapper extends Mapper<Text, Text, Text, IntWritable> {

        IntWritable ONE = new IntWritable(1);

        /**
         * @param key 这一行数据里面，按照设定的分区符号的第一个字段
         * @param value 这一行数据
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, ONE);
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
