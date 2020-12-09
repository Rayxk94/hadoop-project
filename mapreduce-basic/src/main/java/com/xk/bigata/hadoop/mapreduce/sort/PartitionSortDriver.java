package com.xk.bigata.hadoop.mapreduce.sort;

import com.xk.bigata.hadoop.mapreduce.domain.AccessAllSortDomain;
import com.xk.bigata.hadoop.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PartitionSortDriver {

    public static void main(String[] args) throws Exception {

        String input = "mapreduce-basic/data/sort.data";
        String output = "mapreduce-basic/out";

        // 1 创建 MapReduce job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置自定义分区类
        job.setPartitionerClass(PhonePartitioner.class);

        // 删除输出路径
        FileUtils.deleteFile(job.getConfiguration(), output);

        // 2 设置运行主类
        job.setJarByClass(PartitionSortDriver.class);

        // 3 设置Map和Reduce运行的类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 4 设置Map 输出的 KEY 和 VALUE 数据类型
        job.setMapOutputKeyClass(AccessAllSortDomain.class);
        job.setMapOutputValueClass(Text.class);

        // 5 设置Reduce 输出 KEY 和 VALUE 数据类型
        job.setOutputKeyClass(AccessAllSortDomain.class);
        job.setOutputValueClass(NullWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 设置 reduce task 个数
        job.setNumReduceTasks(3);

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    public static class MyMapper extends Mapper<LongWritable, Text, AccessAllSortDomain, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] spilts = value.toString().split("\t");
            String phone = spilts[0];
            long up = Long.parseLong(spilts[1]);
            long down = Long.parseLong(spilts[2]);
            long sum = Long.parseLong(spilts[3]);
            context.write(new AccessAllSortDomain(phone, up, down, sum), new Text(phone));
        }
    }

    public static class MyReducer extends Reducer<AccessAllSortDomain, Text, AccessAllSortDomain, NullWritable> {
        @Override
        protected void reduce(AccessAllSortDomain key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, NullWritable.get());
            }
        }
    }
}