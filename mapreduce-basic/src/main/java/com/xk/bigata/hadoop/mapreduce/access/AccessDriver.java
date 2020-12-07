package com.xk.bigata.hadoop.mapreduce.access;

import com.xk.bigata.hadoop.mapreduce.domain.AccessDomain;
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

public class AccessDriver {

    public static void main(String[] args) throws Exception {

        String input = "mapreduce-basic/data/access.data";
        String output = "mapreduce-basic/out";

        // 1 创建 MapReduce job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 删除输出路径
        FileUtils.deleteFile(job.getConfiguration(), output);

        // 2 设置运行主类
        job.setJarByClass(AccessDriver.class);

        // 3 设置Map和Reduce运行的类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 4 设置Map 输出的 KEY 和 VALUE 数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AccessDomain.class);

        // 5 设置Reduce 输出 KEY 和 VALUE 数据类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(AccessDomain.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, AccessDomain> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] spilts = value.toString().split("\t");
            String phone = spilts[1];
            long up = Long.parseLong(spilts[spilts.length - 3]);
            long down = Long.parseLong(spilts[spilts.length - 2]);
            context.write(new Text(phone), new AccessDomain(phone, up, down));
        }
    }

    public static class MyReducer extends Reducer<Text, AccessDomain, NullWritable, AccessDomain> {
        @Override
        protected void reduce(Text key, Iterable<AccessDomain> values, Context context) throws IOException, InterruptedException {
            Long up = 0L;
            Long down = 0L;
            for (AccessDomain access : values) {
                up += access.getUp();
                down += access.getDown();
            }
            context.write(NullWritable.get(), new AccessDomain(key.toString(), up, down));
        }
    }

}
