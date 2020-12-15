package com.xk.bigata.hadoop.mapreduce.dataskew;

import com.xk.bigata.hadoop.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;

public class DataSkewDriver {

    public static void main(String[] args) throws Exception {
        String input = "mapreduce-basic/data/wc.txt";
        String midput = "mapreduce-basic/out/midput";
        String output = "mapreduce-basic/out/output";

        // Job 1

        // 1 创建 MapReduce job
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf);

        // 删除输出路径
        FileUtils.deleteFile(job1.getConfiguration(), midput);
        FileUtils.deleteFile(job1.getConfiguration(), output);

        // 2 设置运行主类
        job1.setJarByClass(DataSkewDriver.class);

        // 3 设置Map和Reduce运行的类
        job1.setMapperClass(MyMapper1.class);
        job1.setReducerClass(MyReducer.class);

        // 4 设置Map 输出的 KEY 和 VALUE 数据类型
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        // 5 设置Reduce 输出 KEY 和 VALUE 数据类型
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(midput));

        //job1加入控制器
        ControlledJob ctrlJob1 = new ControlledJob(conf);
        ctrlJob1.setJob(job1);

        // Job 2

        // 1 创建 MapReduce job
        Job job2 = Job.getInstance(conf);

        // 2 设置运行主类
        job2.setJarByClass(DataSkewDriver.class);

        // 3 设置Map和Reduce运行的类
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer.class);

        // 4 设置Map 输出的 KEY 和 VALUE 数据类型
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        // 5 设置Reduce 输出 KEY 和 VALUE 数据类型
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job2, new Path(midput));
        FileOutputFormat.setOutputPath(job2, new Path(output));

        //job2加入控制器
        ControlledJob ctrlJob2 = new ControlledJob(conf);
        ctrlJob2.setJob(job2);

        //设置作业之间的以来关系，job2的输入以来job1的输出
        ctrlJob2.addDependingJob(ctrlJob1);

        //设置主控制器，控制job1和job2两个作业
        JobControl jobCtrl = new JobControl("myCtrl");
        //添加到总的JobControl里，进行控制
        jobCtrl.addJob(ctrlJob1);
        jobCtrl.addJob(ctrlJob2);

        //在线程中启动，记住一定要有这个
        Thread thread = new Thread(jobCtrl);
        thread.start();
        while (true) {
            if (jobCtrl.allFinished()) {
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                break;
            }
        }
    }

    public static class MyMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Random random = new Random();
            String[] spilts = value.toString().split(",");
            for (String word : spilts) {
                int num = random.nextInt(10);
                context.write(new Text(num + "-" + word), ONE);
            }
        }
    }

    public static class MyMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] spilts = value.toString().split("\t");
            String[] wordSpilt = spilts[0].split("-");
            context.write(new Text(wordSpilt[1]), new IntWritable(Integer.parseInt(spilts[1])));
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
