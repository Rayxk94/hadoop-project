package com.xk.bigata.hadoop.mapreduce.top;

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

public class GroupTopDriver {

    public static void main(String[] args) throws Exception {

        String input = "mapreduce-basic/data/emp.data";
        String output = "mapreduce-basic/out";

        // 1 创建 MapReduce job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 删除输出路径
        FileUtils.deleteFile(job.getConfiguration(), output);

        // 2 设置运行主类
        job.setJarByClass(GroupTopDriver.class);

        // 3 设置Map和Reduce运行的类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 4 设置Map 输出的 KEY 和 VALUE 数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TopNEmpWritable.class);

        // 5 设置Reduce 输出 KEY 和 VALUE 数据类型
        job.setOutputKeyClass(TopNEmpWritable.class);
        job.setOutputValueClass(NullWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, TopNEmpWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] spilts = value.toString().split("\t");
            TopNEmpWritable topNEmpWritable = new TopNEmpWritable(Integer.parseInt(spilts[0]),
                    spilts[1],
                    Integer.parseInt(spilts[7]),
                    Double.parseDouble(spilts[5]));
            context.write(new Text(spilts[7]), topNEmpWritable);
        }
    }

    public static class MyReducer extends Reducer<Text, TopNEmpWritable, TopNEmpWritable, NullWritable> {
        int topN = 2;

        @Override
        protected void reduce(Text key, Iterable<TopNEmpWritable> values, Context context) throws IOException, InterruptedException {
            int index = 1;
            for (TopNEmpWritable value : values) {
                index++;
                context.write(value, NullWritable.get());
                if (index > topN) {
                    break;
                }
            }
        }
    }
}
