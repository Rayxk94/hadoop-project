package com.xk.bigata.hadoop.mapreduce.outputformat;

import com.xk.bigata.hadoop.mapreduce.domain.MysqlWordCountDoamin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class MysqlDBOutputFormat {

    public static void main(String[] args) throws Exception {
        String input = "mapreduce-basic/data/wc.txt";

        // 1 创建 MapReduce job
        Configuration conf = new Configuration();
        // 设置JDBC连接
        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://bigdatatest01:3306/bigdata",
                "root",
                "Jgw@31500");
        Job job = Job.getInstance(conf);

        // 2 设置运行主类
        job.setJarByClass(MysqlDBOutputFormat.class);

        // 3 设置Map和Reduce运行的类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 4 设置Map 输出的 KEY 和 VALUE 数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置Reduce 输出 KEY 和 VALUE 数据类型
        job.setOutputKeyClass(MysqlWordCountDoamin.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置输出类
        job.setOutputFormatClass(DBOutputFormat.class);
        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(input));
        DBOutputFormat.setOutput(job, "wc", "word", "cnt");
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

    public static class MyReducer extends Reducer<Text, IntWritable, MysqlWordCountDoamin, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            MysqlWordCountDoamin mysqlWordCountDoamin = new MysqlWordCountDoamin(key.toString(), count);
            context.write(mysqlWordCountDoamin, NullWritable.get());
        }
    }
}
