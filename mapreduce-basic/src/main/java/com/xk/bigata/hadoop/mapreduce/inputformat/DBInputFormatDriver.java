package com.xk.bigata.hadoop.mapreduce.inputformat;

import com.xk.bigata.hadoop.mapreduce.domain.MysqlTestDomain;
import com.xk.bigata.hadoop.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DBInputFormatDriver {

    public static void main(String[] args) throws Exception {
        String output = "mapreduce-basic/out";

        // 1 创建 MapReduce job
        Configuration conf = new Configuration();
        // 设置JDBC连接
        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://bigdatatest01:3306/bigdata",
                "root",
                "Jgw@31500");
        Job job = Job.getInstance(conf);

        // 删除输出路径
        FileUtils.deleteFile(job.getConfiguration(), output);

        // 2 设置运行主类
        job.setJarByClass(NLineInputFormatDriver.class);

        // 3 设置Map和Reduce运行的类
        job.setMapperClass(MyMapper.class);

        // 4 设置Map 输出的 KEY 和 VALUE 数据类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(MysqlTestDomain.class);


        // 6 设置输入和输出路径
        job.setInputFormatClass(DBInputFormat.class);
        // 设置Mysql中的详细信息
        DBInputFormat.setInput(job,
                MysqlTestDomain.class,
                "test",
                null,
                null,
                "id", "name");
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    public static class MyMapper extends Mapper<LongWritable, MysqlTestDomain, NullWritable, MysqlTestDomain> {

        @Override
        protected void map(LongWritable key, MysqlTestDomain value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), value);
        }
    }

}
