package com.xk.bigata.hadoop.mapreduce.join;

import com.xk.bigata.hadoop.mapreduce.domain.AccessDomain;
import com.xk.bigata.hadoop.mapreduce.domain.DeptWritable;
import com.xk.bigata.hadoop.mapreduce.domain.EmpInfoWritable;
import com.xk.bigata.hadoop.utils.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MapJoinDriver {

    public static void main(String[] args) throws Exception {

        String input = "mapreduce-basic/data/join/emp.txt";
        String output = "mapreduce-basic/out";

        // 1 创建 MapReduce job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 删除输出路径
        FileUtils.deleteFile(job.getConfiguration(), output);

        // 2 设置运行主类
        job.setJarByClass(MapJoinDriver.class);

        // 3 设置Map和Reduce运行的类
        job.setMapperClass(MyMapper.class);

        // 4 设置Map 输出的 KEY 和 VALUE 数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AccessDomain.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 设置reduce task 数量
        job.setNumReduceTasks(0);

        // 缓存dept路径
        job.addCacheFile(new URI("mapreduce-basic/data/join/dept.txt"));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

    public static class MyMapper extends Mapper<LongWritable, Text, EmpInfoWritable, NullWritable> {

        Map<Integer, DeptWritable> cache = new HashMap<>();

        // 把文件放在缓存的map里面
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String deptPath = context.getCacheFiles()[0].getPath();
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(deptPath)));
            String message = null;
            while (!StringUtils.isEmpty(message = reader.readLine())) {
                String[] split = message.split("\t");
                cache.put(Integer.parseInt(split[0]), new DeptWritable(Integer.parseInt(split[0]), split[1], split[2]));
            }
            reader.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] spilts = value.toString().split("\t");
            int deptNo = Integer.parseInt(spilts[2]);
            DeptWritable deptWritable = cache.get(deptNo);
            // int empNo, String eName, int deptNo, String dName, String dMessage
            EmpInfoWritable empInfoWritable = new EmpInfoWritable(Integer.parseInt(spilts[0]), spilts[1], deptNo, deptWritable.getdName(), deptWritable.getdMessage());
            context.write(empInfoWritable, NullWritable.get());
        }
    }
}
