package com.xk.bigata.hadoop.mapreduce.join;

import com.xk.bigata.hadoop.mapreduce.domain.EmpInfoReduceJoinWritable;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReduceJoinDriver {

    public static void main(String[] args) throws Exception {

        String input = "mapreduce-basic/data/join";
        String output = "mapreduce-basic/out";

        // 1 创建 MapReduce job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 删除输出路径
        FileUtils.deleteFile(job.getConfiguration(), output);

        // 2 设置运行主类
        job.setJarByClass(ReduceJoinDriver.class);

        // 3 设置Map和Reduce运行的类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);

        // 4 设置Map 输出的 KEY 和 VALUE 数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(EmpInfoReduceJoinWritable.class);

        // 5 设置Reduce 输出 KEY 和 VALUE 数据类型
        job.setOutputKeyClass(EmpInfoReduceJoinWritable.class);
        job.setOutputValueClass(NullWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, EmpInfoReduceJoinWritable> {

        private String fileName;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 确定读取的是哪一个文件
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            fileName = fileSplit.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (fileName.contains("dept")) {
                // 进入的数据是dept
                String[] spits = value.toString().split("\t");
                EmpInfoReduceJoinWritable empInfoReduceJoinWritable = new EmpInfoReduceJoinWritable();
                empInfoReduceJoinWritable.setDeptNo(Integer.parseInt(spits[0]));
                empInfoReduceJoinWritable.setdName(spits[1]);
                empInfoReduceJoinWritable.setdMessage(spits[2]);
                empInfoReduceJoinWritable.setEmpNo(0);
                empInfoReduceJoinWritable.setFlg("0");
                empInfoReduceJoinWritable.seteName("");
                context.write(new Text(spits[0]), empInfoReduceJoinWritable);
            } else if (fileName.contains("emp")) {
                // 进来的数据是emp
                String[] splits = value.toString().split("\t");
                EmpInfoReduceJoinWritable empInfoReduceJoinWritable = new EmpInfoReduceJoinWritable();
                empInfoReduceJoinWritable.setEmpNo(Integer.parseInt(splits[0]));
                empInfoReduceJoinWritable.seteName(splits[1]);
                empInfoReduceJoinWritable.setDeptNo(Integer.parseInt(splits[2]));
                empInfoReduceJoinWritable.setFlg("1");
                empInfoReduceJoinWritable.setdMessage("");
                empInfoReduceJoinWritable.setdName("");
                context.write(new Text(splits[2]), empInfoReduceJoinWritable);
            }
        }
    }

    public static class MyReduce extends Reducer<Text, EmpInfoReduceJoinWritable, EmpInfoReduceJoinWritable, NullWritable> {
        // 相同的deptno(key)所对应的emp 和 dept的数据都落在了values
        @Override
        protected void reduce(Text key, Iterable<EmpInfoReduceJoinWritable> values, Context context) throws IOException, InterruptedException {
            List<EmpInfoReduceJoinWritable> emps = new ArrayList<>();
            String dName = null;
            String dMessage = null;
            for (EmpInfoReduceJoinWritable emp : values) {
                if (emp.getFlg().equals("1")) {
                    // 数据来自emp
                    EmpInfoReduceJoinWritable empInfoReduceJoinWritable = new EmpInfoReduceJoinWritable();
                    empInfoReduceJoinWritable.setEmpNo(emp.getEmpNo());
                    empInfoReduceJoinWritable.seteName(emp.geteName());
                    empInfoReduceJoinWritable.setDeptNo(emp.getDeptNo());
                    emps.add(empInfoReduceJoinWritable);
                } else {
                    // 数据来自dept
                    dName = emp.getdName();
                    dMessage = emp.getdMessage();
                }
            }

            for (EmpInfoReduceJoinWritable bean : emps) {
                bean.setdName(dName);
                bean.setdMessage(dMessage);
                context.write(bean, NullWritable.get());
            }

        }
    }

}
