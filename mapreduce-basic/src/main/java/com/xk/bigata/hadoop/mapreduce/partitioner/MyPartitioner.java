package com.xk.bigata.hadoop.mapreduce.partitioner;

import com.xk.bigata.hadoop.mapreduce.domain.AccessDomain;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, AccessDomain> {
    @Override
    public int getPartition(Text text, AccessDomain accessDomain, int numPartitions) {
        String phone = text.toString();
        if (phone.startsWith("13")) {
            return 0;
        } else if (phone.startsWith("18")) {
            return 1;
        } else {
            return 2;
        }
    }
}
