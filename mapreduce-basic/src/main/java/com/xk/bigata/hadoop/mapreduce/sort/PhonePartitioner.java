package com.xk.bigata.hadoop.mapreduce.sort;

import com.xk.bigata.hadoop.mapreduce.domain.AccessAllSortDomain;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PhonePartitioner extends Partitioner<AccessAllSortDomain, Text> {

    @Override
    public int getPartition(AccessAllSortDomain accessAllSortDomain, Text text, int numPartitions) {
        String phone = accessAllSortDomain.getPhone();
        if (phone.startsWith("13")) {
            return 0;
        } else if (phone.startsWith("18")) {
            return 1;
        } else {
            return 2;
        }
    }
}
