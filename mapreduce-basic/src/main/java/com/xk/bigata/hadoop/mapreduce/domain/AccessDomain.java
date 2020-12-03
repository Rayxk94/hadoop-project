package com.xk.bigata.hadoop.mapreduce.domain;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义数据类型步骤：
 * 1. 实现WritableComparable/Writable接口
 * 2. 重写 write、readFields、compareTo方法
 * 3. 要有一个无参的构造器
 * 4. write和readFields的字段顺序要保持一致
 * 5. 输出结果：需要重写toString(非必选)
 */
public class AccessDomain implements WritableComparable<AccessDomain> {

    private String phone;

    private Long up;

    private Long down;

    private Long sum;

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Long getUp() {
        return up;
    }

    public void setUp(Long up) {
        this.up = up;
    }

    public Long getDown() {
        return down;
    }

    public void setDown(Long down) {
        this.down = down;
    }

    public Long getSum() {
        return sum;
    }

    public void setSum(Long sum) {
        this.sum = sum;
    }

    public AccessDomain() {
    }

    public AccessDomain(String phone, Long up, Long down) {
        this.phone = phone;
        this.up = up;
        this.down = down;
        this.sum = up + down;
    }

    @Override
    public String toString() {
        return phone + '\t' +
               up + '\t' +
               down + '\t' +
               sum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        phone = in.readUTF();
        up = in.readLong();
        down = in.readLong();
        sum = in.readLong();
    }

    @Override
    public int compareTo(AccessDomain o) {
        return Integer.parseInt(String.valueOf((o.sum - sum)));
    }
}
