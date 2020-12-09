package com.xk.bigata.hadoop.mapreduce.domain;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DeptWritable implements Writable {

    private Integer deptNo;

    private String dName;

    private String dMessage;

    public DeptWritable() {
    }

    @Override
    public String toString() {
        return deptNo + "\t" + dName + "\t" + dMessage;
    }

    public DeptWritable(Integer deptNo, String dName, String dMessage) {
        this.deptNo = deptNo;
        this.dName = dName;
        this.dMessage = dMessage;
    }

    public int getDeptNo() {
        return deptNo;
    }

    public void setDeptNo(int deptNo) {
        this.deptNo = deptNo;
    }

    public String getdName() {
        return dName;
    }

    public void setdName(String dName) {
        this.dName = dName;
    }

    public String getdMessage() {
        return dMessage;
    }

    public void setdMessage(String dMessage) {
        this.dMessage = dMessage;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(deptNo);
        out.writeUTF(dName);
        out.writeUTF(dMessage);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.deptNo = in.readInt();
        this.dName = in.readUTF();
        this.dMessage = in.readUTF();
    }
}
