package com.xk.bigata.hadoop.mapreduce.top;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TopNEmpWritable implements Writable {

    private int empNo;

    private String eName;

    private int deptNo;

    private double wages;

    @Override
    public String toString() {
        return empNo + "\t" + eName + "\t" + deptNo + "\t" + wages;
    }

    public TopNEmpWritable() {
    }

    public TopNEmpWritable(int empNo, String eName, int deptNo, double wages) {
        this.empNo = empNo;
        this.eName = eName;
        this.deptNo = deptNo;
        this.wages = wages;
    }

    public int getEmpNo() {
        return empNo;
    }

    public void setEmpNo(int empNo) {
        this.empNo = empNo;
    }

    public String geteName() {
        return eName;
    }

    public void seteName(String eName) {
        this.eName = eName;
    }

    public int getDeptNo() {
        return deptNo;
    }

    public void setDeptNo(int deptNo) {
        this.deptNo = deptNo;
    }

    public double getWages() {
        return wages;
    }

    public void setWages(double wages) {
        this.wages = wages;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(empNo);
        out.writeUTF(eName);
        out.writeInt(deptNo);
        out.writeDouble(wages);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.empNo = in.readInt();
        this.eName = in.readUTF();
        this.deptNo = in.readInt();
        this.wages = in.readDouble();
    }
}
