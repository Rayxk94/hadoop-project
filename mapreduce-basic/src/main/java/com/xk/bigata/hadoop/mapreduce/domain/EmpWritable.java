package com.xk.bigata.hadoop.mapreduce.domain;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EmpWritable implements Writable {

    // empno,ename,deptno

    private int empNo;

    private String eName;

    private int deptNo;

    @Override
    public String toString() {
        return empNo + "\t" + eName + "\t" + deptNo;
    }

    public EmpWritable() {
    }

    public EmpWritable(int empNo, String eName, int deptNo) {
        this.empNo = empNo;
        this.eName = eName;
        this.deptNo = deptNo;
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

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(empNo);
        out.writeUTF(eName);
        out.writeInt(deptNo);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.empNo = in.readInt();
        this.eName = in.readUTF();
        this.deptNo = in.readInt();
    }
}
