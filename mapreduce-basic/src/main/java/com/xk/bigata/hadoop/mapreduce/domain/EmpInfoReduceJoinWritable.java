package com.xk.bigata.hadoop.mapreduce.domain;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EmpInfoReduceJoinWritable implements Writable {

    private int empNo;

    private String eName;

    private int deptNo;

    private String dName;

    private String dMessage;

    private String flg;

    @Override
    public String toString() {
        return empNo + "\t" + eName + "\t" +
                deptNo + "\t" + dName + "\t" +
                dMessage;
    }

    public EmpInfoReduceJoinWritable(int empNo, String eName, int deptNo, String dName, String dMessage, String flg) {
        this.empNo = empNo;
        this.eName = eName;
        this.deptNo = deptNo;
        this.dName = dName;
        this.dMessage = dMessage;
        this.flg = flg;
    }

    public EmpInfoReduceJoinWritable() {
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

    public String getFlg() {
        return flg;
    }

    public void setFlg(String flg) {
        this.flg = flg;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(empNo);
        out.writeUTF(eName);
        out.writeInt(deptNo);
        out.writeUTF(dName);
        out.writeUTF(dMessage);
        out.writeUTF(flg);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.empNo = in.readInt();
        this.eName = in.readUTF();
        this.deptNo = in.readInt();
        this.dName = in.readUTF();
        this.dMessage = in.readUTF();
        this.flg = in.readUTF();
    }
}
