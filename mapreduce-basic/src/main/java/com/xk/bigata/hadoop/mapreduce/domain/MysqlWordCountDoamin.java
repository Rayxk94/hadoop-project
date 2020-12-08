package com.xk.bigata.hadoop.mapreduce.domain;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MysqlWordCountDoamin implements Writable, DBWritable {

    private String word;

    private int cnt;

    public MysqlWordCountDoamin() {
    }

    public MysqlWordCountDoamin(String word, int cnt) {
        this.word = word;
        this.cnt = cnt;
    }

    @Override
    public String toString() {
        return "MysqlWordCountDoamin{" +
                "word='" + word + '\'' +
                ", cnt=" + cnt +
                '}';
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCnt() {
        return cnt;
    }

    public void setCnt(int cnt) {
        this.cnt = cnt;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeInt(cnt);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word = in.readUTF();
        cnt = in.readInt();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1, word);
        statement.setInt(2, cnt);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        word = resultSet.getString(1);
        cnt = resultSet.getInt(2);
    }
}
