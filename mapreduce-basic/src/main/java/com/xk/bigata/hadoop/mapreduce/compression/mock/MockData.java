package com.xk.bigata.hadoop.mapreduce.compression.mock;

import java.io.*;
import java.util.Random;

public class MockData {

    public static void main(String[] args) {

        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("mapreduce-basic/data/compression.data")));
            String[] words = new String[]{"hadoop", "flink", "hbase", "spark", "kafka"};
            for (int i = 0; i < 1000000; i++) {
                String line = "";
                for (int j = 0; j < 30; j++) {
                    line = line + words[new Random().nextInt(words.length)] + ",";
                }
                line += "\n";
                writer.write(line);
                writer.flush();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != writer) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
