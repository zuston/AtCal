package io.github.zuston.test;

import io.github.zuston.basic.Trace.OriginalTraceRecordParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by zuston on 2018/1/17.
 */
public class OutputTest {
    public static void main(String[] args) throws IOException {
        OriginalTraceRecordParser parser = new OriginalTraceRecordParser();
        FileReader reader = new FileReader("/Users/zuston/Downloads/part-r-00000 (3)");

        BufferedReader br = new BufferedReader(reader);

        String str = null;

        while ((str = br.readLine()) != null) {
            parser.parser(str.split("\\t+")[1]);
            System.out.println(parser.getSCAN_TIME().split("\\s+")[0].split("-")[0]);
        }

        br.close();
        reader.close();
    }
}
