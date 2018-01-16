package io.github.zuston.example;

import io.github.zuston.basic.Trace.OriginalTraceRecordParser;

import java.sql.Timestamp;

/**
 * Created by zuston on 2018/1/16.
 */
public class TaskTest {
    public static void main(String[] args) {
        String time1 = "2017-11-16 09:34:55";
        String time2 = "2017-11-10 00:00:00";

        String line = "3028390940#57999900309779#50#15287#sw0001#赣州分拨中心#20#赣州市##施令#2017-11-12 17:16:30##赣州章贡五部#sw20395###【赣州市】赣州分拨中心已发出,下一站赣州章贡五部#1#2017-11-14 00:00:00##赣县###0#赣州市#1#";
        OriginalTraceRecordParser parser = new OriginalTraceRecordParser();
        parser.parser(line);
        System.out.println(parser.getSCAN_TIME());
        System.out.println(activeTraceFilter(Timestamp.valueOf(parser.getSCAN_TIME()).getTime()));
    }


    static boolean activeTraceFilter(long timestamp) {
        long thresholdSeconds = 7 * 24 * 60 * 60;
        long minplusV = Math.abs(Timestamp.valueOf("2017-11-10 00:00:00").getTime()-timestamp) / 1000;
        if (minplusV > thresholdSeconds)    return false;
        return true;
    }
}
