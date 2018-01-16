package io.github.zuston.example;

/**
 * Created by zuston on 2018/1/16.
 */
public class TaskTest {
    public static void main(String[] args) {
        String time1 = "2017-11-16 09:34:55";
        String time2 = "2017-11-10 00:00:00";
//
//        String line = "3028390940#57999900309779#50#15287#sw0001#赣州分拨中心#20#赣州市##施令#2017-11-12 17:16:30##赣州章贡五部#sw20395###【赣州市】赣州分拨中心已发出,下一站赣州章贡五部#1#2017-11-14 00:00:00##赣县###0#赣州市#1#";
//        OriginalTraceRecordParser parser = new OriginalTraceRecordParser();
//        parser.parser(line);
//        System.out.println(parser.getSCAN_TIME());

        long t1 = Long.valueOf("1510520698000");
        long t2 = Long.valueOf("1510243200000");
        System.out.println(activeTraceFilter(t1));
    }


    static boolean activeTraceFilter(long timestamp) {
        long thresholdSeconds = 7 * 24 * 60 * 60;
        long minplusV = Math.abs(Long.valueOf("1510243200000")-timestamp) / 1000;
        if (minplusV > thresholdSeconds)    return false;
        return true;
    }
}
