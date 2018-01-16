package io.github.zuston.example;

import io.github.zuston.basic.Trace.OriginalTraceRecordParser;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by zuston on 2017/12/20.
 */
public class Test {
    public static void main(String[] args) {

        OriginalTraceRecordParser parser = new OriginalTraceRecordParser();
        String line = "15447#520723\t3029634580#30000030146671#70#15447#9931201#兰州分拨中心#20#兰州市#348866#苟茂校#2017-11-14 00:14:58##兰州城关九部#9311006###【兰州市】兰州分拨中心已发出,下一站兰州城关九部##2017-11-14 00:33:02##七里河区#5.72##0#兰州市#1#";

        parser.parser(line.split("\\t+")[1]);
        String scanTime = parser.getSCAN_TIME();
        String [] arr = scanTime.split("\\s+")[0].split("-");
        String frontSuffix = arr[0]+"-"+arr[1];
        System.out.println(frontSuffix);
        System.exit(1);

        String time1 = "2018-08-06 00:00:00";
        String time2 = "2018-08-07 00:00:00";
        System.out.println(Timestamp.valueOf(time2).getTime()-Timestamp.valueOf(time1).getTime());
        System.exit(1);

        String text = "3028390940" +
                "#57999900309779" +
                "#50" +
                "#15287" +
                "#sw0001" +
                "#赣州分拨中心" +
                "#20" +
                "#赣州市" +
                "#" +
                "#施令" +
                "#2017-11-12 17:16:30" +
                "#" +
                "#赣州章贡五部" +
                "#sw20395" +
                "#" +
                "#" +
                "#【赣州市】赣州分拨中心已发出,下一站赣州章贡五部" +
                "#1" +
                "#2017-11-14 00:00:00" +
                "#" +
                "#赣县" +
                "#" +
                "#" +
                "#0" +
                "#赣州市" +
                "#1" +
                "#";

        String text1 = "南昌市#1##哈哈";
        System.out.println(text1.split("#").length);
        for (String v:text1.split("#")){
            System.out.println(v);
        }
        System.exit(1);
//        TraceRecordParser parser = new TraceRecordParser();
//        parser.parse("3028390941#59999906490365#20#15371#sw0001#昆山分拨中心#20#苏州市##曹令#2017-11-12 17:16:30######【苏州市】昆山分拨中心已到达#1#2017-11-14 00:00:00##昆山市###0##1#");
//        System.out.println(parser.getEwb_no());
//        System.out.println(parser.getScan_time());
//        System.out.println(parser.getSite_name());
//        System.out.println(parser.getTrace_id());

//        String time = "2017-11-14 01:14:02";
//        System.out.println(time.substring(0,7));

        double a = 10;
        System.out.println(Math.sqrt(1340028));

        List<Integer> list = new ArrayList<Integer>();
        list.add(12);
        list.add(34);
        list.add(355);
        Iterator<Integer> vv  = list.listIterator();

//        576705     column=December:576705_AVG, timestamp=1514875593598, value=927.0
//        576705     column=December:576705_SAMPLE, timestamp=1514875593598, value=2028
//        576705     column=December:576705_VAR, timestamp=1514875593598, value=1340028
//
//                拿这一条方差较大的数据来说，按照 95% 的可信区间，得出 2028+-50.4


        System.out.println(String.format("%s:sdsdsd","hello world"));
    }
}
