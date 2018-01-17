package io.github.zuston.task.ActiveTrace;

import io.github.zuston.basic.Trace.OriginalTraceRecordParser;
import io.github.zuston.Util.HdfsTool;
import io.github.zuston.Util.JobGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;


enum Counter {
    GET_RECORD_COUNT,
    HAVE_ARRIVED_RECORD_COUNT,
    SCAN_TIME_LATER_COUNT,
    DIRTY_DATA_COUNT,
    NAME_2_ID_SIZE,
    DESTINATION_NAME_NOT_EXIST_COUNT,

    FILTERED_DATA_COUNT,
    MAPPER_LINE_COUNT,

    DEBUG_TAG_COUNT,
}

/**
 * Created by zuston on 2018/1/15.
 */
public class FilterCurrentActiveTrace extends Configured implements Tool{
    public static Logger logger = LoggerFactory.getLogger(FilterCurrentActiveTrace.class);

    // 指定的当前日期。
    // for example : 12-08 11-23
    public static String settingDate;
    public static long settingDateTimestamp;
    // default value
    public static int threshold = 7;



    static class FilterMapper extends Mapper<LongWritable, Text, Text, Text>{

        private OriginalTraceRecordParser parser = new OriginalTraceRecordParser();

        private long settingDateTimestamp;
        private int threshold;

        @Override
        protected void setup(Context context) {
            settingDateTimestamp = Long.parseLong(context.getConfiguration().get("settingDateTimestamp"));
            threshold = Integer.parseInt(context.getConfiguration().get("threshold"));
        }


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!parser.parser(value.toString())) return;
            String scan_time = parser.getSCAN_TIME();
            long timestamp = Timestamp.valueOf(scan_time).getTime();
            if (!activeTraceFilter(timestamp))   {
                context.getCounter(Counter.DEBUG_TAG_COUNT).increment(1);
                return;
            }
            String ewbNo = parser.getEWB_NO();
            context.getCounter(Counter.GET_RECORD_COUNT).increment(1);
            context.write(new Text(ewbNo), value);
        }

        private boolean activeTraceFilter(long timestamp) {
            long thresholdSeconds = threshold * 24 * 60 * 60;
            long minplusV = Math.abs(settingDateTimestamp - timestamp) / 1000;
            if (minplusV > thresholdSeconds)    return false;
            return true;
        }
    }


    static class FilterReducer extends Reducer<Text, Text, Text, Text>{
        private OriginalTraceRecordParser parser = new OriginalTraceRecordParser();

        private HashMap<String, String> name2idMapper;

        private long settingDateTimestamp;

        public static final String mapperPath = "/site2nameMapper-1/part-r-00000";

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            if (name2idMapper==null){
                name2idMapper = initMapper(context.getConfiguration(), context);
//                context.getCounter("debug",name2idMapper.toString()).increment(1);
            }
            settingDateTimestamp = Long.parseLong(context.getConfiguration().get("settingDateTimestamp"));
        }

        @Override
        protected void cleanup(Context context){
            name2idMapper = null;
        }


        public HashMap<String,String> initMapper(Configuration config, Context context){
            try {
                HashMap<String,String> mapper = new HashMap<String, String>();
                List<String> lineList = HdfsTool.readFromHdfs(config, mapperPath);
//                context.getCounter(Counter.MAPPER_LINE_COUNT).setValue(lineList.size());

                for (String record : lineList){
                    String [] splitRecord = record.trim().split("\\s+");
                    if (splitRecord.length != 2)    continue;
                    String id = splitRecord[0];
                    String name = splitRecord[1];
                    mapper.put(name, id);
                }

//                context.getCounter(Counter.NAME_2_ID_SIZE).setValue(mapper.size());
                return mapper;

            } catch (IOException e) {
                logger.error("init site2name error, error : {}", e.getMessage());
                System.exit(1);
            }
            return null;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // filter 最小 scan_time 大于 current 的情况
            // 解析订单合理性，时间节点是否连贯
            // 判断订单是否已经送达
            Set<Text> recordList = new HashSet<Text>();
            long minScanTime = Long.MAX_VALUE;
            long maxScanTime = Long.MIN_VALUE;

            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()){
                Text record = iterator.next();
                parser.parser(record.toString());
                long scanTime = Timestamp.valueOf(parser.getSCAN_TIME()).getTime();
                minScanTime = minScanTime > scanTime ? scanTime : minScanTime;
                maxScanTime = maxScanTime > scanTime ? maxScanTime : scanTime;
                recordList.add(record);
            }

            // 不在设置时间的区间内，剔除
            if (minScanTime < (settingDateTimestamp+86400000) &&
                    maxScanTime > settingDateTimestamp){
                for (Text record : recordList){
                    parser.parser(record.toString());

                    String desp = parser.getDESCPT();
                    if (Timestamp.valueOf(parser.getSCAN_TIME()).getTime()==maxScanTime){
                        if (checkHaveArrived(desp)){
                            context.getCounter(Counter.HAVE_ARRIVED_RECORD_COUNT).increment(1);
                        }
                    }
                    String siteId = parser.getSITE_ID();
                    String destinationName = parser.getDEST_SITE_NAME();

                    // 针对数据的特殊情况，有的 destinationName 没有，但是 desp 只有 xxx已到达，证明自己流转

                    //3028390829#59999907067974#20#17814#sw0001#沧州分拨中心#20#沧州市##窦国安#2017-11-12 17:14:20######【沧州市】沧州分拨中心已到达#1#2017-11-14 00:00:00##沧县###0##1#
                    //3028390834#59999907077628#50#15148#sw0001#青浦分拨中心#20#上海市##邹旭#2017-11-12 17:14:20##萧山分拨中心#sw20395###【上海市】青浦分拨中心已发出,下一站萧山分拨中心#1#2017-11-14 00:00:00##青浦区###0#杭州市#1#
                    //3028390838#57999900635893#90#17168#sw0001#广州石井十部#10#广州市##褚风#2017-11-12 17:14:20######【广州市】广州石井十部快件已被签收，签收人是本人#1#2017-11-14 00:00:00##白云区###0##1#
                    //3028390990#57999900407946#80#16479#sw0001#青浦徐泾一部#10#上海市##施天#2017-11-12 17:14:27####施天#4001040088#【上海市】青浦徐泾一部快递员正在派件,联系电话4001040088#1#2017-11-14 00:00:00##青浦区###0##1#

                    boolean trickTag = false;
                    if (destinationName.equals("")){
                        boolean firstSolu = desp.substring(desp.length()-3, desp.length()).equals("已到达");
                        boolean secondSolu = desp.contains("已被签收");
                        boolean thirdSolu = desp.contains("正在派件");
                        trickTag = firstSolu || secondSolu || thirdSolu;
                    }

                    if (siteId != null && destinationName != null && (!destinationName.equals("") || trickTag)){
                        if (!name2idMapper.containsKey(destinationName)){
                            context.getCounter(Counter.DESTINATION_NAME_NOT_EXIST_COUNT).increment(1);
                            logger.debug("mapper dont exist, destinationName : {}",destinationName);
                            continue;
                        }
                        String destinationId;
                        if (trickTag){
                            destinationId = siteId;
                        }else {
                            destinationId = name2idMapper.get(destinationName);
                        }
                        context.getCounter(Counter.FILTERED_DATA_COUNT).increment(1);
                        Text keyText = new Text();
                        keyText.set(String.format("%s#%s",siteId, destinationId));
                        context.write(keyText, record);
                    }else{
                        context.getCounter(Counter.DIRTY_DATA_COUNT).increment(1);
                        logger.info("siteId, destination is null, 订单: {}, 数据: {}",parser.getEWB_NO(), record.toString());
                        return;
                    }
                }
            }else{
                context.getCounter(Counter.SCAN_TIME_LATER_COUNT).increment(1);
                return;
            }
        }

        private boolean checkHaveArrived(String desp) {
            if (desp.contains("已被签收") || desp.contains("正在派件"))  return true;
            return false;
        }
    }



    /**
     *
     * @param strings
     * @return
     * @throws Exception
     *
     * 参数1  输入
     * 参数2  输出
     * 参数3  reduce 数目
     * 参数4  设定的时间 例如 2018-08-13
     * 参数5  上下浮动的日期, default值为 7
     */
    public int run(String[] strings) throws Exception {

        settingDate = strings[3];
        if (strings.length == 5)    threshold = Integer.parseInt(strings[4]);
        
        checkDate(settingDate);

        this.getConf().set("settingDateTimestamp", String.valueOf(settingDateTimestamp));
        this.getConf().set("threshold", String.valueOf(threshold));

        Job job = JobGenerator.SimpleJobGenerator(this, this.getConf(), strings);
        job.setJarByClass(FilterCurrentActiveTrace.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    private void checkDate(String settingDate) {
        // TODO: 2018/1/15 check 
        settingDate += " 00:00:00";
        settingDateTimestamp = Timestamp.valueOf(settingDate).getTime();
        logger.info("setting the time : {}, timestamp value : {}", settingDate, settingDateTimestamp);
    }
}
