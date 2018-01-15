package io.github.zuston.task.ActiveTrace;

import io.github.zuston.basic.TraceTime.TraceRecordParser;
import io.github.zuston.basic.Util.HdfsTool;
import io.github.zuston.basic.Util.JobGenerator;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


enum Counter {
    GET_RECORD_COUNT,
    HAVE_ARRIVED_RECORD_COUNT,
    SCAN_TIME_LATER_COUNT,
    DIRTY_DATA_COUNT
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

    public static HashMap<String, String> name2IdMapper = null;

    public static final String mapperPath = "/site2nameMapper-1/part-r-00000";

    static class FilterMapper extends Mapper<LongWritable, Text, Text, Text>{

        private TraceRecordParser parser = new TraceRecordParser();
        @Override
        public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
            if (!parser.parse(text.toString())) return;
            String scan_time = parser.getScan_time();
            long timestamp = Timestamp.valueOf(scan_time).getTime();
            if (!activeTraceFilter(timestamp))   return;
            String ewbNo = parser.getEwb_no();
            context.getCounter(Counter.GET_RECORD_COUNT).increment(1);
            context.write(new Text(ewbNo), text);
        }

        private boolean activeTraceFilter(long timestamp) {
            long thresholdSeconds = threshold * 24 * 60 * 60;
            long minplusV = Math.abs(settingDateTimestamp-timestamp) / 1000;
            if (minplusV > thresholdSeconds)    return false;
            return true;
        }
    }



    static class FilterReducer extends Reducer<Text, Text, Text, Text>{
        private TraceRecordParser parser = new TraceRecordParser();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // filter 最小 scan_time 大于 current 的情况
            // 解析订单合理性，时间节点是否连贯
            // 判断订单是否已经送达
            List<Text> recordList = new ArrayList<Text>();
            long minScanTime = Long.MAX_VALUE;
            long maxScanTime = Long.MIN_VALUE;
            for (Text record : values){
                parser.parse(record.toString());
                long scanTime = Timestamp.valueOf(parser.getScan_time()).getTime();
                minScanTime = minScanTime > scanTime ? scanTime : minScanTime;
                maxScanTime = maxScanTime > scanTime ? maxScanTime : scanTime;
                recordList.add(record);
            }
            // 不在设置时间的区间内，剔除
            if (minScanTime < (settingDateTimestamp+86400000) &&
                    maxScanTime > settingDateTimestamp){
                for (Text record : recordList){
                    parser.parse(record.toString());
                    if (Timestamp.valueOf(parser.getScan_time()).getTime()==maxScanTime){
                        String desp = parser.getDesp();
                        if (checkHaveArrived(desp)){
                            context.getCounter(Counter.HAVE_ARRIVED_RECORD_COUNT).increment(1);
                        }
                    }
                    String siteId = parser.getSite_id();
                    String destinationName = parser.getDes_site_name();
                    if (siteId != null && destinationName != null){
                        if (!name2IdMapper.containsKey(destinationName)){
                            logger.debug("mapper dont exist, destinationName : %s",destinationName);
                            continue;
                        }
                        String destinationId = name2IdMapper.get(destinationName);
                        // tricks, 对应获取预测时间的值, siteID#destination
                        Text keyText = new Text();
                        keyText.set(String.format("%s#%s", siteId, destinationId));
                        context.write(keyText, record);
                    }else{
                        context.getCounter(Counter.DIRTY_DATA_COUNT).increment(1);
                        logger.info("siteId, destination is null, 订单: %s, 数据: %s",parser.getEwb_no(), record.toString());
                        return;
                    }
                }
            }else{
                context.getCounter(Counter.SCAN_TIME_LATER_COUNT).increment(1);
                return;
            }


        }

        private boolean checkHaveArrived(String desp) {
            if (desp.contains("已被签收"))  return true;
            return false;
        }
    }

    public static void initMapper(Configuration config){
        try {
            List<String> lineList = HdfsTool.readFromHdfs(config, mapperPath);
            name2IdMapper = new HashMap<String, String>();
            for (String record : lineList){
                String [] splitRecord = record.split("\\s+");
                if (splitRecord.length != 2)    return;
                String id = splitRecord[0];
                String name = splitRecord[1];
                name2IdMapper.put(name, id);
            }
        } catch (IOException e) {
            logger.error("init site2name error, error : %s", e.getMessage());
            System.exit(1);
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
        // 初始化 mapper
        initMapper(this.getConf());

        settingDate = strings[3];
        if (strings.length == 5)    threshold = Integer.parseInt(strings[4]);
        
        checkDate(settingDate);


        Job job = JobGenerator.SimpleJobGenerator(this, this.getConf(), strings);
        job.setJarByClass(FilterCurrentActiveTrace.class);

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
        logger.info("setting the time : %s, timestamp value : %s", settingDate, settingDateTimestamp);
    }
}
