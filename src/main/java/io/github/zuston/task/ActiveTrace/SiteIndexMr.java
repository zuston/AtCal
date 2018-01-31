package io.github.zuston.task.ActiveTrace;

import io.github.zuston.basic.Trace.OriginalTraceRecordParser;
import io.github.zuston.util.BulkLoadTool;
import io.github.zuston.util.HbaseTool;
import io.github.zuston.util.HdfsTool;
import io.github.zuston.util.JobGenerator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by zuston on 2018/1/23.
 */
// siteID -------> ewbNO#ewbNO#ewbNO#ewbNO#ewbNO#ewbNO
public class SiteIndexMr extends Configured implements Tool {

    public static final String tableName_IN = "siteIndex_In";

    public static final String tableName_OUT = "siteIndex_Out";

    static class SiteIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

        OriginalTraceRecordParser parser = new OriginalTraceRecordParser();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 直接统计全链路的
//            String [] lines = value.toString().split("\\t+");
//            String record = lines[1];
//            String header = lines[0];
//            if (!parser.parser(record) || header.split("#").length!=2){
//                context.getCounter("RelationIndex","parser_error").increment(1);
//                return;
//            }
//            String tag = context.getConfiguration().get("tag");
//            if (tag.equals("in")){
//                context.write(new Text(header.split("#")[1]), new Text(parser.getEWB_NO()));
//                return;
//            }
//            context.write(new Text(parser.getSITE_ID()),new Text(parser.getEWB_NO()));

            // 只计算当时的在途订单
            String [] lines = value.toString().split("\\t+");
            String record = lines[1];
            String header = lines[0];
            if (!parser.parser(record) || header.split("#").length!=2){
                context.getCounter("SiteIndexMrMapper","parser_error").increment(1);
                return;
            }
            context.write(new Text(parser.getEWB_NO()),value);

        }
    }

    static class SiteIndexReducer extends Reducer<Text, Text, Text, Text> {
        OriginalTraceRecordParser parser = new OriginalTraceRecordParser();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//              直接统计全链路
//            Set<String> container = new HashSet<String>();
//            for (Text value : values){
//                container.add(value.toString());
//            }
//            String indexLine = StringUtils.join(container, "#");
//            context.write(key,new Text(indexLine));


            // 只统计当前的站点
            long settingTimeStamp = Long.parseLong(context.getConfiguration().get("settingTime"));
            long minV = Long.MAX_VALUE;
            String traveTrace = null;

            for (Text v : values){
                String [] lines = v.toString().split("\\t+");
                String record = lines[1];
                String header = lines[0];
                if (!parser.parser(record) || header.split("#").length!=2){
                    context.getCounter("SiteIndexMrReducer","parser_error").increment(1);
                    continue;
                }
                String scanTime = parser.getSCAN_TIME();
                long timestamp = Timestamp.valueOf(scanTime).getTime();
                if (timestamp < settingTimeStamp && settingTimeStamp-timestamp < minV) {
                    traveTrace = v.toString();
                    minV = settingTimeStamp - timestamp;
                }
            }

            if (traveTrace==null) return;
            String tag = context.getConfiguration().get("tag");
            String [] lines = traveTrace.split("\\t+");
            String startId = lines[0].split("#")[0];
            String endId = lines[0].split("#")[1];
            String ewbNo =  key.toString();

            if (tag.equals("in")){
                context.write(new Text(endId),new Text(ewbNo));
            }else {
                context.write(new Text(startId),new Text(ewbNo));
            }
        }
    }

    static class MergerMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String [] lines = value.toString().split("\\t");
            context.write(new Text(lines[0]),new Text(lines[1]));
        }
    }

    static class MergerReducer extends Reducer<Text, Text, Text, Text> {
        OriginalTraceRecordParser parser = new OriginalTraceRecordParser();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> container = new HashSet<String>();
            for (Text value : values){
                container.add(value.toString());
            }
            String indexLine = StringUtils.join(container, "#");
            context.write(key,new Text(indexLine));
        }
    }

    static final byte[] COLUMN_FAMILIY_INFO = Bytes.toBytes("info");
    static class SiteIndex2HbaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] lines = value.toString().split("\\t+");
            byte[] rowKey = Bytes.toBytes(lines[0]);
            Put condition = new Put(rowKey);
            condition.add(COLUMN_FAMILIY_INFO,Bytes.toBytes("index"),Bytes.toBytes(lines[1]));
            context.write(new ImmutableBytesWritable(rowKey), condition);
        }
    }

    // 第四个参数 为 in or out
    // 第五个参数 为 设置的时间
    @Override
    public int run(String[] strings) throws Exception {
        this.getConf().set("tag",strings[3]);

        this.getConf().set("settingTime", String.valueOf(Timestamp.valueOf(strings[3]).getTime()));

        String tableName = strings[3].equals("in") ? tableName_IN : tableName_OUT;
        String hFilePath = "/temp/B_siteIndex_"+strings[3];
        if (!generateIndexHfile(strings)) return -1;

        // 不是全链路操作
        String mergetOutputFile = "/temp/B_siteIndex_"+strings[3]+"_Merger";
        String [] mergeOpts = new String[]{
                strings[1],
                mergetOutputFile,
                strings[2]
        };

        if (!merge(mergeOpts)) return -1;

        String [] options = new String[]{
//                strings[1],
                mergetOutputFile,
                hFilePath,
                tableName
        };
        import2HBase(options, tableName);
        HdfsTool.deleteDir(hFilePath);
        return 1;
    }

    private boolean merge(String[] mergeOpts) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = JobGenerator.SimpleJobGenerator(this, this.getConf(), mergeOpts);
        job.setJarByClass(SiteIndexMr.class);
        job.setJobName("siteIndex_merger");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(MergerMapper.class);
        job.setReducerClass(MergerReducer.class);

        if (job.waitForCompletion(true))    return true;
        return false;
    }

    private boolean generateIndexHfile(String[] strings) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = JobGenerator.SimpleJobGenerator(this, this.getConf(), strings);
        job.setJarByClass(SiteIndexMr.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setMapperClass(SiteIndexMapper.class);
        job.setReducerClass(SiteIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (job.waitForCompletion(true))    return true;
        return false;
    }

    private void import2HBase(String[] strings, String tableName) throws IOException {
        HbaseTool htool = new HbaseTool();
        htool.createHbaseTable(tableName);
        HTable table = null;
        try {
            Job job = JobGenerator.HbaseQuickImportJobGnerator(this, this.getConf(), strings, table);
            job.setJobName("Index2Hbase");
            job.setMapperClass(SiteIndex2HbaseMapper.class);
            job.getConfiguration().setStrings("mapreduce.reduce.shuffle.input.buffer.percent", "0.1");

            if (job.waitForCompletion(true)) {
                // bulkload
                ToolRunner.run(new BulkLoadTool(), new String[]{strings[2], strings[1]});
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
