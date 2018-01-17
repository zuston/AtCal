package io.github.zuston.task.ValidateTraceTime;

import io.github.zuston.Util.BulkLoadTool;
import io.github.zuston.Util.HbaseTool;
import io.github.zuston.Util.HdfsTool;
import io.github.zuston.Util.JobGenerator;
import io.github.zuston.basic.Trace.OriginalTraceRecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;

/**
 * Created by zuston on 2018/1/17.
 */
public class Validate extends Configured implements Tool {

    public static Logger logger = LoggerFactory.getLogger(Validate.class);

    public static final String CONTEXT_TIME_TAG = "timeTag";

    // 从 activeTrace 中读入数据
    static class ValidateMapper extends Mapper<LongWritable, Text, Text, Text>{

        public OriginalTraceRecordParser parser = new OriginalTraceRecordParser();

        @Override
        public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
            String [] recordList = text.toString().split("\\t");
            String originalRecord = recordList[1].split("_")[0];
            parser.parser(originalRecord);
            String ewbno = parser.getEWB_NO();
            context.getCounter(Counter.ALL_DATA_COUNT).increment(1);
            context.write(new Text(ewbno),text);
        }
    }

    static class ValidateReducer extends Reducer<Text, Text, Text, IntWritable>{

        public OriginalTraceRecordParser parser = new OriginalTraceRecordParser();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 设置的时间点
            long settingTimeStamp = Long.parseLong(context.getConfiguration().get(CONTEXT_TIME_TAG));

            // 找出的最近的那条记录
            String record = "";
            long minV = Long.MAX_VALUE;

            for (Text value : values){
                String [] recordList = value.toString().split("\\t");
                String originalRecord = recordList[1].split("_")[0];
                parser.parser(originalRecord);
                String scanTime = parser.getSCAN_TIME();
                long timestamp = Timestamp.valueOf(scanTime).getTime();
                if (timestamp < settingTimeStamp && settingTimeStamp-timestamp < minV) {
                    record = value.toString();
                    minV = settingTimeStamp - timestamp;
                }
            }

            if (minV == Long.MAX_VALUE)     return;

            String [] recordList = record.split("\\t");
            String originalRecord = recordList[1].split("_")[0];
            parser.parser(originalRecord);
            String predictTime = recordList[1].split("_")[1];
            double ptime = Double.valueOf(predictTime);
            String scanTime = parser.getSCAN_TIME();
            long recordTime = Timestamp.valueOf(scanTime).getTime();

            boolean normalTag = true;
            if (recordTime + ptime * 1000 * 60 > settingTimeStamp)  normalTag = false;

            int valueComponent = ((normalTag ? 0 : 1));
            context.getCounter(Counter.VALIDATE_LINE_COUNT).increment(1);
            context.write(new Text(recordList[0]),new IntWritable(valueComponent));
        }

    }

    static class MergeMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] vlist = value.toString().split("\\s+");
            context.write(new Text(vlist[0]), new IntWritable(Integer.valueOf(vlist[1])));
        }
    }

    static class MergeReducer extends Reducer<Text, IntWritable, Text, Text>{
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            int nonormal = 0;
            for (IntWritable v : values){
                count++;
                nonormal += v.get();
            }
            if (nonormal > 0)   context.getCounter(Counter.NO_NORMAL_COUNT).increment(1);
            context.write(key,new Text(count+"#"+nonormal));
        }
    }

    static class ImporterMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        static final byte[] COLUMN_FAMILIY_INFO = Bytes.toBytes("info");

        static final byte[] COLUMN_TOTAL = Bytes.toBytes("total");

        static final byte[] COLUMN_ABNORMAL = Bytes.toBytes("abnormal");

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] recordArr = value.toString().split("\\t");
            byte[] rowKey = Bytes.toBytes(recordArr[0]);
            String total = recordArr[0].split("#")[0];
            String abnormal = recordArr[0].split("#")[1];
            Put condition = new Put(rowKey);
            condition.add(COLUMN_FAMILIY_INFO,COLUMN_TOTAL,Bytes.toBytes(total));
            condition.add(COLUMN_ABNORMAL,COLUMN_ABNORMAL,Bytes.toBytes(abnormal));
            context.write(new ImmutableBytesWritable(rowKey), condition);
        }
    }

    /**
     *  输入路径
     *  输出路径
     *  reducer 个数
     *  设置时间值
     */
    public int run(String[] args) throws Exception {

        String middlePath = "/tempJob";
        String hfilePath = "/tempHfile";

        String [] validateOptions = new String[]{
                args[0],
                middlePath,
                args[2]
        };
        String [] mergeOptions = new String[]{
            middlePath,
                args[1],
                args[2]
        };
        String tableName = "Validate";

        String [] hfileOptions = new String[] {
            args[1],
                hfilePath,
                tableName
        };

        String [] bulkloadOptions = new String[]{
                tableName,
                hfilePath
        };

        this.getConf().set(CONTEXT_TIME_TAG, String.valueOf(Timestamp.valueOf(args[3]).getTime()));
        try {
            validateJob(validateOptions);
            mergeJob(mergeOptions);
            createHbaseTable(tableName);
            generateHfile(hfileOptions);
            bulkLoad(bulkloadOptions);
            HdfsTool.deleteDir(middlePath);
            HdfsTool.deleteDir(hfilePath);
        }catch (Exception e){
            e.printStackTrace();
        }

        return 0;
    }

    private void validateJob(String [] opts) throws Exception {
        Job validateJob = JobGenerator.SimpleJobGenerator(this, this.getConf(), opts);
        validateJob.setJarByClass(Validate.class);
        validateJob.setMapperClass(ValidateMapper.class);
        validateJob.setMapOutputValueClass(Text.class);
        validateJob.setMapOutputKeyClass(Text.class);
        validateJob.setReducerClass(ValidateReducer.class);
        validateJob.setOutputKeyClass(Text.class);
        validateJob.setOutputValueClass(IntWritable.class);
        if (validateJob.waitForCompletion(true)){
            logger.error("validate JOB SUCCES");
        }else {
            throw new Exception("validateJob error");
        }
    }

    private void mergeJob(String [] opts) throws Exception {
        Job mergeJob = JobGenerator.SimpleJobGenerator(this, this.getConf(), opts);
        mergeJob.setJarByClass(Validate.class);
        mergeJob.setMapperClass(MergeMapper.class);
        mergeJob.setMapOutputKeyClass(Text.class);
        mergeJob.setMapOutputValueClass(IntWritable.class);
        mergeJob.setReducerClass(MergeReducer.class);
        mergeJob.setOutputKeyClass(Text.class);
        mergeJob.setOutputValueClass(Text.class);
        if (mergeJob.waitForCompletion(true)){
            logger.error("merge JOB SUCCES");
        }else {
            throw  new Exception("merge job is error");
        }
    }

    private void createHbaseTable(String tableName) throws IOException {
        HbaseTool htool = new HbaseTool();
        htool.createHbaseTable(tableName);
    }

    private void generateHfile(String [] opts) throws Exception {
        HTable table = null;
        Job job = JobGenerator.HbaseQuickImportJobGnerator(this, this.getConf(),opts, table);
        job.setJobName("validate2Hbase");
        job.setMapperClass(ImporterMapper.class);
        job.getConfiguration().setStrings("mapreduce.reduce.shuffle.input.buffer.percent", "0.1");
        if (job.waitForCompletion(true)){
            logger.error("生成 hfile 成功");
        }else {
            throw new Exception("生成 hfile 失败");
        }
    }

    private void bulkLoad(String [] opts) throws Exception {
        ToolRunner.run(new BulkLoadTool(), opts);
    }
}
