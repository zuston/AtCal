package io.github.zuston.task.ValidateTraceTime;

import io.github.zuston.Util.HdfsTool;
import io.github.zuston.Util.JobGenerator;
import io.github.zuston.basic.Trace.OriginalTraceRecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
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
            if (recordTime + ptime > settingTimeStamp)  normalTag = false;

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

    /**
     *  输入路径
     *  输出路径
     *  reducer 个数
     *  设置时间值
     */
    public int run(String[] args) throws Exception {

        String middlePath = "/tempJob";
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

        this.getConf().set(CONTEXT_TIME_TAG, String.valueOf(Timestamp.valueOf(args[3]).getTime()));
        Job validateJob = JobGenerator.SimpleJobGenerator(this, this.getConf(), validateOptions);
        validateJob.setJarByClass(Validate.class);
        validateJob.setMapperClass(ValidateMapper.class);
        validateJob.setMapOutputValueClass(Text.class);
        validateJob.setMapOutputKeyClass(Text.class);
        validateJob.setReducerClass(ValidateReducer.class);
        validateJob.setOutputKeyClass(Text.class);
        validateJob.setOutputValueClass(IntWritable.class);

        if (validateJob.waitForCompletion(true)){
            Job mergeJob = JobGenerator.SimpleJobGenerator(this, this.getConf(), mergeOptions);
            mergeJob.setJarByClass(Validate.class);
            mergeJob.setMapperClass(MergeMapper.class);
            mergeJob.setMapOutputKeyClass(Text.class);
            mergeJob.setMapOutputValueClass(IntWritable.class);
            mergeJob.setReducerClass(MergeReducer.class);
            mergeJob.setOutputKeyClass(Text.class);
            mergeJob.setOutputValueClass(Text.class);
            if (mergeJob.waitForCompletion(true)){
                HdfsTool.deleteDir(middlePath);
            }else {
                logger.error("merge job is error");
            }
        }else {
            logger.error("validate job is error");
        }

        return 0;
    }
}
