package io.github.zuston.check;

import io.github.zuston.util.JobGenerator;
import io.github.zuston.basic.Trace.OriginalTraceRecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by zuston on 2018/1/24.
 */
public class CheckValidate extends Configured implements Tool {

    static class CVmapper extends Mapper<LongWritable, Text, Text, Text>{
        public OriginalTraceRecordParser parser = new OriginalTraceRecordParser();
        @Override
        public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
            String [] recordList = text.toString().split("\\t");
            String destination = recordList[0];
            if (destination.equals(context.getConfiguration().get("value"))){
                context.write(new Text(destination), text);
            }
        }
    }

    static class CVreducer extends Reducer<Text, Text, Text, Text>{
        public OriginalTraceRecordParser parser = new OriginalTraceRecordParser();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String[] recordList = value.toString().split("\\t");
                String line = recordList[1];
                String originalRecord = line.substring(0, line.lastIndexOf("#"));
                if (!parser.parser(originalRecord)) return;
                context.write(new Text(parser.getEWB_NO()),new Text(line));
            }
        }
    }


    // 输入文件位置
    // 输出
    // reduce Number
    // 比较值
    @Override
    public int run(String[] strings) throws Exception {
        this.getConf().set("value",strings[3]);
        Job job = JobGenerator.SimpleJobGenerator(this, this.getConf(), strings);
        job.setJobName("validate check");
        job.setJarByClass(CheckValidate.class);

        job.setMapperClass(CVmapper.class);
        job.setReducerClass(CVreducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
