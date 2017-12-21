package io.github.zuston.ane.Trace;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by zuston on 2017/12/18.
 * 承接上一个算出来的地点耗时，来计算均值
 */
public class TraceTimeMr extends Configured implements Tool {

    static class TraceTimeMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] record = value.toString().split("\\s+");
            context.write(new Text(record[0]),new LongWritable(Long.valueOf(record[1])));
        }
    }

    static class TraceTimeReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            int count = 0;
            // TODO: 2017/12/18 可能数据量过大导致值放不下
            for (LongWritable value : values){
                count ++ ;
                sum += Long.valueOf(value.toString());
            }

            // TODO: 2017/12/18 写入 hbase
            context.write(key, new LongWritable(sum/count));
        }
    }

    public int run(String[] strings) throws Exception {
        Job job = JobGenerator.SimpleJobGenerator(this, this.getConf(), strings);

        job.setJarByClass(TraceTimeMapper.class);
        job.setMapperClass(TraceTimeMapper.class);
        job.setReducerClass(TraceTimeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
