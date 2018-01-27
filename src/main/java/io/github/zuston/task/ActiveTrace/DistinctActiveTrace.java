package io.github.zuston.task.ActiveTrace;

import io.github.zuston.basic.Trace.OriginalTraceRecordParser;
import io.github.zuston.util.JobGenerator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by zuston on 2018/1/17.
 */
public class DistinctActiveTrace extends Configured implements Tool {


    static class distinctMapper extends Mapper<LongWritable, Text, Text, Text>{

        public OriginalTraceRecordParser parser = new OriginalTraceRecordParser();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter("distinct","NONE_DISTINCTED_ACTIVE_RECORD_COUNT").increment(1);
            String no = value.toString().split("\\t+")[1];
            parser.parser(no);
            context.write(new Text(parser.getTRACE_ID()), value);
        }

    }



    static class distinctReducer extends Reducer<Text, Text, Text, Text>{

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.getCounter("distinct","DISTINCTED_ACTIVE_RECORD_COUNT").increment(1);
            context.write(values.iterator().next(),null);
        }
    }

    public int run(String[] strings) throws Exception {
        Job job = JobGenerator.SimpleJobGenerator(this, this.getConf(), strings);

        job.setJarByClass(DistinctActiveTrace.class);

        job.setMapperClass(distinctMapper.class);
        job.setReducerClass(distinctReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }
}
