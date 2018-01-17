package io.github.zuston.basic.TraceTime;

import io.github.zuston.Util.JobGenerator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by zuston on 2017/12/30.
 */
// 算法优劣比较，依据方差
public class TraceTimeComparsionMr extends Configured implements Tool {

    static class ComparsionMapper extends Mapper<LongWritable, Text, Text, Text>{

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] array = value.toString().split("\\s+");
            context.write(new Text(array[0]), new Text(array[1]));
        }
    }

    static class ComparsionReduder extends Reducer<Text, Text, Text, Text>{

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long varianceAverage;
            long varianceMiddle;
            String first = values.iterator().toString();
            String second = values.iterator().toString();
            if (first.split(":").length==3 && second.split(":").length==2){
                varianceAverage = Long.parseLong(second.split(":")[1]);
                varianceMiddle = Long.parseLong(first.split(":")[1]);
            }else{
                varianceMiddle = Long.parseLong(second.split(":")[1]);
                varianceAverage = Long.parseLong(first.split(":")[1]);
            }
            if (varianceAverage > varianceMiddle){
//                context.getCounter(Counter.MIDDLE_WIN_COUNT).increment(1);
            }else{
//                context.getCounter(Counter.AVEARGE_WIN_COUNT).increment(1);
            }
            context.write(null, null);
        }
    }
    public int run(String[] strings) throws Exception {
        Job job = JobGenerator.SimpleJobGenerator(this, getConf(), strings);
        if (job == null)    return -1;


        job.setJarByClass(TraceTimeComparsionMr.class);

        job.setMapperClass(TraceTimeComparsionMr.ComparsionMapper.class);
        job.setReducerClass(TraceTimeComparsionMr.ComparsionReduder.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
