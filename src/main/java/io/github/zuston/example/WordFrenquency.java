package io.github.zuston.example;

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
 * Created by zuston on 2017/12/22.
 */
public class WordFrenquency extends Configured implements Tool {
    static class WordFrenquencyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        Text tempKey = new Text();
        LongWritable tempValue = new LongWritable(1);
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] words = value.toString().split("\\s+");
            for (int i=0; i<words.length-1;i++){
                for (int j=i+1;j<words.length;j++){
                    tempKey.set(words[i]+"#"+words[j]);
                    context.write(tempKey,tempValue);
                }
            }
        }
    }


    static class WordFrenquencyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        LongWritable tempValue = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v : values){
                sum += Long.valueOf(v.toString());
            }
            if (sum >= 5){
                tempValue.set(sum);
                context.write(key,tempValue);
            }
        }
    }

    public int run(String[] strings) throws Exception {
        Job job = JobGenerator.SimpleJobGenerator(this, getConf(), strings);
        if (job == null)    return -1;

        job.setJarByClass(WordFrenquency.class);

        job.setMapperClass(WordFrenquency.WordFrenquencyMapper.class);
        job.setReducerClass(WordFrenquency.WordFrenquencyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

//        job.setNumReduceTasks(1);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
