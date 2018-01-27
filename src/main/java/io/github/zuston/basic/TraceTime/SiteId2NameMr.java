package io.github.zuston.basic.TraceTime;

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
 * Created by zuston on 2017/12/28.
 */
// id 对应的 站点名称
public class SiteId2NameMr extends Configured implements Tool{

    static class SiteId2NameMapper extends Mapper<LongWritable, Text, Text, Text> {
        private TraceRecordParser parser = new TraceRecordParser();

        Text tempTextValue = new Text();
        Text tempTextKey = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!parser.parse(value.toString())) {
                context.getCounter(COUNTER.SITE_ID_MISSING).increment(1);
                return;
            }
            String site_id = parser.getSite_id();
            String site_name = parser.getSite_name();
            tempTextKey.set(site_id);
            tempTextValue.set(site_name);
            // 可做一次 combiner 优化
            context.write(tempTextKey,tempTextValue);
        }
    }

    static class SiteId2NameReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key,new Text(values.iterator().next().toString()));
        }
    }

    public int run(String[] strings) throws Exception {
        Job job = JobGenerator.SimpleJobGenerator(this, getConf(), strings);
        if (job == null)    return -1;

        job.setJarByClass(SiteId2NameMr.class);

        job.setMapperClass(SiteId2NameMapper.class);
        job.setReducerClass(SiteId2NameReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (strings.length==3){
            job.setNumReduceTasks(Integer.parseInt(strings[2]));
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
