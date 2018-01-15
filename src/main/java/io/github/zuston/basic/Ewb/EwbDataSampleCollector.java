package io.github.zuston.basic.Ewb;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Random;

/**
 * Created by zuston on 2018/1/5.
 */
// 采样算法
public class EwbDataSampleCollector extends Configured implements Tool {
    static class SampleMapper extends Mapper<LongWritable, Text, Text, Text>{

        public static EwbRecordParser parser = new EwbRecordParser();

        public static Random random = new Random();

        @Override
        public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
            // TODO: 2018/1/6 算法更换为鱼塘采样
            if (random.nextInt(50)!=0)  return;
            if (!parser.parser(text.toString()))    return;
//            String rowKeyComponent = String.format("%s#%s#%s#%s",parser.getEWB_NO(),parser.getSEND_SITE_ID(),parser.getDISPATCH_SITE_ID(),parser.getCREATED_TIME());
            String rowKeyComponent = String.format("%s", parser.getEWB_NO());
            context.write(new Text(rowKeyComponent), new Text(""));
        }
    }


    static class SampleReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text text, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v : values){
                context.write(text, v);
            }
        }
    }

    public int run(String[] strings) throws Exception {
        Job job = new Job(this.getConf());
        job.setJarByClass(EwbDataSampleCollector.class);
        job.setJobName("ewbDataSampleCollector");
        job.setMapperClass(SampleMapper.class);
        job.setReducerClass(SampleReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
