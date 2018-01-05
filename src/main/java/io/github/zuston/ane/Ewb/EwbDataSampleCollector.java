package io.github.zuston.ane.Ewb;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by zuston on 2018/1/5.
 */
// 采样
public class EwbDataSampleCollector extends Configured implements Tool {
    static class SampleMapper extends Mapper<LongWritable, Text, Text, Text>{

        public static EwbRecordParser parser = new EwbRecordParser();

        @Override
        public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
            if (!parser.parser(text.toString()))    return;
            String rowKeyComponent = String.format("%s#%s#%s#%s",parser.getCREATED_TIME().hashCode(),parser.getEWB_NO(),parser.getSEND_SITE_ID(),parser.getDISPATCH_SITE_ID());
            context.write(new Text(rowKeyComponent), new Text(""));
        }
    }

    public int run(String[] strings) throws Exception {
        Job job = new Job(this.getConf());
        job.setJarByClass(EwbDataSampleCollector.class);
        job.setJobName("ewbDataSampleCollector");
        job.setNumReduceTasks(0);
        job.setInputFormatClass(FileInputFormat.class);
        job.setMapperClass(SampleMapper.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path(strings[1]));

        InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.001,10000,12);
        InputSampler.writePartitionFile(job, sampler);
        FileInputFormat.addInputPath(job, new Path("/ewbSample-1"));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
