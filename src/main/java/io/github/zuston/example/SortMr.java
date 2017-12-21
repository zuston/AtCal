package io.github.zuston.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.sql.Timestamp;

/**
 * Created by zuston on 2017/12/11.
 */
public class SortMr extends Configured implements Tool {

    static class TemMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Integer v = Integer.parseInt(value.toString().split(":")[1]);
            context.write(new IntWritable(v), value);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SortMr");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJarByClass(SortMr.class);
        job.setMapperClass(TemMapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
//        int exitCode = ToolRunner.run(new SortMr(), args);
//        System.exit(exitCode);
        String time = "2017-09-24 14:23:00";
        Timestamp timeStamp = Timestamp.valueOf(time);
        System.out.println(timeStamp);
        System.out.println(timeStamp.getNanos());
        System.out.println(timeStamp.getTime());

        String time1 = "2017-09-25 15:23:00";
        Timestamp timeStamp1 = Timestamp.valueOf(time1);
        System.out.println(timeStamp1);
        System.out.println(timeStamp1.getNanos());
        System.out.println(timeStamp1.getTime());

        System.out.println(timeStamp1.getTime()-timeStamp.getTime());
    }
}
