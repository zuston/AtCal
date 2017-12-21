package io.github.zuston.ane.Trace;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by zuston on 2017/12/18.
 */
public class JobGenerator {
    public static Logger logger =  LoggerFactory.getLogger(JobGenerator.class);
    public static Job SimpleJobGenerator(Tool tool, Configuration configuration, String [] args) throws IOException {
        if (args.length != 2){
            logger.error("please check the input file and output file path");
            return null;
        }

        Job job = new Job(configuration);
        job.setJarByClass(tool.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        return job;
    }
}
