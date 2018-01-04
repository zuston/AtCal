package io.github.zuston.ane.Util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
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

    // mr job
    public static Job SimpleJobGenerator(Tool tool, Configuration configuration, String [] args) throws IOException {
//        if (args.length != 3){
//            logger.error("please check the input file and output file path");
//            return null;
//        }

        Job job = new Job(configuration);
        job.setJarByClass(tool.getClass());
        if (args[0].contains(",")){
            FileInputFormat.addInputPaths(job, args[0]);
        }else {
            FileInputFormat.addInputPath(job, new Path(args[0]));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(Integer.parseInt(args[2]));

        return job;
    }

    // hbase mr job
    public static Job HbaseImportJobGnerator(Tool tool, Configuration configuration, String [] args) throws IOException {
        configuration.set("hbase.master", "master:60000");
        configuration.set("hbase.zookeeper.quorum","slave1,slave2,slave3");
        Job job = new Job(configuration);
        job.setJarByClass(tool.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, args[1]);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(TableOutputFormat.class);
        return job;
    }

    // bulk load
    public static Job HbaseQuickImportJobGnerator(Tool tool, Configuration configuration, String[] args, HTable table) throws IOException {
        configuration.set("hbase.master", "master:60000");
        configuration.set("hbase.zookeeper.quorum","slave1,slave2,slave3");
        Job job = new Job(configuration);

        job.setJarByClass(tool.getClass());
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setSpeculativeExecution(false);
        job.setReduceSpeculativeExecution(false);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        table = new HTable(configuration, args[2]);
        HFileOutputFormat2.configureIncrementalLoad(job, table);

        return job;
    }
}
