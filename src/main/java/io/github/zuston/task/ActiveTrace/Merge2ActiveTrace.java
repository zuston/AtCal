package io.github.zuston.task.ActiveTrace;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by zuston on 2018/1/15.
 */
public class Merge2ActiveTrace extends Configured implements Tool {

    // 已经过滤过的 trace 记录
    static class FilteredTraceMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] splitArr = value.toString().split("\\s+");
            context.write(new Text(splitArr[0]+"_1"),value);
        }
    }

    // 已经预测的时间
    static class PredictTimeMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] splitArr = value.toString().split("\\s+");
            context.write(new Text(splitArr[0]+"_0"), new Text(splitArr[1]));
        }
    }

    // 分区
    static class MergePartitioner extends Partitioner<Text, Text>{

        public int getPartition(Text key, Text value, int i) {
            String partitionerKey = key.toString().split("_")[0];
            return ( partitionerKey.hashCode() & Integer.MAX_VALUE )% i;
        }
    }

    // 分组
    static class MergeGrouper extends WritableComparator{
        public int compare(WritableComparable a, WritableComparable b) {
            String aT = a.toString().split("_")[0];
            String bT = b.toString().split("_")[0];
            return aT.compareTo(bT);
        }
    }


    static class Merge2ActiveTraceReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            Text predictTime = iterator.next();
            while (iterator.hasNext()){
                Text value = iterator.next();
                context.write(new Text(value.toString()+"#"+predictTime),null);
            }
        }
    }




    public int run(String[] strings) throws Exception {
        Job job = new Job(this.getConf(),"merge2ActiveTrace");
        job.setJarByClass(Merge2ActiveTrace.class);

        Path FilterTimeActiveRecordPath = new Path(strings[0]);
        Path PredictTimeRecord = new Path(strings[1]);
        Path output = new Path(strings[2]);

        MultipleInputs.addInputPath(job, FilterTimeActiveRecordPath, TextInputFormat.class, FilteredTraceMapper.class);
        MultipleInputs.addInputPath(job, PredictTimeRecord, TextInputFormat.class, PredictTimeMapper.class);

        FileOutputFormat.setOutputPath(job, output);

        job.setPartitionerClass(MergePartitioner.class);

        job.setGroupingComparatorClass(MergeGrouper.class);

        job.setReducerClass(Merge2ActiveTraceReducer.class);

        job.setNumReduceTasks(Integer.parseInt(strings[3]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
