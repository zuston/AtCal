package io.github.zuston.ane.TraceTime;

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
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by zuston on 2018/1/2.
 */
// 异常点过滤
public class OutliersFilterMr extends Configured implements Tool{

    public static final org.slf4j.Logger LOGGER_FACTORY = LoggerFactory.getLogger(OutliersFilterMr.class);

    public static final int baseLineValue = 3;
    static class BaseLineMapper extends Mapper<LongWritable, Text, FilterPair, Text>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] record = value.toString().split("\\s+");
            context.getCounter(Counter.BEFORE_FILTER_ORDER_COUNT).increment(1);
            context.write(new FilterPair(record[0],"0"),new Text(record[1]));
        }
    }

    static class OriginalDateMapper extends Mapper<LongWritable, Text, FilterPair, Text>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter(Counter.BEFORE_FILTER_ORDER_COUNT).increment(1);
            String[] record = value.toString().split("\\s+");
            context.write(new FilterPair(record[0],"1"),new Text(record[1]));
        }
    }

    static class KeyPartitioner extends Partitioner<FilterPair, Text>{

        public int getPartition(FilterPair filterPair, Text text, int i) {
            return (filterPair.getFirst().hashCode() & Integer.MAX_VALUE) % i;
        }
    }

    static class KeyGrouping extends WritableComparator {
        public KeyGrouping(){
            super(FilterPair.class, true);
        }
        public int compare(WritableComparable a, WritableComparable b) {
            FilterPair ap = (FilterPair) a;
            FilterPair bp = (FilterPair) b;
            return ap.getFirst().compareTo(bp.getFirst());
        }
    }

    static class FilterReducer extends Reducer<FilterPair, Text, Text, Text>{
        @Override
        public void reduce(FilterPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            Text baseLine = iterator.next();
            String bl [] = baseLine.toString().split(":");
            if (bl.length!=3 || bl[0].equals("") || bl[1].equals("")){
                context.getCounter(Counter.BASELINE_ERROR).increment(1);
                return;
            }
            double baseLineAvg = Double.parseDouble(bl[0]);
            // 标准差
            double baseLineStandardDeviation = Math.sqrt(Double.parseDouble(bl[1]));

            while (iterator.hasNext()){
                Text record = iterator.next();
                //正态去除离群点
                if (record.toString().equals(""))   continue;
                long value = Long.parseLong(record.toString());
                if (Math.abs(value-baseLineAvg) > baseLineValue*baseLineStandardDeviation){
                    context.getCounter(Counter.ABSORT_COUNT).increment(1);
                    continue;
                }
                context.write(key.getFirst(),record);
            }
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 4){
            LOGGER_FACTORY.error("please check the command options");
            return -1;
        }
        Job job = new Job(getConf(), "filter the outliers");
        job.setJarByClass(getClass());

        Path baseLinePath = new Path(args[0]);
        Path originalPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        MultipleInputs.addInputPath(job, baseLinePath, TextInputFormat.class, BaseLineMapper.class);
        MultipleInputs.addInputPath(job, originalPath, TextInputFormat.class, OriginalDateMapper.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(KeyGrouping.class);

        job.setMapOutputKeyClass(FilterPair.class);
        job.setReducerClass(FilterReducer.class);

        job.setOutputKeyClass(Text.class);

        job.setNumReduceTasks(Integer.parseInt(args[3]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}


class FilterPair implements WritableComparable<FilterPair>{
    private Text first;
    private Text second;

    public FilterPair(){
        set(new Text(),new Text());
    }

    public FilterPair(String first, String second){
        set(new Text(first), new Text(second));
    }

    public FilterPair(Text first, Text second){
        set(first, second);
    }

    public void set(Text first, Text second){
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    public int compareTo(FilterPair o) {
        int cmp = first.compareTo(o.getFirst());
        if (cmp != 0)   return cmp;
        return second.compareTo(o.getSecond());
    }

    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    public String toString(){
        return first + "\t" + second;
    }
}

