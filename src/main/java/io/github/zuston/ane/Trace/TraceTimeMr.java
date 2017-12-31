package io.github.zuston.ane.Trace;

import io.github.zuston.ane.Util.JobGenerator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zuston on 2017/12/18.
 * 承接上一个算出来的地点耗时，来计算均值
 */
public class TraceTimeMr extends Configured implements Tool {

    static class TraceTimeMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

        Text tempKeyText = new Text();
        LongWritable tempValueLong = new LongWritable();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] record = value.toString().split("\\s+");
            tempKeyText.set(record[0]);
            tempValueLong.set(Long.valueOf(record[1]));
            context.write(tempKeyText, tempValueLong);
        }
    }

    // 计算均值的代价函数
    static class TraceTimeReducer extends Reducer<Text, LongWritable, Text, Text>{

        Text tempValueLong = new Text();
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            int count = 0;
            List<LongWritable> valueList = new ArrayList<LongWritable>();
            for (LongWritable value : values){
                count ++ ;
                sum += Long.valueOf(value.toString());
                valueList.add(value);
            }
            // 平均值
            double average = sum / count;

            long varianceSum = 0;
            for (LongWritable value : valueList){
                Double differenceValue = Double.valueOf(value.toString()) - average;
                varianceSum += Math.pow((differenceValue),2);
            }
            // 方差
            long varianceSumAverage = varianceSum / count;

            tempValueLong.set(average+":"+varianceSumAverage);
            context.write(key, tempValueLong);
        }
    }

    // 计算中心点，离开各点距离最小的
    static class TraceTimeKmeansReducer extends Reducer<Text, LongWritable, Text, Text> {
        Text tempValue = new Text();
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            List<Long> valueList = new ArrayList<Long>();
            for (LongWritable v : values){
                valueList.add(Long.valueOf(v.toString()));
            }
            long minMinus = Long.MAX_VALUE;
            long targetValue = 0;
            for (int i=0;i<valueList.size();i++){
                long currentMinus = 0;
                long currentValue = valueList.get(i);
                for (int j=0;j<valueList.size();j++){
                    currentMinus += Math.abs(currentValue - valueList.get(j));
                }
                if (currentMinus < minMinus){
                    minMinus = currentMinus;
                    targetValue = currentValue;
                }
            }
            long varianceSum = 0;
            for (long v : valueList){
                varianceSum += Math.pow(v-targetValue,2);
            }
            long variance = varianceSum / valueList.size();
            // 表示类 kmeans 算法
            tempValue.set(String.valueOf(targetValue)+":"+variance+":m");
            context.write(key, tempValue);
        }
    }

    public int run(String[] strings) throws Exception {
        Job job = JobGenerator.SimpleJobGenerator(this, this.getConf(), strings);

        job.setJarByClass(TraceTimeMr.class);
        job.setMapperClass(TraceTimeMapper.class);

        // // TODO: 2017/12/30 待优化
        if (strings[3].equals("1")){
            job.setReducerClass(TraceTimeReducer.class);
        }else{
            job.setReducerClass(TraceTimeKmeansReducer.class);
        }

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
