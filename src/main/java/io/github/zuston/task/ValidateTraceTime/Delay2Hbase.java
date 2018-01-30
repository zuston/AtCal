package io.github.zuston.task.ValidateTraceTime;

import io.github.zuston.util.BulkLoadTool;
import io.github.zuston.util.HbaseTool;
import io.github.zuston.util.HdfsTool;
import io.github.zuston.util.JobGenerator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by zuston on 2018/1/30.
 */
// 将延迟的订单整合到 hbase 中
public class Delay2Hbase extends Configured implements Tool {

    public static final String DELAY_TABLE = "delayIndex";

    static class Dmapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        public void map(LongWritable key, Text text, Context context){
            try {
                String lineKey = text.toString().split("\\t")[0];
                String [] splitArr = lineKey.toString().split("&");
                String ewbNo = splitArr[1];
                String start_id = splitArr[0].split("#")[0];
                String end_id = splitArr[0].split("#")[1];
                if (!start_id.equals(end_id)){
                    context.getCounter("Delay2Hbase","NO_MATCH").increment(1);
                }
                context.write(new Text(start_id),new Text(ewbNo));
            }catch (Exception e){
                context.getCounter("Delay2Hbase","SPLIT_ERROR").increment(1);
                return;
            }
        }
    }

    static class Dreducer extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> list = new HashSet<String>();
            for (Text v : values){
                list.add(v.toString());
            }
            String info = StringUtils.join("#", list);
            context.write(key,new Text(info));
        }
    }

    static final byte[] COLUMN_FAMILIY_INFO = Bytes.toBytes("info");
    static class D2HbaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
        @Override
        public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
            String lineArr [] = text.toString().split("\\t");
            byte[] rowKey = Bytes.toBytes(lineArr[0]);
            Put condition = new Put(rowKey);
            condition.add(COLUMN_FAMILIY_INFO,Bytes.toBytes("index"),Bytes.toBytes(lineArr[1]));
            context.write(new ImmutableBytesWritable(rowKey), condition);
        }
    }

    // 输入文件，输出文件，reduceNum
    @Override
    public int run(String[] strings) throws Exception {
        String [] handlerOpts = new String[]{
                strings[0],
                strings[1],
                strings[2]
        };

        String tempHbaseHfilePath = "/temp/C_delayIndex_hfile";

        String [] _2HbaseOpts = new String[]{
                strings[1],
                tempHbaseHfilePath,
                DELAY_TABLE
        };

        if (handlerData(handlerOpts)){
            _2Hbase(_2HbaseOpts);
            HdfsTool.deleteDir(tempHbaseHfilePath);
        }else {
            throw  new Exception("handlerAbnormal2Hbase_1 error");
        }
        return 0;
    }

    private boolean handlerData(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = JobGenerator.SimpleJobGenerator(this,this.getConf(),args);

        job.setJobName("handlerAbnormal2Hbase_1");
        job.setJarByClass(Delay2Hbase.class);

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setMapperClass(Dmapper.class);
        job.setReducerClass(Dreducer.class);

        if (job.waitForCompletion(true))    return true;
        return false;
    }

    private boolean _2Hbase(String [] args) throws IOException {
        HbaseTool htool = new HbaseTool();
        htool.createHbaseTable(DELAY_TABLE);
        HTable table = null;
        try {
            Job job = JobGenerator.HbaseQuickImportJobGnerator(this, this.getConf(), args, table);
            job.setJobName("handlerAbnormal2Hbase_2");
            job.setMapperClass(D2HbaseMapper.class);
            job.getConfiguration().setStrings("mapreduce.reduce.shuffle.input.buffer.percent", "0.5");

            if (job.waitForCompletion(true)) {
                ToolRunner.run(new BulkLoadTool(), new String[]{args[2], args[1]});
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }
}
