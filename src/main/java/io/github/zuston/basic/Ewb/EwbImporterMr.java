package io.github.zuston.basic.Ewb;

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
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * Created by zuston on 2017/12/29.
 */
// create "hsOptEwb","info"

enum Error {
    RECORD_ERROR,
    REFLECT_ERROR
}

public class EwbImporterMr extends Configured implements Tool {

    public static Logger logger = LoggerFactory.getLogger(EwbImporterMr.class);

    public static final byte[] EWB_FAMILY_COLUMN = Bytes.toBytes("info");

    public static EwbRecordParser parser = new EwbRecordParser();

    public static Field [] fields = parser.getClass().getDeclaredFields();

    static class EwbImporterMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!parser.parser(value.toString()))   {
                context.getCounter(Error.RECORD_ERROR).increment(1);
                return;
            }
            // 运单号
            String rowKeyComponent = String.format("%s",parser.getEWB_NO());
            byte[] rowKey = Bytes.toBytes(rowKeyComponent);
            Put putCondition = new Put(rowKey);

            for (Field field : fields){
                field.setAccessible(true);
                try {
                    String name = field.getName();
                    String fieldValue = (String) field.get(parser);
                    if (fieldValue.equals(""))  continue;
                    putCondition.add(EWB_FAMILY_COLUMN,Bytes.toBytes(name),Bytes.toBytes(fieldValue));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                    context.getCounter(Error.REFLECT_ERROR).increment(1);
                }
            }
            context.write(new ImmutableBytesWritable(rowKey), putCondition);
        }
    }

    public int run(String[] strings) throws Exception {
        HTable table = null;
        try {
            Job job = JobGenerator.HbaseQuickImportJobGnerator(this, this.getConf(),strings, table);
            job.setJobName("Ewb2Hbase");
            job.setMapperClass(EwbImporterMapper.class);

            job.getConfiguration().setStrings("mapreduce.reduce.shuffle.input.buffer.percent", "0.1");
            return job.waitForCompletion(true) ? 1 : 0;

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (table!=null)    table.close();
        }

        return 0;
    }
}

