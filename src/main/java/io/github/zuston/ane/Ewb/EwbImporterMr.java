package io.github.zuston.ane.Ewb;

import io.github.zuston.ane.Util.JobGenerator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

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
            // 时间，运单号，出发地，目的地
            String rowKeyComponent = String.format("%s:%s:%s:%s",parser.getCREATED_TIME(),parser.getEWB_NO(),parser.getSEND_SITE_ID(),parser.getDISPATCH_SITE_ID());
            byte[] rowKey = Bytes.toBytes(rowKeyComponent);
            Put putCondition = new Put(rowKey);

            for (Field field : fields){
                field.setAccessible(true);
                try {
                    String name = field.getName();
                    String fieldValue = (String) field.get(parser);
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
        Job job = JobGenerator.HbaseImportJobGnerator(this, this.getConf(),strings);
        job.setJobName("Ewb2Hbase");
        job.setMapperClass(EwbImporterMapper.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
