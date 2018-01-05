package io.github.zuston.ane.Trace;

import io.github.zuston.ane.Util.JobGenerator;
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

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * Created by zuston on 2018/1/4.
 */

// create "hsOptTrace", "info"
enum ERROR {
    ORIGINAL_TRACE_RECORD_ERROR,
    REFLECT_ERROR
}
public class OriginalTraceImporterMr extends Configured implements Tool {

    static final byte[] COLUMN_FAMILIY_INFO = Bytes.toBytes("info");

    public static OriginalTraceRecordParser parser = new OriginalTraceRecordParser();

    public static Field[] fields = parser.getClass().getDeclaredFields();


    static class TraceImporterMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!parser.parser(value.toString())){
                context.getCounter(ERROR.ORIGINAL_TRACE_RECORD_ERROR).increment(1);
                return;
            }
            // rowkey : siteId#desId#scanTime#ewbNo
            String rowkeyComponent = String.format("%s:%s:%s:%s",parser.getSITE_ID(),parser.getDEST_SITE_ID(),parser.getSCAN_TIME(),parser.getEWB_NO());
            byte[] rowKey = Bytes.toBytes(rowkeyComponent);
            Put condition = new Put(rowKey);

            // 待查，是否对性能有巨大影响

            for (Field field : fields){
                try {
                    field.setAccessible(true);
                    String fieldName = field.getName();
                    String fieldValue = (String) field.get(parser);
                    if (fieldValue.equals(""))  continue;
                    condition.add(COLUMN_FAMILIY_INFO,Bytes.toBytes(fieldName),Bytes.toBytes(fieldValue));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                    context.getCounter(ERROR.REFLECT_ERROR).increment(1);
                    continue;
                }
            }
            context.write(new ImmutableBytesWritable(rowKey), condition);
        }
    }

    public int run(String[] strings) throws Exception {
        HTable table = null;
        try {
            Job job = JobGenerator.HbaseQuickImportJobGnerator(this, this.getConf(),strings, table);
            job.setJobName("Trace2Hbase");
            job.setMapperClass(TraceImporterMapper.class);

            job.setNumReduceTasks(18);

            return job.waitForCompletion(true) ? 1 : 0;

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (table!=null)    table.close();
        }

        return 0;
    }
}
