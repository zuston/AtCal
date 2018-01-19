package io.github.zuston.task.ActiveTrace;

import io.github.zuston.Util.BulkLoadTool;
import io.github.zuston.Util.HbaseTool;
import io.github.zuston.Util.JobGenerator;
import io.github.zuston.basic.Trace.OriginalTraceRecordParser;
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
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * Created by zuston on 2018/1/17.
 */
public class ActiveTrace2Hbase extends Configured implements Tool {

    public static Logger logger = LoggerFactory.getLogger(ActiveTrace2Hbase.class);


    static final byte[] COLUMN_FAMILIY_INFO = Bytes.toBytes("info");

    public static OriginalTraceRecordParser parser = new OriginalTraceRecordParser();

    public static Field[] fields = parser.getClass().getDeclaredFields();

    public static final String tableTag = "tableTag";
    public static final String tableTagIn = "in";


    static class AtMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] record = value.toString().split("\\t+");
            String originalSqlRecord = record[1].split("_")[0];
            String predictTime = record[1].split("_")[1];

            String rowKeyId = record[0].split("#")[0];
            if (context.getConfiguration().get(tableTag).equals(tableTagIn)){
                rowKeyId = record[0].split("#")[1];
            }

            if (!parser.parser(originalSqlRecord)){
                return;
            }

            if (parser.getEWB_NO().equals("") || parser.getSITE_ID().equals(""))  {
                return;
            }
            // 出发地+订单号  out
            // 目的地+订单号  in
            String rowKeyComponent = String.format("%s#%s", rowKeyId, parser.getEWB_NO());

            byte[] rowKey = Bytes.toBytes(rowKeyComponent);
            Put condition = new Put(rowKey);

            // 待查，是否对性能有巨大影响
            for (Field field : fields){
                try {
                    field.setAccessible(true);
                    String fieldValue = (String) field.get(parser);
                    String fieldName = field.getName();
                    if (fieldValue.equals(""))  continue;
                    condition.add(COLUMN_FAMILIY_INFO,Bytes.toBytes(fieldName),Bytes.toBytes(fieldValue));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                    continue;
                }
            }
            condition.add(COLUMN_FAMILIY_INFO,Bytes.toBytes("ptime"),Bytes.toBytes(predictTime));
            context.write(new ImmutableBytesWritable(rowKey), condition);
        }
    }


    public int run(String[] strings) throws Exception {

        // 建表
        HbaseTool htool = new HbaseTool();
        // ActiveTrace_Out
        htool.createHbaseTable(strings[2]);

        logger.error("建表成功");

        // 生成数据
        HTable table = null;
        try {
            transferConf(strings[2]);
            Job job = JobGenerator.HbaseQuickImportJobGnerator(this, this.getConf(),strings, table);
            job.setJobName("ActiveTrace2Hbase-"+strings[2]);
            job.setMapperClass(AtMapper.class);
            job.getConfiguration().setStrings("mapreduce.reduce.shuffle.input.buffer.percent", "0.1");

            if (job.waitForCompletion(true)){
                logger.error("生成 hfile 成功");
                // bulkload
                ToolRunner.run(new BulkLoadTool(),new String[]{strings[2],strings[1]});
                logger.error("导入成功");

            }else{
                logger.error("生成 hfile 失败");
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (table!=null)    table.close();
        }
        return 0;
    }

    private void transferConf(String tableName) {
        if (tableName.toLowerCase().contains(tableTagIn)){
            this.getConf().set(tableTag,tableTagIn);
            return;
        }
        this.getConf().set(tableTag, "out");
    }

}
