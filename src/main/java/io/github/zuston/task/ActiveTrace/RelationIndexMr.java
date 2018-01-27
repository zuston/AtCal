package io.github.zuston.task.ActiveTrace;

import io.github.zuston.util.BulkLoadTool;
import io.github.zuston.util.HbaseTool;
import io.github.zuston.util.HdfsTool;
import io.github.zuston.util.JobGenerator;
import io.github.zuston.basic.Trace.OriginalTraceRecordParser;
import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zuston on 2018/1/22.
 */
// ewb_no --> siteID#siteID#siteID#siteID
public class RelationIndexMr extends Configured implements Tool{

    public static final String tableName_Out = "ewbIndex_Out";
    public static final String tableName_In = "ewbIndex_In";

    public static final String TABLE = "ewbIndex";

    static class RelationIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

        OriginalTraceRecordParser parser = new OriginalTraceRecordParser();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] lines = value.toString().split("\\t+");
            String header = lines[0];
            String record = lines[1];
            if (!parser.parser(record) || header.split("#").length!=2){
                context.getCounter("RelationIndex","parser_error").increment(1);
                return;
            }
//            String tag = context.getConfiguration().get("tag");
//            if (tag.equals("in")){
//                context.write(new Text(parser.getEWB_NO()),new Text(header.split("#")[1]));
//                return;
//            }
            context.write(new Text(parser.getEWB_NO()),new Text(parser.getTRACE_ID()));
        }
    }

    static class RelationIndexReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> reslist = new ArrayList<String>();
            for (Text value : values){
                reslist.add(value.toString());
            }
            String indexLine = StringUtils.join(reslist, "%");
            context.write(key,new Text(indexLine));
        }
    }

    // 索引导入 hbase 中
    static final byte[] COLUMN_FAMILIY_INFO = Bytes.toBytes("info");
    static class RelationIndexImport2Hbase extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] lines = value.toString().split("\\t+");
            byte[] rowKey = Bytes.toBytes(lines[0]);
            Put condition = new Put(rowKey);
            condition.add(COLUMN_FAMILIY_INFO,Bytes.toBytes("index"),Bytes.toBytes(lines[1]));
            context.write(new ImmutableBytesWritable(rowKey), condition);
        }
    }

    /**
     * 输入文件
     * 输出文件
     * reducer 个数
     * @param strings
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] strings) throws Exception {
//        this.getConf().set("tag",strings[3]);
//        String tableName = strings[3].equals("in") ? tableName_In : tableName_Out;
        String hFileMidlleFilePath = "/temp/A_ewbIndexHfile";
        if (!generateIndexHfile(strings)) return -1;
        String [] options = new String[]{
                strings[1],
                hFileMidlleFilePath,
                TABLE
        };
        import2HBase(options, TABLE);
        HdfsTool.deleteDir(hFileMidlleFilePath);
        return 1;
    }

    private boolean generateIndexHfile(String[] strings) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = JobGenerator.SimpleJobGenerator(this, this.getConf(), strings);
        job.setJarByClass(RelationIndexMr.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setMapperClass(RelationIndexMapper.class);
        job.setReducerClass(RelationIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (job.waitForCompletion(true))    return true;
        return false;
    }

    private void import2HBase(String [] strings, String tableName) throws IOException {
        HbaseTool htool = new HbaseTool();
        htool.createHbaseTable(tableName);
        HTable table = null;
        try {
            Job job = JobGenerator.HbaseQuickImportJobGnerator(this, this.getConf(), strings, table);
            job.setJobName("Index2Hbase");
            job.setMapperClass(RelationIndexImport2Hbase.class);
            job.getConfiguration().setStrings("mapreduce.reduce.shuffle.input.buffer.percent", "0.1");

            if (job.waitForCompletion(true)) {
                // bulkload
                ToolRunner.run(new BulkLoadTool(), new String[]{strings[2], strings[1]});
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
