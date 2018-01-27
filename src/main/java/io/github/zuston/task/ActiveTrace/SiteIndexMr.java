package io.github.zuston.task.ActiveTrace;

import io.github.zuston.util.BulkLoadTool;
import io.github.zuston.util.HbaseTool;
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
 * Created by zuston on 2018/1/23.
 */
// siteID -------> ewbNO#ewbNO#ewbNO#ewbNO#ewbNO#ewbNO
public class SiteIndexMr extends Configured implements Tool {

    public static final String tableName_IN = "siteIndex_In";

    public static final String tableName_OUT = "siteIndex_Out";

    static class SiteIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

        OriginalTraceRecordParser parser = new OriginalTraceRecordParser();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] lines = value.toString().split("\\t+");
            String record = lines[1];
            String header = lines[0];
            if (!parser.parser(record) || header.split("#").length!=2){
                context.getCounter("RelationIndex","parser_error").increment(1);
                return;
            }
            String tag = context.getConfiguration().get("tag");
            if (tag.equals("in")){
                context.write(new Text(header.split("#")[1]), new Text(parser.getEWB_NO()));
                return;
            }
            context.write(new Text(parser.getSITE_ID()),new Text(parser.getEWB_NO()));
        }
    }

    static class SiteIndexReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> reslist = new ArrayList<String>();
            int count = 0;
            for (Text value : values){
                reslist.add(value.toString());
                if (count >= 100)   break;
                count ++;
            }
            String indexLine = StringUtils.join(reslist, "#");
            context.write(key,new Text(indexLine));
        }
    }

    static final byte[] COLUMN_FAMILIY_INFO = Bytes.toBytes("info");
    static class SiteIndex2HbaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] lines = value.toString().split("\\t+");
            byte[] rowKey = Bytes.toBytes(lines[0]);
            Put condition = new Put(rowKey);
            condition.add(COLUMN_FAMILIY_INFO,Bytes.toBytes("index"),Bytes.toBytes(lines[1]));
            context.write(new ImmutableBytesWritable(rowKey), condition);
        }
    }

    // 第四个参数 为 in or out
    @Override
    public int run(String[] strings) throws Exception {
        this.getConf().set("tag",strings[3]);
        String tableName = strings[3].equals("in") ? tableName_IN : tableName_OUT;
        if (!generateIndexHfile(strings)) return -1;
        String [] options = new String[]{
                strings[1],
                "/A_SITE_INDEX_"+strings[3],
                tableName
        };
        import2HBase(options, tableName);
        return 1;
    }

    private boolean generateIndexHfile(String[] strings) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = JobGenerator.SimpleJobGenerator(this, this.getConf(), strings);
        job.setJarByClass(SiteIndexMr.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setMapperClass(SiteIndexMapper.class);
        job.setReducerClass(SiteIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (job.waitForCompletion(true))    return true;
        return false;
    }

    private void import2HBase(String[] strings, String tableName) throws IOException {
        HbaseTool htool = new HbaseTool();
        htool.createHbaseTable(tableName);
        HTable table = null;
        try {
            Job job = JobGenerator.HbaseQuickImportJobGnerator(this, this.getConf(), strings, table);
            job.setJobName("Index2Hbase");
            job.setMapperClass(SiteIndex2HbaseMapper.class);
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
