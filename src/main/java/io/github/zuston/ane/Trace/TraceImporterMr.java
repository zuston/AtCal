package io.github.zuston.ane.Trace;

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

/**
 * Created by zuston on 2017/12/22.
 */
public class TraceImporterMr extends Configured implements Tool {

    public static final byte[] HS_OPT_TRACE_COLUMN_FAMILY_SITES = Bytes.toBytes("site");

    /**
     *
     *     site_name ------> sites:A B C D E F
     */
    static class HS_OPT_TRACE_ImporterMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] arrs = value.toString().split("\\s+");
            String averageTime = arrs[1];

            if (arrs[0].split("#").length!=2){
                return;
            }

            String startSite = arrs[0].split("#")[0];
            String endSite = arrs[0].split("#")[1];

            byte[] rowKey = Bytes.toBytes(startSite);
            Put putCondition = new Put(rowKey);
            putCondition.add(
                    HS_OPT_TRACE_COLUMN_FAMILY_SITES,
                    Bytes.toBytes(endSite),
                    Bytes.toBytes(averageTime)
            );
            context.write(new ImmutableBytesWritable(rowKey), putCondition);
        }
    }

    public int run(String[] strings) throws Exception {
        Job job = JobGenerator.HbaseImportJobGnerator(this, this.getConf(),strings);
        job.setJobName("TraceTime2Hbase");
        job.setMapperClass(HS_OPT_TRACE_ImporterMapper.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
