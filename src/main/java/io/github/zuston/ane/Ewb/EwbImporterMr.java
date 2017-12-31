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

/**
 * Created by zuston on 2017/12/29.
 */
public class EwbImporterMr extends Configured implements Tool {

    public static final byte[] EWB_FAMILY_COLUMN = Bytes.toBytes("info");

    public static EwbRecordParser parser = new EwbRecordParser();


    static class EwbImporterMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parser(value.toString());
            byte[] rowKey = Bytes.toBytes("");
            Put putCondition = new Put(rowKey);
            putCondition.add(
                    EWB_FAMILY_COLUMN,
                    Bytes.toBytes(""),
                    Bytes.toBytes("")
            );
            context.write(new ImmutableBytesWritable(rowKey), putCondition);
        }
    }

    public int run(String[] strings) throws Exception {
        Job job = JobGenerator.HbaseImportJobGnerator(this, this.getConf(),strings);
        job.setJobName("Ewb2Hbase");
        job.setMapperClass(EwbImporterMr.EwbImporterMapper.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
