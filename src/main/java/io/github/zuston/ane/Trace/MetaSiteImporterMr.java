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
 * Created by zuston on 2017/12/29.
 */
enum META_COUNTER{
    ID_2_ERROR
}

public class MetaSiteImporterMr extends Configured implements Tool {

    public static final byte[] META_FAMILY_COLUMN_SITES = Bytes.toBytes("site");
    public static final byte[] META_CHILD_COLUMN_NAME = Bytes.toBytes("name");

    static class Meta_ImporterMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] splitArr = value.toString().split("\\s+");
            if (splitArr.length != 2){
                context.getCounter(META_COUNTER.ID_2_ERROR).increment(1);
                return;

            }
            String siteId = splitArr[0];
            String siteName = splitArr[1];
            if (siteId.equals("") || siteId == null || siteName.equals("") || siteName == null){
                context.getCounter(META_COUNTER.ID_2_ERROR).increment(1);
                return;
            }
            byte [] rowKey = Bytes.toBytes(siteId);
            Put condition = new Put(rowKey);
            condition.add(
                    META_FAMILY_COLUMN_SITES,
                    META_CHILD_COLUMN_NAME,
                    Bytes.toBytes(siteName)
                    );
            context.write(new ImmutableBytesWritable(rowKey), condition);
        }
    }

    public int run(String[] strings) throws Exception {
        Job job = JobGenerator.HbaseImportJobGnerator(this, this.getConf(),strings);
        job.setJobName("Id2NameImporter");
        job.setMapperClass(MetaSiteImporterMr.Meta_ImporterMapper.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
