package io.github.zuston.ane.Trace;

import io.github.zuston.ane.Util.JobGenerator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by zuston on 2017/12/18.
 */

enum COUNTER {
    SITE_ID_MISSING,

    ID_2_NAME_ERROR
}

public class OrderTimeMr extends Configured implements Tool {

    static class OrderTimeMapper extends Mapper<LongWritable, Text, Text, Text>{
        private TraceRecordParser parser = new TraceRecordParser();

        Text tempTextValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!parser.parse(value.toString())) return;
            String ewb_no = parser.getEwb_no();
            tempTextValue.set(ewb_no);
            context.write(tempTextValue,value);
        }
    }

    static class OrderTimeReduer extends Reducer<Text, Text, Text, LongWritable>{
        private TraceRecordParser parser = new TraceRecordParser();

        Text tempKeyText = new Text();
        LongWritable tempValueLong = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // 按照时间来排序,聚合的应该是完整的一个订单
            List<OrderEntity> orderEntities = new ArrayList<OrderEntity>();
            for (Text value : values){
                parser.parse(value.toString());
                String scan_time = parser.getScan_time();
                String site_name = parser.getSite_name();
                String des_site_name = parser.getDes_site_name();
                String desp = parser.getDesp();
                String site_id = parser.getSite_id();
                if (site_id == null){
                    context.getCounter(COUNTER.SITE_ID_MISSING).increment(1);
                    continue;
                }
                OrderEntity orderEntity = new OrderEntity(scan_time, site_name, des_site_name, desp, site_id);
                orderEntities.add(orderEntity);
            }

            // 排序运单时间
            Collections.sort(orderEntities, new Comparator<OrderEntity>() {
                public int compare(OrderEntity o1, OrderEntity o2) {
                    return (int) (Timestamp.valueOf(o1.getScan_time()).getTime() - Timestamp.valueOf(o2.getScan_time()).getTime());
                }
            });
            // TODO: 2017/12/18 检测订单流程的完整性
            // 依次按照运单来计算时间
            for (int i=1; i<orderEntities.size(); i++){

                OrderEntity startEntity = orderEntities.get(i-1);
                OrderEntity endEntity = orderEntities.get(i);
                // filter,剔除脏数据
                if (!filter(startEntity,endEntity))  continue;

                long timePlus = Timestamp.valueOf(endEntity.getScan_time()).getTime()-
                        Timestamp.valueOf(startEntity.getScan_time()).getTime();
                tempKeyText.set(startEntity.getSite_id()+"#"+endEntity.getSite_id());
                // sort by minutes
                tempValueLong.set(timePlus/1000/60);
                context.write(
                        tempKeyText,
                        tempValueLong
                );
            }
        }

        private boolean filter(OrderEntity startEntity, OrderEntity endEntity) {
            String startLineDesp = startEntity.getDesp();
            String endLineSiteName = endEntity.getSite_name();
            if (startLineDesp.contains(endLineSiteName))
                return true;
            return false;
        }
    }


    public int run(String[] strings) throws Exception {
        Job job = JobGenerator.SimpleJobGenerator(this, getConf(), strings);
        if (job == null)    return -1;

        job.setJarByClass(OrderTimeMr.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(OrderTimeMapper.class);
        job.setReducerClass(OrderTimeReduer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // TODO: 2017/12/19 待调优 reducer 数目
//        job.setNumReduceTasks(1);

        return job.waitForCompletion(true) ? 0 : 1;

    }
}


class OrderEntity{
    private String scan_time;
    private String site_name;
    private String des_site_name;
    // 运单状态描述
    private String desp;
    private String site_id;

    public OrderEntity(String scan_time, String site_name, String des_site_name, String desp, String site_id) {
        this.scan_time = scan_time;
        this.site_name = site_name;
        this.des_site_name = des_site_name;
        this.desp = desp;
        this.site_id = site_id;
    }

    public String getScan_time() {
        return scan_time;
    }

    public String getDesp() {
        return desp;
    }

    public String getSite_name() {
        return site_name;

    }

    public String getSite_id(){
        return site_id;
    }

    public String getDes_site_name() {
        return des_site_name;
    }
}
