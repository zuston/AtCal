package spark.core.predict;

import io.github.zuston.basic.Entity.OrderEntity;
import io.github.zuston.basic.TraceTime.TraceRecordParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by zuston on 2018/2/27.
 */
public class OrderRdd implements Serializable {

    public static TraceRecordParser parser = new TraceRecordParser();

    public static final String  BLANK = "&";

    public static final String AGGRETE_BLANK = "$";

    public void orderRdd_action(){
        String traceHdfsPath = "/aneInput/hs_opt_trace/export_10.txt";
        SparkConf conf = new SparkConf().setAppName("orderRdd_action");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> traceData = sc.textFile(traceHdfsPath).cache();
        
        // 每一笔订单的 rdd 算子
        // TODO: 2018/2/27 时间上施加限制，使得符合一个月的期限 
        JavaPairRDD<String, String> traceTimeRdd = traceData.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                if (!parser.parse(s)) return null;
                String ewb_no = parser.getEwb_no();
                return new Tuple2<String, String>(ewb_no, s);
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s+BLANK+s2;
            }
        }).mapToPair(new OrderAggrete()).
                flatMapToPair(s -> {
                    String aggreteString = s._2();
                    String oneLineStringArr[] = aggreteString.split(BLANK);
                    List<Tuple2<String, String>> results = new ArrayList<Tuple2<String, String>>();
                    for (String oneLine : oneLineStringArr){
                        String keyValueArr [] = oneLine.split(AGGRETE_BLANK);
                        if (keyValueArr.length!=2)  continue;
                        results.add(new Tuple2<>(keyValueArr[0],keyValueArr[1]));
                    }
                    return results.iterator();
                });

        traceTimeRdd.saveAsTextFile("/sparkData/tmp/orderTime/");
        sc.close();
    }


    /**
     * 将同一订单下的时间计算出来
     */
    class OrderAggrete implements PairFunction<Tuple2<String, String>, String, String>{

        @Override
        public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
            String ewbNo = stringStringTuple2._1();
            String aggreteLine = stringStringTuple2._2();
            String [] lineArr = aggreteLine.split(BLANK);

            List<OrderEntity> orderEntities = new ArrayList<OrderEntity>();
            String recordTime = "";
            int tagCount = 0;

            for (String value : lineArr){
                parser.parse(value.toString());
                String scan_time = parser.getScan_time();
                if (tagCount==0){
                    recordTime = scan_time;
                }
                tagCount ++ ;
                String site_name = parser.getSite_name();
                String des_site_name = parser.getDes_site_name();
                String desp = parser.getDesp();
                String site_id = parser.getSite_id();
                if (site_id == null){
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

            recordTime = recordTime.substring(0,7);

            // 时间和地点的聚合,便于下一步 FlatMap
            String timeAndSite = "";

            // TODO: 2017/12/18 检测订单流程的完整性
            // 依次按照运单来计算时间
            for (int i=1; i<orderEntities.size(); i++){

                OrderEntity startEntity = orderEntities.get(i-1);
                OrderEntity endEntity = orderEntities.get(i);
                // filter,剔除脏数据
                if (!filter(startEntity,endEntity))  continue;

                long timePlus = Timestamp.valueOf(endEntity.getScan_time()).getTime()-
                        Timestamp.valueOf(startEntity.getScan_time()).getTime();

                String setKey = startEntity.getSite_id()+"#"+endEntity.getSite_id()+"#"+recordTime;
                String setValue = String.valueOf(timePlus/1000/60);

                timeAndSite += setKey + AGGRETE_BLANK + setValue + BLANK;
            }

            timeAndSite = timeAndSite.substring(0,timeAndSite.length()-1);
            return new Tuple2<>(ewbNo, timeAndSite);
        }

        private boolean filter(OrderEntity startEntity, OrderEntity endEntity) {
            String startLineDesp = startEntity.getDesp();
            String endLineSiteName = endEntity.getSite_name();
            if (startLineDesp.contains(endLineSiteName))
                return true;
            return false;
        }
    }
}

