package io.github.zuston.task;

import io.github.zuston.task.ActiveTrace.*;
import io.github.zuston.task.ValidateTraceTime.Validate;
import io.github.zuston.util.FileTool;
import io.github.zuston.util.HdfsTool;
import io.github.zuston.util.RedisTool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Created by zuston on 2018/1/19.
 */

// 将分散的分析操作聚合化
public class TaskProcess extends Configured implements Tool {

    // 设定的日期的 redis key 值
    public static final String DATE_KEY = "TASK_PROCESS_CACHE_DATE";

    // 中间状态文件
    public static final String statusFilePath = "aneTmp";
    // date 缓存文件目录
    public static final String cacheFilePath = statusFilePath + "/cache";
    // 执行顺序目录
    public static final String actOrderPath = statusFilePath + "/order";

    // 输入最原始文件路径
    public static final String traceInputPath = "/aneInput/hs_opt_trace";
    // 预测时间的文件 hdfs 地址
    public static final String predictTimePath = "/aneTrace-with-time-filter";
    // 设定的reduce数目
    public static final String reduceNum = "60";
    // 从原始数据中抽取出数据之后的 输出目录
    public static final String filterOutputPath = "/temp/A_1_filter";
    // 合成预测时间的 原始数据
    public static final String mergePath = "/temp/A_3_merge";

    /**
     * 传入参数，设定日期
     * section 值
     * @param args
     * @return
     */
    public int taskFirst(String[] args) throws Exception {
        if (!checkOrder(1))    throw new Exception("请按照执行顺序，执行");
        String date = args[0];
        String section = args.length >= 2 ? args[1] : "7";

        cacheDate2File(date);

        String [] filterOpts = new String[]{
                traceInputPath,
                filterOutputPath,
                reduceNum,
                date,
                section
        };
        ToolRunner.run(new FilterCurrentActiveTrace(), filterOpts);
        cacheOrder(1);
        cache2Redis(date);
        return 0;
    }

    private void cache2Redis(String date) throws Exception {
        if (!RedisTool.set(DATE_KEY,date))  throw new Exception("date cache to redis is error");
    }

    private boolean checkOrder(int i) {
        List<String> tagList = FileTool.getDirNames(actOrderPath);
        // 校验下面没有标记位
        if (i==1){
            if (tagList == null || tagList.size()==0)  return true;
            return false;
        }
        // 执行到第二步时候，第一步的标记位需要有
        if (i==2){
            if (tagList !=null && tagList.size()==1 && tagList.get(0).equals("1"))    return true;
            return false;
        }
        if (i==3){
            if (tagList != null && tagList.size()==2 && tagList.contains("1") && tagList.contains("2"))    return true;
            return false;
        }
        return false;
    }

    private boolean cacheOrder(int i) {
        return FileTool.createDir(actOrderPath + "/" + i);
    }

    private boolean cacheDate2File(String date) {
        return FileTool.createDir(cacheFilePath + "/date/" + date);
    }

    public int taskSecond(String[] args) throws Exception {
        if (!checkOrder(2)) throw new Exception("当前为第二阶段，请按照执行顺序执行");
        String distinctPath = "/temp/A_2_distinct";
        String [] distinctOpts = new String[]{
                filterOutputPath,
                distinctPath,
                reduceNum
        };

        String indexPath_OUT = "/temp/A_2_1_index_OUT";
        String [] indexOpts_OUT = new String[]{
                distinctPath,
                indexPath_OUT,
                reduceNum
        };


        String siteIndexPath_OUT = "/temp/A_2_2_siteIndex_OUT";
        String [] siPathOpts_OUT = new String[]{
                distinctPath,
                siteIndexPath_OUT,
                reduceNum,
                "out"
        };

        String siteIndexPath_IN = "/temp/A_2_3_siteIndex_IN";
        String [] siPathOpts_IN = new String[]{
                distinctPath,
                siteIndexPath_IN,
                reduceNum,
                "in"
        };

        String [] mergeOpts = new String[] {
                distinctPath,
                predictTimePath,
                mergePath,
                reduceNum
        };

        String hbaseActiveTracePath_OUT = "/temp/A_4_activeTrace2hbase_OUT";
        String [] _2HbaseOpts_OUT = new String[]{
                mergePath,
                hbaseActiveTracePath_OUT,
                "ActiveRecord"
        };
        ToolRunner.run(new DistinctActiveTrace(), distinctOpts);
        ToolRunner.run(new RelationIndexMr(), indexOpts_OUT);
        ToolRunner.run(new SiteIndexMr(), siPathOpts_OUT);
        ToolRunner.run(new SiteIndexMr(), siPathOpts_IN);
        ToolRunner.run(new Merge2ActiveTrace(), mergeOpts);
        ToolRunner.run(new ActiveTrace2Hbase(), _2HbaseOpts_OUT);
        HdfsTool.deleteDir(distinctPath);
        HdfsTool.deleteDir(indexPath_OUT);
        HdfsTool.deleteDir(siteIndexPath_OUT);
        HdfsTool.deleteDir(siteIndexPath_IN);
        HdfsTool.deleteDir(hbaseActiveTracePath_OUT);
        HdfsTool.deleteDir(filterOutputPath);
        cacheOrder(2);
        return 0;
    }


    // TODO: 2018/1/30 debug阶段
    public int taskThird(String[] args) throws Exception {
        if (!checkOrder(3)) throw new Exception("当前第三阶段，请按照执行顺序执行");
        String date = getCacheDate();
        if (date==null) throw new Exception("date缓存不存在，请确认执行步骤!");
        String validatePath = "/temp/C_5_validate";
        String currentTime = currentTime();
        String validateTime = date + " " + currentTime.split("\\s")[1];
        String [] validateOpts = new String[]{
                mergePath,
                validatePath,
                reduceNum,
                validateTime
        };
        ToolRunner.run(new Validate(), validateOpts);
        HdfsTool.deleteDir(validatePath);
        HdfsTool.deleteDir(mergePath);
        resetOrder();
        return 0;
    }

    private String getCacheDate() {
        return FileTool.getSimpleDirName(cacheFilePath+"/date");
    }

    private void resetOrder() {
        String order1 = actOrderPath + "/1";
        String order2 = actOrderPath + "/2";
        String cacheDate  = cacheFilePath + "/date";
        FileTool.delete(new File(order1));
        FileTool.delete(new File(order2));
        FileTool.delete(new File(cacheDate));
    }


    /**
     * 两个参数
     * 1， 日期
     * 2.  范围 default 值， 以日期为中间点做辐射
     * @param args
     * @return
     * @throws Exception
     * 改成分为三个阶段
     */
    @Override
    public int run(String[] args) throws Exception {
//                * 参数1  输入全部 trace 文件，固定了
//                * 参数2  输出
//                * 参数3  reduce 数目
//                * 参数4  设定的时间 例如 2018-08-13
//                * 参数5  上下浮动的日期, default值为 7

        String date = args[0];
        String defaultv = (args.length>=2) ? args[1] : "7";

        String reducerNum = "60";
        // 输入最原始文件路径
        String traceInputPath = "/aneInput/hs_opt_trace";
        // 预测时间的文件 hdfs 地址
        String predictTimePath = "/aneTrace-with-time-filter";


        String filterOutputPath = "/temp/A_1_filter";

        String [] filterOpts = new String[]{
                traceInputPath,
                filterOutputPath,
                reducerNum,
                date,
                defaultv
        };

        String distinctPath = "/temp/A_2_distinct";
        String [] distinctOpts = new String[]{
                filterOutputPath,
                distinctPath,
                reducerNum
        };

        String indexPath_OUT = "/temp/A_2_1_index_OUT";
        String [] indexOpts_OUT = new String[]{
                distinctPath,
                indexPath_OUT,
                reducerNum
        };


        String siteIndexPath_OUT = "/temp/A_2_2_siteIndex_OUT";
        String [] siPathOpts_OUT = new String[]{
                distinctPath,
                siteIndexPath_OUT,
                reducerNum,
                "out"
        };

        String siteIndexPath_IN = "/temp/A_2_3_siteIndex_IN";
        String [] siPathOpts_IN = new String[]{
                distinctPath,
                siteIndexPath_IN,
                reducerNum,
                "in"
        };

        String mergePath = "/temp/A_3_merge";
        String [] mergeOpts = new String[] {
                distinctPath,
                predictTimePath,
                mergePath,
                reducerNum
        };

        String mysqlActiveTracePath = "/temp/A_4_activeTrace2mysql";
        String [] _2mysqlOpts = new String[]{
                mergePath,
                mysqlActiveTracePath,
                "0"
        };

        String hbaseActiveTracePath_OUT = "/temp/A_4_activeTrace2hbase_OUT";
        String [] _2HbaseOpts_OUT = new String[]{
                mergePath,
                hbaseActiveTracePath_OUT,
                "ActiveRecord"
        };

//        String hbaseActiveTracePath_IN = "/A_4_activeTrace2hbase_IN";
//        String [] _2HbaseOpts_IN = new String[]{
//                mergePath,
//                hbaseActiveTracePath_IN,
//                "ActiveRecord_In"
//        };

        String validatePath = "/temp/A_5_validate";
        String currentTime = currentTime();
        String validateTime = date + " " + currentTime.split("\\s")[1];
        String [] validateOpts = new String[]{
                mergePath,
                validatePath,
                reducerNum,
                validateTime
        };

        // 前置筛选过滤数据
        ToolRunner.run(new FilterCurrentActiveTrace(), filterOpts);
        ToolRunner.run(new DistinctActiveTrace(), distinctOpts);

        // ewb -----> siteID#siteID
        ToolRunner.run(new RelationIndexMr(), indexOpts_OUT);

        // siteID =======> EWB#EWB
        ToolRunner.run(new SiteIndexMr(), siPathOpts_OUT);
        ToolRunner.run(new SiteIndexMr(), siPathOpts_IN);

        // 合并预测时间
        ToolRunner.run(new Merge2ActiveTrace(), mergeOpts);

//        ToolRunner.run(new ActiveTrace2Mysql(), _2mysqlOpts);

        // 链路数据进 hbase
        ToolRunner.run(new ActiveTrace2Hbase(), _2HbaseOpts_OUT);

        // 实时异常件反馈
        ToolRunner.run(new Validate(), validateOpts);

//        HdfsTool.deleteDir(filterOutputPath);
//        HdfsTool.deleteDir(distinctPath);
//        HdfsTool.deleteDir(mergePath);
//        HdfsTool.deleteDir(mysqlActiveTracePath);
//        HdfsTool.deleteDir(validatePath);

        return 0;
    }

    public static String currentTime(){
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        return df.format(new Date());// new Date()为获取当前系统时间
    }


    public static void main(String[] args) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        String a = df.format(new Date());// new Date()为获取当前系统时间
        String validateTime = "2017-10-10" + " " + a.split("\\s")[1];
        System.out.println(validateTime);
    }

}
