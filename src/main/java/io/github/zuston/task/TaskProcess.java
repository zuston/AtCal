package io.github.zuston.task;

import io.github.zuston.task.ActiveTrace.*;
import io.github.zuston.task.ValidateTraceTime.Validate;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by zuston on 2018/1/19.
 */

// 将分散的分析操作聚合化
public class TaskProcess extends Configured implements Tool {

    /**
     * 两个参数
     * 1， 日期
     * 2.  范围 default 值
     * @param args
     * @return
     * @throws Exception
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
        String traceInputPath = "/aneInput/hs_opt_trace";
        String predictTimePath = "/aneTrace-with-time-filter";

        String filterOutputPath = "/A_1_filter";

        String [] filterOpts = new String[]{
                traceInputPath,
                filterOutputPath,
                reducerNum,
                date,
                defaultv
        };

        String distinctPath = "/A_2_distinct";
        String [] distinctOpts = new String[]{
                filterOutputPath,
                distinctPath,
                reducerNum
        };

        String indexPath_OUT = "/A_2_1_index_OUT";
        String [] indexOpts_OUT = new String[]{
                distinctPath,
                indexPath_OUT,
                reducerNum,
                "out"
        };

        String indexPath_IN = "/A_2_1_index_IN";
        String [] indexOpts_IN = new String[]{
                distinctPath,
                indexPath_IN,
                reducerNum,
                "in"
        };

        String siteIndexPath_OUT = "/A_2_2_siteIndex_OUT";
        String [] siPathOpts_OUT = new String[]{
                distinctPath,
                siteIndexPath_OUT,
                reducerNum,
                "out"
        };

        String siteIndexPath_IN = "/A_2_2_siteIndex_IN";
        String [] siPathOpts_IN = new String[]{
                distinctPath,
                siteIndexPath_IN,
                reducerNum,
                "in"
        };

        String mergePath = "/A_3_merge";
        String [] mergeOpts = new String[] {
                distinctPath,
                predictTimePath,
                mergePath,
                reducerNum
        };

        String mysqlActiveTracePath = "/A_4_activeTrace2mysql";
        String [] _2mysqlOpts = new String[]{
                mergePath,
                mysqlActiveTracePath,
                "0"
        };

        String hbaseActiveTracePath_OUT = "/A_4_activeTrace2hbase_OUT";
        String [] _2HbaseOpts_OUT = new String[]{
                mergePath,
                hbaseActiveTracePath_OUT,
                "ActiveRecord_Out"
        };

        String hbaseActiveTracePath_IN = "/A_4_activeTrace2hbase_IN";
        String [] _2HbaseOpts_IN = new String[]{
                mergePath,
                hbaseActiveTracePath_IN,
                "ActiveRecord_Out"
        };

        String validatePath = "/A_5_validate";
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
        ToolRunner.run(new RelationIndexMr(), indexOpts_IN);

        // siteID =======> EWB#EWB
        ToolRunner.run(new SiteIndexMr(), siPathOpts_OUT);
        ToolRunner.run(new SiteIndexMr(), siPathOpts_IN);

        // 合并预测时间
        ToolRunner.run(new Merge2ActiveTrace(), mergeOpts);

//        ToolRunner.run(new ActiveTrace2Mysql(), _2mysqlOpts);

        // 链路数据进 hbase
        ToolRunner.run(new ActiveTrace2Hbase(), _2HbaseOpts_OUT);
        ToolRunner.run(new ActiveTrace2Hbase(), _2HbaseOpts_IN);

        // 实时异常件反馈
        ToolRunner.run(new Validate(), validateOpts);


//        HdfsTool.deleteDir(filterOutputPath);
//        HdfsTool.deleteDir(distinctPath);
//        HdfsTool.deleteDir(mergePath);
//        HdfsTool.deleteDir(mysqlActiveTracePath);
//        HdfsTool.deleteDir(validatePath);

        return 0;
    }

    private String currentTime(){
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        return df.format(new Date());// new Date()为获取当前系统时间
    }

}
