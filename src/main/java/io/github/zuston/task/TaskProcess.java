package io.github.zuston.task;

import io.github.zuston.Util.HdfsTool;
import io.github.zuston.task.ActiveTrace.ActiveTrace2Mysql;
import io.github.zuston.task.ActiveTrace.DistinctActiveTrace;
import io.github.zuston.task.ActiveTrace.FilterCurrentActiveTrace;
import io.github.zuston.task.ActiveTrace.Merge2ActiveTrace;
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

        String reducerNum = "10";
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

        String validatePath = "/A_5_validate";
        String currentTime = currentTime();
        String validateTime = date + " " + currentTime.split("\\s")[1];
        String [] validateOpts = new String[]{
                mergePath,
                validatePath,
                reducerNum,
                validateTime
        };

        ToolRunner.run(new FilterCurrentActiveTrace(), filterOpts);
        ToolRunner.run(new DistinctActiveTrace(), distinctOpts);
        ToolRunner.run(new Merge2ActiveTrace(), mergeOpts);
        ToolRunner.run(new ActiveTrace2Mysql(), _2mysqlOpts);
        ToolRunner.run(new Validate(), validateOpts);

        HdfsTool.deleteDir(filterOutputPath);
        HdfsTool.deleteDir(distinctPath);
        HdfsTool.deleteDir(mergePath);
        HdfsTool.deleteDir(mysqlActiveTracePath);
        HdfsTool.deleteDir(validatePath);

        return 0;
    }

    private String currentTime(){
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        return df.format(new Date());// new Date()为获取当前系统时间
    }

}
