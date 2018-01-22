package io.github.zuston;

import io.github.zuston.Util.BulkLoadTool;
import io.github.zuston.Util.HbaseTool;
import io.github.zuston.basic.Ewb.EwbDataSampleCollector;
import io.github.zuston.basic.Ewb.EwbImporterMr;
import io.github.zuston.basic.Trace.OriginalTraceImporterMr;
import io.github.zuston.basic.Trace.TraceDataSampleCollector;
import io.github.zuston.basic.TraceTime.*;
import io.github.zuston.task.ActiveTrace.*;
import io.github.zuston.task.TaskProcess;
import io.github.zuston.task.ValidateTraceTime.Validate;
import io.github.zuston.task.ValidateTraceTime.Validate2Mysql;
import io.github.zuston.test.HdfsToolTest;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zuston on 2017/12/18.
 */
public class Init {
    public static Logger loggerFactory = LoggerFactory.getLogger(Init.class);

    public static HashMap<String, Integer> commandHm = new HashMap<String, Integer>();
    public static final int ORDER_NUMBER = 1;
    public static final int TRACE_NUMBER = 2;
    public static final int TRACE_IMPORT_NUMBER = 3;
    public static final int SITE_2_NAME = 4;
    public static final int SITE_2_NAME_IMP = 5;
    public static final int TRACE_COMPAR = 6;
    public static final int FILTER = 7;

    public static final int IMPTRACE = 8;
    public static final int IMPEWB = 9;

    public static final int EWB_SAMPLE = 10;
    public static final int CREATE_TABLE = 11;

    public static final int BULKLOAD = 12;

    public static final int TRACE_SAMPLE = 13;

    public static final int FILTER_TRACE_OF_TIME = 14;
    public static final int DISTINCT_ACTIVE_TRACE = 17;
    public static final int MERGE_PREDICT_TIME_2_ACTIVE_TRACE = 15;
    public static final int AT2HBAE = 18;
    public static final int VALIDATE = 19;

    public static final int AT2MYSQL = 20;

    public static final int V2MYSQL = 21;


    public static final int ANE = 22;

    public static final int RELATION_INDEX = 23;


    public static final int HDFS_TEST = 16;

    static {
        commandHm.put("order", ORDER_NUMBER);
        commandHm.put("trace", TRACE_NUMBER);
        commandHm.put("imptracetime".toLowerCase(), TRACE_IMPORT_NUMBER);
        commandHm.put("site2name".toLowerCase(), SITE_2_NAME);
        commandHm.put("site2nameImp".toLowerCase(), SITE_2_NAME_IMP );
        commandHm.put("traceCompar".toLowerCase(), TRACE_COMPAR);
        commandHm.put("filter".toLowerCase(), FILTER);

        commandHm.put("impewb".toLowerCase(), IMPEWB);
        commandHm.put("sampleewb", EWB_SAMPLE);
        commandHm.put("createtable", CREATE_TABLE);
        commandHm.put("bulkload", BULKLOAD);

        commandHm.put("imptrace".toLowerCase(), IMPTRACE);
        commandHm.put("sampletrace", TRACE_SAMPLE);

        commandHm.put("filtertraceoftime".toLowerCase(), FILTER_TRACE_OF_TIME);
        commandHm.put("distincttrace".toLowerCase(), DISTINCT_ACTIVE_TRACE);
        commandHm.put("merge2activetrace".toLowerCase(), MERGE_PREDICT_TIME_2_ACTIVE_TRACE);
        commandHm.put("at2hbase".toLowerCase(), AT2HBAE);

        commandHm.put("at2mysql".toLowerCase(), AT2MYSQL);

        commandHm.put("validate".toLowerCase(), VALIDATE);

        commandHm.put("validate2mysql".toLowerCase(), V2MYSQL);

        commandHm.put("ane", ANE);

        commandHm.put("ri",RELATION_INDEX);

        commandHm.put("hdfstest".toLowerCase(), HDFS_TEST);
    }

    public static void main(String[] args) throws Exception {

        String commandOption = args[0].toLowerCase();
        String reducerNum = args.length<=3 ? "1" : args[3];
        String [] newArgs =  new String[]{args[1], args[2], reducerNum};



        String [] options = getOptions(args);

        int exitCode;
        switch (commandHm.get(commandOption)){

            case ORDER_NUMBER :
                exitCode = ToolRunner.run(new OrderTimeMr(), newArgs);
                break;
            case TRACE_NUMBER :
                String [] opts =  new String[]{args[1], args[2], reducerNum};
                exitCode = ToolRunner.run(new TraceTimeMr(), opts);
                break;

            // 分析过后的 trace 时间数据
            case TRACE_IMPORT_NUMBER :
                exitCode = ToolRunner.run(HBaseConfiguration.create(), new TraceCalculateTimeImporterMr(), newArgs);
                break;

            case SITE_2_NAME :
                exitCode = ToolRunner.run(new SiteId2NameMr(), newArgs);
                break;

            case SITE_2_NAME_IMP :
                exitCode = ToolRunner.run(HBaseConfiguration.create(), new MetaSiteImporterMr(), newArgs);
                break;

            case TRACE_COMPAR :
                exitCode = ToolRunner.run(new TraceTimeComparsionMr(), newArgs);
                break;

            case FILTER :
                String [] argArr =  new String[]{args[1], args[2], args[3], args[4]};
                exitCode = ToolRunner.run(new OutliersFilterMr(), argArr);
                break;

            case IMPTRACE :
                exitCode = ToolRunner.run(HBaseConfiguration.create(), new OriginalTraceImporterMr(), newArgs);
                break;

            case IMPEWB :
                exitCode = ToolRunner.run(HBaseConfiguration.create(), new EwbImporterMr(), newArgs);
                break;

            case EWB_SAMPLE :
                exitCode = ToolRunner.run(new EwbDataSampleCollector(), newArgs);
                break;

            case CREATE_TABLE :
                String ars [] = new String[]{args[1],args[2],args[3],args[4]};
                exitCode = ToolRunner.run(new HbaseTool(), ars);
                break;

            case BULKLOAD :
                String arrs [] = new String[]{args[1],args[2]};
                exitCode = ToolRunner.run(new BulkLoadTool(), arrs);
                break;

            case TRACE_SAMPLE :
                exitCode = ToolRunner.run(new TraceDataSampleCollector(), newArgs);
                break;


            case FILTER_TRACE_OF_TIME :
                exitCode = ToolRunner.run(new FilterCurrentActiveTrace(), options);
                break;

            case DISTINCT_ACTIVE_TRACE :
                exitCode = ToolRunner.run(new DistinctActiveTrace(), options);
                break;

            case MERGE_PREDICT_TIME_2_ACTIVE_TRACE :
                exitCode = ToolRunner.run(new Merge2ActiveTrace(), options);
                break;

            case  AT2HBAE :
                exitCode = ToolRunner.run(new ActiveTrace2Hbase(), options);
                break;

            case VALIDATE :
                exitCode = ToolRunner.run(new Validate(), options);
                break;

            case HDFS_TEST :
                exitCode = ToolRunner.run(new HdfsToolTest(), options);
                break;

            case AT2MYSQL :
                exitCode = ToolRunner.run(new ActiveTrace2Mysql(), options);
                break;

            case V2MYSQL :
                exitCode = ToolRunner.run(new Validate2Mysql(), options);
                break;

            // 连续分析入库
            case ANE :
                exitCode = ToolRunner.run(new TaskProcess(), new String[]{args[1]});
                break;

            case RELATION_INDEX :
                exitCode = ToolRunner.run(new RelationIndexMr(), options);
                break;

            default:
                exitCode = 0;
                break;
        }
        System.exit(exitCode);
    }

    private static String[] getOptions(String[] args) {
        List<String> optionList = new ArrayList<String>();
        if (args.length==1){
            return new String[]{};
        }
        for (int i=1;i<args.length;i++){
            optionList.add(args[i]);
        }
        String[] array = new String[optionList.size()];
        return optionList.toArray(array);
    }
}
