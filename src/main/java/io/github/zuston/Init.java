package io.github.zuston;

import io.github.zuston.ane.Trace.*;
import io.github.zuston.example.WordFrenquency;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

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

    public static final int WORDFRENQUENCY_NUMBER = 100;

    static {
        commandHm.put("order", ORDER_NUMBER);
        commandHm.put("trace", TRACE_NUMBER);
        commandHm.put("traceImp".toLowerCase(), TRACE_IMPORT_NUMBER);
        commandHm.put("site2name".toLowerCase(), SITE_2_NAME);
        commandHm.put("site2nameImp".toLowerCase(), SITE_2_NAME_IMP );
        commandHm.put("traceCompar".toLowerCase(), TRACE_COMPAR);
        commandHm.put("wf", WORDFRENQUENCY_NUMBER);
    }

    public static void main(String[] args) throws Exception {
//        if (args.length>4){
//            loggerFactory.error("please check the command line");
//            return;
//        }

        // TODO: 2017/12/30 引入命令行操作
        String commandOption = args[0].toLowerCase();
//        String [] opts = new String[args.length-1];
//        for (int i=1;i<args.length;i++){
//            opts[i-1] = args[i];
//        }
        String reducerNum = args.length<=3 ? "1" : args[3];
        String [] newArgs =  new String[]{args[1], args[2], reducerNum};

        int exitCode;
        switch (commandHm.get(commandOption)){

            case ORDER_NUMBER :
                exitCode = ToolRunner.run(new OrderTimeMr(), newArgs);
                break;
            case TRACE_NUMBER :
                String [] opts =  new String[]{args[1], args[2], reducerNum, args[4]};
                exitCode = ToolRunner.run(new TraceTimeMr(), opts);
                break;

            case TRACE_IMPORT_NUMBER :
                exitCode = ToolRunner.run(HBaseConfiguration.create(), new TraceImporterMr(), newArgs);
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

            // chenjie 跑实验的
            case WORDFRENQUENCY_NUMBER :
                exitCode = ToolRunner.run(new WordFrenquency(), newArgs);
                break;

            default:
                exitCode = 0;
                break;
        }
        System.exit(exitCode);
    }
}
