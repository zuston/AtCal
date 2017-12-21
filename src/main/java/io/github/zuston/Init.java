package io.github.zuston;

import io.github.zuston.ane.Trace.OrderTimeMr;
import io.github.zuston.ane.Trace.TraceTimeMr;
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

    static {
        commandHm.put("order", ORDER_NUMBER);
        commandHm.put("trace", TRACE_NUMBER);
    }

    public static void main(String[] args) throws Exception {
        if (args.length!=3){
            loggerFactory.error("please check the command line");
            return;
        }

        String commandOption = args[0];

        String [] newArgs = new String[]{args[1], args[2]};

        int exitCode;
        switch (commandHm.get(commandOption)){

            case ORDER_NUMBER :
                exitCode = ToolRunner.run(new OrderTimeMr(), newArgs);
                System.exit(exitCode);
                break;
            case TRACE_NUMBER :
                exitCode = ToolRunner.run(new TraceTimeMr(), newArgs);
                System.exit(exitCode);
                break;
            default:
                System.exit(0);
        }
    }
}
