package io.github.zuston.basic.Util;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/**
 * Created by zuston on 2018/1/5.
 */
// 大数据采样，避免导致数据倾斜，单点过热
public class DataSampleCollector extends Configured implements Tool{
    public int run(String[] strings) throws Exception {
        return 0;
    }
}
