package io.github.zuston.test;

import io.github.zuston.basic.Util.HdfsTool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.HashMap;
import java.util.List;

/**
 * Created by zuston on 2018/1/16.
 */

public class HdfsToolTest extends Configured implements Tool {

    public static final String mapperPath = "/site2nameMapper-1/part-r-00000";


    public int run(String[] strings) throws Exception {
        HashMap<String,String> name2IdMapper = new HashMap<String, String>();
        List<String> lineList = HdfsTool.readFromHdfs(this.getConf(), mapperPath);
        System.out.println(lineList.size());
        for (String record : lineList){
            String [] splitRecord = record.split("\\s+");
            if (splitRecord.length != 2)    continue;
            String id = splitRecord[0];
            String name = splitRecord[1];
            name2IdMapper.put(name, id);
        }
        System.out.println(name2IdMapper.size());
        System.out.println(name2IdMapper.get("开福北辰三角洲分部"));
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HdfsToolTest(), new String[]{});
    }
}
