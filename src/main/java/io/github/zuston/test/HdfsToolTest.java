package io.github.zuston.test;

import io.github.zuston.Util.HdfsTool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            String name = splitRecord[1];
            String id = splitRecord[0];
            name2IdMapper.put(name, id);
        }

        for (Map.Entry<String,String> entry : name2IdMapper.entrySet()){
            System.out.println(entry.getKey() + "-" + entry.getValue());
        }

        System.out.println(name2IdMapper.toString());
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HdfsToolTest(), new String[]{});
    }
}
