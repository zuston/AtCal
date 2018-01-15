package io.github.zuston.basic.Util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zuston on 2018/1/15.
 */
public class HdfsTool {

    public static Logger logger = LoggerFactory.getLogger(HdfsTool.class);

    public static final String HDFS_URL = "hdfs://10.10.0.91:8020";

    public static List<String> readFromHdfs(Configuration configuration, String path) throws IOException {
        String dst = HDFS_URL + path;
        FileSystem fs = FileSystem.get(URI.create(dst), configuration);
        FSDataInputStream hdfsInStream = fs.open(new Path(dst));

        String record = null;
        List<String> lineList = new ArrayList<String>();
        while ((record=hdfsInStream.readLine())!=null){
            lineList.add(record);
        }

        hdfsInStream.close();
        fs.close();
        return lineList;
    }
}
