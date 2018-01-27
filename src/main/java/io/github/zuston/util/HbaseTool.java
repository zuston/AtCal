package io.github.zuston.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Created by zuston on 2018/1/6.
 */
// 依据采样文件进行 hbase region 分区
// 读取采样文件，按分区数进行划分，直接建表，在导入
public class HbaseTool extends Configured implements Tool{

    public static final Logger logger = LoggerFactory.getLogger(HbaseTool.class);
    public static final String HDFS_URL = "hdfs://10.10.0.91:8020";
    public static int splitNumber = 10;
    public static long sampleKeysNumber;

    // return split point
    public byte[][] readFromHdfs(Configuration conf,String path) throws IOException {
        byte[][] splitKeys = new byte[splitNumber][];
        String dst = HDFS_URL + path;
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        FSDataInputStream hdfsInStream = fs.open(new Path(dst));

        String record = null;
        int index = 0;
        int pointer = 0;
        int splitKeyBase = (int) (sampleKeysNumber / splitNumber);
        while ((record=hdfsInStream.readLine())!=null){
            // TODO: 2018/1/6 均分待优化
            if ((pointer!=0) && (pointer % splitKeyBase == 0)) {
                if (index < splitNumber) {
                    splitKeys[index] = Bytes.toBytes(record);
                    index ++;
                }
            }
            pointer ++;
        }

        hdfsInStream.close();
        fs.close();
        return splitKeys;
    }



    public void createHbaseTable(String tableName, byte [][] splitKeys) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.master", "master:60000");
        config.set("hbase.zookeeper.quorum","slave4,slave2,slave3");
        HBaseAdmin admin = new HBaseAdmin(config);
        TableName tname = TableName.valueOf(tableName);

        if (admin.tableExists(tname)){
            try {
                admin.disableTable(tname);
            }catch (Exception e){

            }
            admin.deleteTable(tname);
        }
        HTableDescriptor tableDesc = new HTableDescriptor(tname);
        // 统一只建一个族，info, 可选
        HColumnDescriptor columnsDesc = new HColumnDescriptor(Bytes.toBytes("info"));
        columnsDesc.setMaxVersions(1);
        tableDesc.addFamily(columnsDesc);
        if (splitKeys==null){
            admin.createTable(tableDesc);
        }else {
            admin.createTable(tableDesc, splitKeys);
        }
        admin.close();
    }

    public void createHbaseTable(String tableName) throws IOException {
        this.createHbaseTable(tableName, null);
    }

    /**
     *
     * @param strings
     * @return
     * @throws Exception
     * 参数1：分区文件
     * 参数2：表名
     * 参数3：分区数目
     * 参数4：分区表的行数
     */
    public int run(String[] strings) throws Exception {
        if (strings.length != 4){
            logger.error("HbaseTool command is error");
            System.exit(1);
        }
        // set the splitNumber
        splitNumber = Integer.parseInt(strings[2]);
        sampleKeysNumber = Long.parseLong(strings[3]);
        byte [][] splitKeys = readFromHdfs(this.getConf(),strings[0]);
        createHbaseTable(strings[1], splitKeys);

        return 0;
    }
}
