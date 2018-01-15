package io.github.zuston.basic.Util;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by zuston on 2018/1/9.
 */
public class BulkLoadTool extends Configured implements Tool {

    public int run(String[] strings) throws IOException {
        this.getConf().set("hbase.master", "master:60000");
        this.getConf().set("hbase.zookeeper.quorum","slave4,slave2,slave3");
        HTable hTable = null;
        // tableName
        try {
            hTable = new HTable(this.getConf(), strings[0]);
            FsShell shell = new FsShell(this.getConf());
            shell.run(new String[]{"-chmod", "-R", "777", strings[1]});
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(this.getConf());
            loader.doBulkLoad(new Path(strings[1]), hTable);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }finally {
            if (hTable != null){
                hTable.close();
            }
        }

        return 0;
    }
}
