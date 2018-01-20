package io.github.zuston.task.ActiveTrace;

import io.github.zuston.Util.JobGenerator;
import io.github.zuston.Util.ShellTool;
import io.github.zuston.Util.StringTool;
import io.github.zuston.basic.Trace.OriginalTraceRecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by zuston on 2018/1/19.
 */
// 采用 sqoop 导入 mysql，但是数据需要预处理下
public class ActiveTrace2Mysql extends Configured implements Tool {

    static class HandleMapper extends Mapper<LongWritable, Text, Text, Text>{

        private static OriginalTraceRecordParser parser = new OriginalTraceRecordParser();

        @Override
        public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
            String [] arr = text.toString().split("\\t");
            if (arr[0].split("#").length<=1)    {
                context.getCounter("ActiveTrace2Mysql","header error").increment(1);
                return;
            }
            String endId = arr[0].split("#")[1];
            String record = arr[1];
            String originalRecord = record.substring(0, record.lastIndexOf("#"));
            if (!parser.parser(originalRecord)){
                context.getCounter("ActiveTraceMysql","recordError").increment(1);
                return;
            }
            if (parser.getDEST_SITE_ID().equals("")){
                record = StringTool.insertOfPrefix(record,"#",endId, 11);
            }
            if (record.contains("_")) record = record.replace("_","#");
            context.write(new Text(record), null);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = JobGenerator.SimpleJobGenerator(this, this.getConf(), strings);
        job.setJobName("activeTrace2mysqlDataHandler");
        job.setJarByClass(ActiveTrace2Mysql.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(HandleMapper.class);

        if (job.waitForCompletion(true)){
            String deleteDataLine = "mysql -uroot -pshacha -e \"delete from ane.activeTrace\" ";
            String import2MysqlLine = "sqoop-export   --connect \"jdbc:mysql://10.10.0.91:3306/ane?useUnicode=true&characterEncoding=utf-8\"  --username root  --password shacha --table activeTrace  --input-fields-terminated-by \"#\" --export-dir  "+strings[1];
            System.out.println(ShellTool.exec(deleteDataLine));
            System.out.println(ShellTool.exec(import2MysqlLine));
        }else{
            throw new Exception("生成 mysql 文件失败");
        }
        return 1;
    }
}
