package io.github.zuston.task.ValidateTraceTime;

import io.github.zuston.Util.JobGenerator;
import io.github.zuston.Util.ShellTool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

/**
 * Created by zuston on 2018/1/19.
 */
public class Validate2Mysql extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = JobGenerator.SimpleJobGenerator(this, this.getConf(), strings);
        job.setJobName("Validate2Mysql");
        job.setJarByClass(Validate2Mysql.class);
        job.setMapperClass(Validate.MysqlHandlerMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        if (job.waitForCompletion(true)){
            System.out.println("mysqlHandler JOB SUCCES");
            String deleteDataLine = "mysql -uroot -pshacha -e \"delete from ane.validate\" ";
            String import2MysqlLine = "sqoop-export   --connect \"jdbc:mysql://10.10.0.91:3306/ane?useUnicode=true&characterEncoding=utf-8\"  --username root  --password shacha --table validate  --input-fields-terminated-by \"#\" --export-dir  "+strings[1];
            try {
                System.out.println(ShellTool.exec(deleteDataLine));
                System.out.println(ShellTool.exec(import2MysqlLine));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else {
            throw  new Exception("mysqlHandler job is error");
        }
        return 0;
    }
}
