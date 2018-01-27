package io.github.zuston.test;

import io.github.zuston.util.ShellTool;

/**
 * Created by zuston on 2018/1/19.
 */
public class ShellToolTest {
    public static void main(String[] args) throws Exception {
//        System.out.println(ShellTool.exec("mysql -h 10.10.0.91 -P 3306 -uroot -pshacha -e \"delete from ane.activeTrace\""));

//        String[] cmds = {"/bin/sh", "-c", "mysql -h 10.10.0.91 -P 3306 -uroot -pshacha -e \"delete from ane.activeTrace\""};
//        Process pro = Runtime.getRuntime().exec(cmds);
//        pro.waitFor();
//        InputStream in = pro.getInputStream();
//        BufferedReader read = new BufferedReader(new InputStreamReader(in));
//        String line = null;
//        while((line = read.readLine())!=null){
//            System.out.println(line);
//        }
        System.out.println(ShellTool.exec("cat /Users/zuston/Desktop/a.txt | tail -n 1"));
    }
}
