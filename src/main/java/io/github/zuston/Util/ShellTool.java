package io.github.zuston.Util;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import static org.apache.hadoop.io.IOUtils.closeStream;

/**
 * Created by zuston on 2018/1/19.
 */
public class ShellTool {

    public static String exec(String cmd) throws Exception{
        return exec(cmd, null);
    }

    public static String exec(String cmd, File dir) throws Exception {
        StringBuilder result = new StringBuilder();

        Process process = null;
        BufferedReader bufrIn = null;
        BufferedReader bufrError = null;
        String [] cmds = new String[]{
                "/bin/bash",
                "-c",
                cmd
        };
        try {
            process = Runtime.getRuntime().exec(cmds, null, dir);

            process.waitFor();

            bufrIn = new BufferedReader(new InputStreamReader(process.getInputStream(), "UTF-8"));
            bufrError = new BufferedReader(new InputStreamReader(process.getErrorStream(), "UTF-8"));

            String line = null;
            while ((line = bufrIn.readLine()) != null) {
                result.append(line).append('\n');
            }
            while ((line = bufrError.readLine()) != null) {
                result.append(line).append('\n');
            }

        } finally {
            closeStream(bufrIn);
            closeStream(bufrError);
            if (process != null) {
                process.destroy();
            }
        }
        return result.toString();
    }
}
