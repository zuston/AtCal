package io.github.zuston.util;

import java.util.List;

/**
 * Created by zuston on 2018/1/22.
 */
public class ListTool {
    public static String[] list2arr(List<String> list){
        String [] arr = new String[list.size()];
        int count = 0;
        for (String v : list){
            arr[count] = v;
            count ++;
        }
        return arr;
    }
}
