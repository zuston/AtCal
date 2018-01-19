package io.github.zuston.Util;

/**
 * Created by zuston on 2018/1/18.
 */
public class StringTool {
    public static String reverseByTag(String line, char tag){
        return null;
    }

    public static String insertOfPrefix(String record, String prefix, String insertValue, int prefixIndex){
        String [] arr = record.split(prefix);
        int count = 0;
        for (int i=0; i<prefixIndex; i++){
            count += arr[i].length();
        }
        count += prefix.length() * prefixIndex;
        return new StringBuilder(record).insert(count, insertValue).toString();
    }
}
