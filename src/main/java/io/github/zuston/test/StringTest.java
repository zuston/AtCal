package io.github.zuston.test;

/**
 * Created by zuston on 2018/1/17.
 */
public class StringTest {
    public static void main(String[] args) {
        String line = "【沧州市】沧州分拨中心已到达";
        System.out.println(line.substring(line.length()-3,line.length()).equals("已到达"));
    }
}
