package io.github.zuston.test;

import java.sql.Timestamp;

/**
 * Created by zuston on 2018/1/17.
 */
public class TimeTest {
    public static void main(String[] args) {
        long time = Timestamp.valueOf("2018-01-10 12:00:00").getTime();
        long time1 = Timestamp.valueOf("2018-01-10 13:00:00").getTime();
        System.out.println((time1-time) / 60 / 1000);
    }
}
