package io.github.zuston.util;

import redis.clients.jedis.Jedis;

/**
 * Created by zuston on 2018/1/30.
 */
public class RedisTool {
    public static Jedis jedis = new Jedis("localhost");

    public static int getInt(String id){
        if (jedis.exists(id)){
            return Integer.parseInt(jedis.get(id));
        }else{
            return -1;
        }
    }

    public static boolean set(String id, String value){
        return jedis.set(id,value).equals("OK");
    }

    public static String getString(String id){
        if (jedis.exists(id)){
            return jedis.get(id);
        }else{
            return null;
        }
    }

    public static boolean delete(String key){
        return jedis.del(key) >= 1 ? true : false;
    }
}
