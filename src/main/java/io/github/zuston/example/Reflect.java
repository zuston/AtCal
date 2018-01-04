package io.github.zuston.example;

import java.lang.reflect.Field;

/**
 * Created by zuston on 2018/1/4.
 */
public class Reflect {
    public static Bean bean = new Bean();
    static Field [] fields = bean.getClass().getDeclaredFields();

    public static void main(String[] args) throws IllegalAccessException {
        String oneLine = "hello";
        String line = "world";
        bean.parser(oneLine);
        System.out.println(bean.getName());
        fields[0].setAccessible(true);
        System.out.println(fields[0].getName()+":"+fields[0].get(bean));
        bean.parser(line);
        System.out.println(fields[0].getName()+":"+fields[0].get(bean));
    }
}

class Bean{
    private String name;
    public void parser(String record){
        this.name = record;
    }
    public String getName(){
        return name;
    }
}
