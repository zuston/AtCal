package io.github.zuston.example;

/**
 * Created by zuston on 2018/1/16.
 */
public class ReflectStatic {
    public static int i = 10;
    public int a = 1000;

    static class A {
        public void map(){
            System.out.println(i);
        }
    }

    public void run(){
        System.out.println("start the run");
    }

    public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchFieldException {
        ReflectStatic a = new ReflectStatic();
        a.i = 1000;
        A instance = A.class.newInstance();

        instance.map();

    }
}
