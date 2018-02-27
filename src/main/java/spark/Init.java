package spark;

import spark.core.predict.OrderRdd;

/**
 * Created by zuston on 2018/2/27.
 */
public class Init {
    public static void main(String[] args) {
        OrderRdd orderRdd = new OrderRdd();
        orderRdd.orderRdd_action();
    }
}
