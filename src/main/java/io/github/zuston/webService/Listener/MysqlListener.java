package io.github.zuston.webService.Listener;

import com.mysql.jdbc.Connection;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationListener;

import java.sql.DriverManager;
import java.sql.SQLException;



/**
 * Created by zuston on 2018/1/20.
 */
public class MysqlListener implements ApplicationListener<ApplicationStartedEvent> {
    public static ThreadLocal<com.mysql.jdbc.Connection> connContainer = new ThreadLocal<com.mysql.jdbc.Connection>();

    public static final String DB_NAME = "ane";

    public static final String DB_USER = "root";

    public static final String DB_PWD = "shacha";

    @Override
    public void onApplicationEvent(ApplicationStartedEvent applicationStartedEvent) {
        Connection conn = connContainer.get();
        if (conn==null){
            try {
                System.out.println("init the connection");
                String dbUrl = String.format("jdbc:mysql://10.10.0.91:3306/%s?user=%s&password=%s&characterEncoding=utf8",DB_NAME,DB_USER,DB_PWD);
                Class.forName("com.mysql.jdbc.Driver") ;
                conn = (com.mysql.jdbc.Connection) DriverManager.getConnection(dbUrl);
                connContainer.set(conn);
            } catch (SQLException e) {
                System.out.println("connection error");
                e.printStackTrace();
            } catch (ClassNotFoundException e){
                System.out.println("can not find the jdbc driver");
                e.printStackTrace();
            }
        }
    }
}
