package io.github.zuston.webService;

import io.github.zuston.webService.Listener.HBaseListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


/**
 * Created by zuston on 2018/1/18.
*/

@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(Application.class);
        application.addListeners(new HBaseListener());
//        application.addListeners(new MysqlListener());
        application.run(args);
    }
}
