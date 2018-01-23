package io.github.zuston.webService.Listener;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationListener;

import java.io.IOException;
import java.util.HashMap;


/**
 * Created by zuston on 2018/1/18.
 */
public class HBaseListener implements ApplicationListener<ApplicationStartedEvent> {

    public static Logger logger = LoggerFactory.getLogger(HBaseListener.class);

    public static Configuration configuration;

    public static HTable ActiveTraceTable;

    public static HTable ValidateTable;

    public static HTable TraceTable;

    public static HTable EwbIndexTable;

    public static HTable SiteIndexTable_IN;
    public static HTable SiteIndexTable_OUT;

    public static HashMap<String, HTable> Container = new HashMap<String, HTable>();

    @Override
    public void onApplicationEvent(ApplicationStartedEvent applicationStartedEvent) {
        logger.info("init the HBase configuration");
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", "master:60000");
        configuration.set("hbase.zookeeper.quorum","slave4,slave2,slave3");
        logger.info("finish the HBase configuration");

        try {
            ActiveTraceTable = new HTable(configuration,"ActiveRecord");
            ValidateTable = new HTable(configuration, "Validate");
            TraceTable = new HTable(configuration, "trace");

            EwbIndexTable = new HTable(configuration, "ewbIndex");

            SiteIndexTable_IN = new HTable(configuration, "siteIndex_In");
            SiteIndexTable_OUT = new HTable(configuration, "siteIndex_Out");

            Container.put("Validate", ValidateTable);
            Container.put("trace",TraceTable);

            Container.put("ActiveRecord", ActiveTraceTable);

            Container.put("ewbIndex",EwbIndexTable);

            Container.put("siteIndex_In",SiteIndexTable_IN);
            Container.put("siteIndex_Out",SiteIndexTable_OUT);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
