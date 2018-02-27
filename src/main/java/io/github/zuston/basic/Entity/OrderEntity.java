package io.github.zuston.basic.Entity;

/**
 * Created by zuston on 2018/2/27.
 */
public class OrderEntity {
    private String scan_time;
    private String site_name;
    private String des_site_name;
    // 运单状态描述
    private String desp;
    private String site_id;

    public OrderEntity(String scan_time, String site_name, String des_site_name, String desp, String site_id) {
        this.scan_time = scan_time;
        this.site_name = site_name;
        this.des_site_name = des_site_name;
        this.desp = desp;
        this.site_id = site_id;
    }

    public String getScan_time() {
        return scan_time;
    }

    public String getDesp() {
        return desp;
    }

    public String getSite_name() {
        return site_name;

    }

    public String getSite_id(){
        return site_id;
    }

    public String getDes_site_name() {
        return des_site_name;
    }
}
