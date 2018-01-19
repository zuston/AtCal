package io.github.zuston.webService.Pojo;

/**
 * Created by zuston on 2018/1/18.
 */
public class Site2SitePojo {
    public String startId;
    public String endId;
    public int total;
    public int abnormal;

    public Site2SitePojo(String startName, String endName, int total, int abnormal) {
        this.startId = startName;
        this.endId = endName;
        this.total = total;
        this.abnormal = abnormal;
    }
}
