package io.github.zuston.ane.Trace;

/**
 * Created by zuston on 2017/12/18.
 */
public class TraceRecordParser {
    private String trace_id;
    private String ewb_no;
    private String site_name;
    private String scan_time;
    private String des_site_name;

    private String desp;

    public void parse(String record){
        String [] valueArr = record.split("#");
        trace_id = valueArr[0];
        ewb_no = valueArr[1];
        site_name = valueArr[5];
        scan_time = valueArr[10];
        des_site_name = valueArr[12];
        desp = valueArr[16];
    }


    public String getTrace_id() {
        return trace_id;
    }

    public String getEwb_no() {
        return ewb_no;
    }

    public String getSite_name() {
        return site_name;
    }

    public String getScan_time() {
        return scan_time;
    }

    public String getDes_site_name() {
        return des_site_name;
    }

    public String getDesp() {
        return desp;
    }
}
