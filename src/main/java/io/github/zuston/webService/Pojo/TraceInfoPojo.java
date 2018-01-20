package io.github.zuston.webService.Pojo;

/**
 * Created by zuston on 2018/1/20.
 */
public class TraceInfoPojo {
    private String TRACE_ID        ;
    private String EWB_NO 			;
    private String SITE_ID			;
    private String SITE_NAME		;
    private String SCAN_TIME		;
    private String DEST_SITE_ID	;
    private String DEST_SITE_NAME 	;
    private String PREDICT_TIME;

    public TraceInfoPojo(String TRACE_ID, String EWB_NO, String SITE_ID, String SITE_NAME, String SCAN_TIME, String DEST_SITE_ID, String DEST_SITE_NAME, String PREDICT_TIME) {
        this.TRACE_ID = TRACE_ID;
        this.EWB_NO = EWB_NO;
        this.SITE_ID = SITE_ID;
        this.SITE_NAME = SITE_NAME;
        this.SCAN_TIME = SCAN_TIME;
        this.DEST_SITE_ID = DEST_SITE_ID;
        this.DEST_SITE_NAME = DEST_SITE_NAME;
        this.PREDICT_TIME = PREDICT_TIME;
    }

    public TraceInfoPojo(String TRACE_ID, String EWB_NO, String SITE_ID, String SITE_NAME, String SCAN_TIME, String DEST_SITE_ID, String DEST_SITE_NAME) {
        this.TRACE_ID = TRACE_ID;
        this.EWB_NO = EWB_NO;
        this.SITE_ID = SITE_ID;
        this.SITE_NAME = SITE_NAME;
        this.SCAN_TIME = SCAN_TIME;
        this.DEST_SITE_ID = DEST_SITE_ID;
        this.DEST_SITE_NAME = DEST_SITE_NAME;
    }

    public TraceInfoPojo() {
    }
}
