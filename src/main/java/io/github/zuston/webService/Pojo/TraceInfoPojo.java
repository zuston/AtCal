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

    public void setTRACE_ID(String TRACE_ID) {
        this.TRACE_ID = TRACE_ID;
    }

    public void setEWB_NO(String EWB_NO) {
        this.EWB_NO = EWB_NO;
    }

    public void setSITE_ID(String SITE_ID) {
        this.SITE_ID = SITE_ID;
    }

    public void setSITE_NAME(String SITE_NAME) {
        this.SITE_NAME = SITE_NAME;
    }

    public void setSCAN_TIME(String SCAN_TIME) {
        this.SCAN_TIME = SCAN_TIME;
    }

    public void setDEST_SITE_ID(String DEST_SITE_ID) {
        this.DEST_SITE_ID = DEST_SITE_ID;
    }

    public void setDEST_SITE_NAME(String DEST_SITE_NAME) {
        this.DEST_SITE_NAME = DEST_SITE_NAME;
    }

    public void setPREDICT_TIME(String PREDICT_TIME) {
        this.PREDICT_TIME = PREDICT_TIME;
    }

    public String getTRACE_ID() {
        return TRACE_ID;
    }

    public String getEWB_NO() {
        return EWB_NO;
    }

    public String getSITE_ID() {
        return SITE_ID;
    }

    public String getSITE_NAME() {
        return SITE_NAME;
    }

    public String getSCAN_TIME() {
        return SCAN_TIME;
    }

    public String getDEST_SITE_ID() {
        return DEST_SITE_ID;
    }

    public String getDEST_SITE_NAME() {
        return DEST_SITE_NAME;
    }

    public String getPREDICT_TIME() {
        return PREDICT_TIME;
    }
}
