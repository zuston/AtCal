package io.github.zuston.ane.Trace;

/**
 * Created by zuston on 2018/1/4.
 */
public class OriginalTraceRecordParser {

     private String TRACE_ID        ;
     private String EWB_NO 			;
     private String STEP			;
     private String SITE_ID			;
     private String SITE_CODE		;
     private String SITE_NAME		;
     private String SITE_TYPE		;
     private String CITY_NAME		;
     private String CLERK_ID		;
     private String CLERK_NAME		;
     private String SCAN_TIME		;
     private String DEST_SITE_ID	;
     private String DEST_SITE_NAME 	;
     private String DEST_SITE_CODE 	;
     private String CONTACTER		;
     private String CONTACT_PHONE	;
     private String DESCPT 			;
     private String STATUS 			;
     private String CREATE_TIME		;
     private String ISPUSH 			;
     private String DISTRICT		;
     private String WEIGHT 			;
     private String EC_ID			;
     private String ELECPUSH		;
     private String NEXTCITY		;
     private String DATA_SOURCE		;
     private String TASK_NO			;

    public boolean parser(String record){
        String [] splitArr = record.split("#");
        if (splitArr.length!=27 && splitArr.length!=26)    return false;
        TRACE_ID        =  splitArr[0];
        EWB_NO 			=  splitArr[1];
        STEP			=  splitArr[2];
        SITE_ID			=  splitArr[3];
        SITE_CODE		=  splitArr[4];
        SITE_NAME		=  splitArr[5];
        SITE_TYPE		=  splitArr[6];
        CITY_NAME		=  splitArr[7];
        CLERK_ID		=  splitArr[8];
        CLERK_NAME		=  splitArr[9];
        SCAN_TIME		=  splitArr[10];
        DEST_SITE_ID	=  splitArr[11];
        DEST_SITE_NAME 	=  splitArr[12];
        DEST_SITE_CODE 	=  splitArr[13];
        CONTACTER		=  splitArr[14];
        CONTACT_PHONE	=  splitArr[15];
        DESCPT 			=  splitArr[16];
        STATUS 			=  splitArr[17];
        CREATE_TIME		=  splitArr[18];
        ISPUSH 			=  splitArr[19];
        DISTRICT		=  splitArr[20];
        WEIGHT 			=  splitArr[21];
        EC_ID			=  splitArr[22];
        ELECPUSH		=  splitArr[23];
        NEXTCITY		=  splitArr[24];
        DATA_SOURCE		=  splitArr[25];
        if (splitArr.length == 26) {
            TASK_NO = "";
            return true;
        }
        TASK_NO			=  splitArr[26];
        return true;
    }

    public String getTRACE_ID() {
        return TRACE_ID;
    }

    public String getEWB_NO() {
        return EWB_NO;
    }

    public String getSTEP() {
        return STEP;
    }

    public String getSITE_ID() {
        return SITE_ID;
    }

    public String getSITE_CODE() {
        return SITE_CODE;
    }

    public String getSITE_NAME() {
        return SITE_NAME;
    }

    public String getSITE_TYPE() {
        return SITE_TYPE;
    }

    public String getCITY_NAME() {
        return CITY_NAME;
    }

    public String getCLERK_ID() {
        return CLERK_ID;
    }

    public String getCLERK_NAME() {
        return CLERK_NAME;
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

    public String getDEST_SITE_CODE() {
        return DEST_SITE_CODE;
    }

    public String getCONTACTER() {
        return CONTACTER;
    }

    public String getCONTACT_PHONE() {
        return CONTACT_PHONE;
    }

    public String getDESCPT() {
        return DESCPT;
    }

    public String getSTATUS() {
        return STATUS;
    }

    public String getCREATE_TIME() {
        return CREATE_TIME;
    }

    public String getISPUSH() {
        return ISPUSH;
    }

    public String getDISTRICT() {
        return DISTRICT;
    }

    public String getWEIGHT() {
        return WEIGHT;
    }

    public String getEC_ID() {
        return EC_ID;
    }

    public String getELECPUSH() {
        return ELECPUSH;
    }

    public String getNEXTCITY() {
        return NEXTCITY;
    }

    public String getDATA_SOURCE() {
        return DATA_SOURCE;
    }

    public String getTASK_NO() {
        return TASK_NO;
    }
}
