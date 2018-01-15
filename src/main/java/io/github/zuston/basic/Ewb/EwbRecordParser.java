package io.github.zuston.basic.Ewb;

/**
 * Created by zuston on 2017/12/29.
 */
public class EwbRecordParser {


     private String EWB_NO 				            ;
     private String OPERATION_ID					;
     private String EWB_DATE					    ;
     private String SEND_CUSTOMER_ID				;
     private String SEND_CUSTOMER_ADDRESS_ID		;
     private String RECEIVE_CUSTOMER_ID			    ;
     private String RECEIVE_CUSTOMER_ADDRESS_ID	    ;
     private String PIECE						    ;
     private String WEIGHT 					        ;
     private String CALC_WEIGHT					    ;
     private String LENGTH 					        ;
     private String WIDTH						    ;
     private String HIGH						    ;
     private String VOL						        ;
     private String VOL_WEIGHT					    ;
     private String GOODS_TYPE_ID					;
     private String GOODS_EXPLAIN					;
     private String PAY_SIDE_ID					    ;
     private String PAY_MODE_ID					    ;
     private String CLASS_ID					    ;
     private String PICK_GOODS_MODE_ID				;
     private String SMS_MODE_ID					    ;
     private String FREIGHT_CHARGE 				    ;
     private String FIRST_FREIGHT_CHARGE			;
     private String FREIGHT_CURRENCY_ID			    ;
     private String INSURED_AMOUNT 				    ;
     private String INSURED_CURRENCY_ID			    ;
     private String COD_CHARGE					    ;
     private String COD_CURRENCY_ID				    ;
     private String COD_PAY_MODE_ID				    ;
     private String REWB_NO					        ;
     private String INPUT_SITE_ID					;
     private String SEND_SITE_ID					;
     private String SALE_EMPLOYEE_ID			    ;
     private String RECEIVE_EMPLOYEE_ID		        ;
     private String DISPATCH_SITE_ID			    ;
     private String DISPATCH_EMPLOYEE_ID		    ;
     private String EC_ID						    ;
     private String EC_WAREHOUSE_ID			        ;
     private String ORDER_NO					    ;
     private String BL_SIGN					        ;
     private String REFUND_FLAG				        ;
     private String WITHDRAW_FLAG				    ;
     private String COD_FLAG_3					    ;
     private String COD_FLAG_4					    ;
     private String COD_FLAG_5					    ;
     private String COD_FLAG_6					    ;
     private String REMARK 					        ;
     private String REMARK_CLOB_ID 			        ;
     private String CREATED_BY					    ;
     private String CREATED_TIME				    ;
     private String MODIFIED_BY				        ;
     private String MODIFIED_TIME				    ;
     private String RD_STATUS					    ;
     private String CHECK_STATUS				    ;
     private String CHECK_TOTAL_AMOUNT			    ;
     private String PICKUP_CHARGE				    ;
     private String ZDBXL_CHARGE				    ;
     private String FUEL_ADD_CHARGE			        ;
     private String EC_IN_CHARGE				    ;
     private String PREMIUM_RATE				    ;
     private String KFSLF_CHARGE				    ;
     private String COUPON 					        ;
     private String COUPON_RET_CHARGE			    ;
     private String GOODS_NAME					    ;
     private String DISP_SITE_ID				    ;
     private String DATA_RESOURCE				    ;
     private String DISPATCH_SMS_ID			        ;
     private String DISPATCH_CENTER_ID			    ;
     private String RETURN_TIME_ID 			        ;
     private String ORDER_WEIGHT				    ;
     private String SMS_RECIPIENTS_ID			    ;


    public boolean parser(String record){
        String [] splitArr = record.split("'");
        if (splitArr.length!=72)    return false;
        EWB_NO 				            =   splitArr[0];
        OPERATION_ID					=   splitArr[1];
        EWB_DATE					    =   splitArr[2];
        SEND_CUSTOMER_ID				=   splitArr[3];
        SEND_CUSTOMER_ADDRESS_ID		=   splitArr[4];
        RECEIVE_CUSTOMER_ID			    =   splitArr[5];
        RECEIVE_CUSTOMER_ADDRESS_ID	    =   splitArr[6];
        PIECE						    =   splitArr[7];
        WEIGHT 					        =   splitArr[8];
        CALC_WEIGHT					    =   splitArr[9];
        LENGTH 					        =   splitArr[10];
        WIDTH						    =   splitArr[11];
        HIGH						    =   splitArr[12];
        VOL						        =   splitArr[13];
        VOL_WEIGHT					    =   splitArr[14];
        GOODS_TYPE_ID					=   splitArr[15];
        GOODS_EXPLAIN					=   splitArr[16];
        PAY_SIDE_ID					    =   splitArr[17];
        PAY_MODE_ID					    =   splitArr[18];
        CLASS_ID					    =   splitArr[19];
        PICK_GOODS_MODE_ID				=   splitArr[20];
        SMS_MODE_ID					    =   splitArr[21];
        FREIGHT_CHARGE 				    =   splitArr[22];
        FIRST_FREIGHT_CHARGE			=   splitArr[23];
        FREIGHT_CURRENCY_ID			    =   splitArr[24];
        INSURED_AMOUNT 				    =   splitArr[25];
        INSURED_CURRENCY_ID			    =   splitArr[26];
        COD_CHARGE					    =   splitArr[27];
        COD_CURRENCY_ID				    =   splitArr[28];
        COD_PAY_MODE_ID				    =   splitArr[29];
        REWB_NO					        =   splitArr[30];
        INPUT_SITE_ID					=   splitArr[31];
        SEND_SITE_ID					=   splitArr[32];
        SALE_EMPLOYEE_ID			    =   splitArr[33];
        RECEIVE_EMPLOYEE_ID		        =   splitArr[34];
        DISPATCH_SITE_ID			    =   splitArr[35];
        DISPATCH_EMPLOYEE_ID		    =   splitArr[36];
        EC_ID						    =   splitArr[37];
        EC_WAREHOUSE_ID			        =   splitArr[38];
        ORDER_NO					    =   splitArr[39];
        BL_SIGN					        =   splitArr[40];
        REFUND_FLAG				        =   splitArr[41];
        WITHDRAW_FLAG				    =   splitArr[42];
        COD_FLAG_3					    =   splitArr[43];
        COD_FLAG_4					    =   splitArr[44];
        COD_FLAG_5					    =   splitArr[45];
        COD_FLAG_6					    =   splitArr[46];
        REMARK 					        =   splitArr[47];
        REMARK_CLOB_ID 			        =   splitArr[48];
        CREATED_BY					    =   splitArr[49];
        CREATED_TIME				    =   splitArr[50];
        MODIFIED_BY				        =   splitArr[51];
        MODIFIED_TIME				    =   splitArr[52];
        RD_STATUS					    =   splitArr[53];
        CHECK_STATUS				    =   splitArr[54];
        CHECK_TOTAL_AMOUNT			    =   splitArr[55];
        PICKUP_CHARGE				    =   splitArr[56];
        ZDBXL_CHARGE				    =   splitArr[57];
        FUEL_ADD_CHARGE			        =   splitArr[58];
        EC_IN_CHARGE				    =   splitArr[59];
        PREMIUM_RATE				    =   splitArr[60];
        KFSLF_CHARGE				    =   splitArr[61];
        COUPON 					        =   splitArr[62];
        COUPON_RET_CHARGE			    =   splitArr[63];
        GOODS_NAME					    =   splitArr[64];
        DISP_SITE_ID				    =   splitArr[65];
        DATA_RESOURCE				    =   splitArr[66];
        DISPATCH_SMS_ID			        =   splitArr[67];
        DISPATCH_CENTER_ID			    =   splitArr[68];
        RETURN_TIME_ID 			        =   splitArr[69];
        ORDER_WEIGHT				    =   splitArr[70];
        SMS_RECIPIENTS_ID			    =   splitArr[71];
        return true;
    }

    public String getEWB_NO() {
        return EWB_NO;
    }

    public String getOPERATION_ID() {
        return OPERATION_ID;
    }

    public String getEWB_DATE() {
        return EWB_DATE;
    }

    public String getSEND_CUSTOMER_ID() {
        return SEND_CUSTOMER_ID;
    }

    public String getSEND_CUSTOMER_ADDRESS_ID() {
        return SEND_CUSTOMER_ADDRESS_ID;
    }

    public String getRECEIVE_CUSTOMER_ID() {
        return RECEIVE_CUSTOMER_ID;
    }

    public String getRECEIVE_CUSTOMER_ADDRESS_ID() {
        return RECEIVE_CUSTOMER_ADDRESS_ID;
    }

    public String getPIECE() {
        return PIECE;
    }

    public String getWEIGHT() {
        return WEIGHT;
    }

    public String getCALC_WEIGHT() {
        return CALC_WEIGHT;
    }

    public String getLENGTH() {
        return LENGTH;
    }

    public String getWIDTH() {
        return WIDTH;
    }

    public String getHIGH() {
        return HIGH;
    }

    public String getVOL() {
        return VOL;
    }

    public String getVOL_WEIGHT() {
        return VOL_WEIGHT;
    }

    public String getGOODS_TYPE_ID() {
        return GOODS_TYPE_ID;
    }

    public String getGOODS_EXPLAIN() {
        return GOODS_EXPLAIN;
    }

    public String getPAY_SIDE_ID() {
        return PAY_SIDE_ID;
    }

    public String getPAY_MODE_ID() {
        return PAY_MODE_ID;
    }

    public String getCLASS_ID() {
        return CLASS_ID;
    }

    public String getPICK_GOODS_MODE_ID() {
        return PICK_GOODS_MODE_ID;
    }

    public String getSMS_MODE_ID() {
        return SMS_MODE_ID;
    }

    public String getFREIGHT_CHARGE() {
        return FREIGHT_CHARGE;
    }

    public String getFIRST_FREIGHT_CHARGE() {
        return FIRST_FREIGHT_CHARGE;
    }

    public String getFREIGHT_CURRENCY_ID() {
        return FREIGHT_CURRENCY_ID;
    }

    public String getINSURED_AMOUNT() {
        return INSURED_AMOUNT;
    }

    public String getINSURED_CURRENCY_ID() {
        return INSURED_CURRENCY_ID;
    }

    public String getCOD_CHARGE() {
        return COD_CHARGE;
    }

    public String getCOD_CURRENCY_ID() {
        return COD_CURRENCY_ID;
    }

    public String getCOD_PAY_MODE_ID() {
        return COD_PAY_MODE_ID;
    }

    public String getREWB_NO() {
        return REWB_NO;
    }

    public String getINPUT_SITE_ID() {
        return INPUT_SITE_ID;
    }

    public String getSEND_SITE_ID() {
        return SEND_SITE_ID;
    }

    public String getSALE_EMPLOYEE_ID() {
        return SALE_EMPLOYEE_ID;
    }

    public String getRECEIVE_EMPLOYEE_ID() {
        return RECEIVE_EMPLOYEE_ID;
    }

    public String getDISPATCH_SITE_ID() {
        return DISPATCH_SITE_ID;
    }

    public String getDISPATCH_EMPLOYEE_ID() {
        return DISPATCH_EMPLOYEE_ID;
    }

    public String getEC_ID() {
        return EC_ID;
    }

    public String getEC_WAREHOUSE_ID() {
        return EC_WAREHOUSE_ID;
    }

    public String getORDER_NO() {
        return ORDER_NO;
    }

    public String getBL_SIGN() {
        return BL_SIGN;
    }

    public String getREFUND_FLAG() {
        return REFUND_FLAG;
    }

    public String getWITHDRAW_FLAG() {
        return WITHDRAW_FLAG;
    }

    public String getCOD_FLAG_3() {
        return COD_FLAG_3;
    }

    public String getCOD_FLAG_4() {
        return COD_FLAG_4;
    }

    public String getCOD_FLAG_5() {
        return COD_FLAG_5;
    }

    public String getCOD_FLAG_6() {
        return COD_FLAG_6;
    }

    public String getREMARK() {
        return REMARK;
    }

    public String getREMARK_CLOB_ID() {
        return REMARK_CLOB_ID;
    }

    public String getCREATED_BY() {
        return CREATED_BY;
    }

    public String getCREATED_TIME() {
        return CREATED_TIME;
    }

    public String getMODIFIED_BY() {
        return MODIFIED_BY;
    }

    public String getMODIFIED_TIME() {
        return MODIFIED_TIME;
    }

    public String getRD_STATUS() {
        return RD_STATUS;
    }

    public String getCHECK_STATUS() {
        return CHECK_STATUS;
    }

    public String getCHECK_TOTAL_AMOUNT() {
        return CHECK_TOTAL_AMOUNT;
    }

    public String getPICKUP_CHARGE() {
        return PICKUP_CHARGE;
    }

    public String getZDBXL_CHARGE() {
        return ZDBXL_CHARGE;
    }

    public String getFUEL_ADD_CHARGE() {
        return FUEL_ADD_CHARGE;
    }

    public String getEC_IN_CHARGE() {
        return EC_IN_CHARGE;
    }

    public String getPREMIUM_RATE() {
        return PREMIUM_RATE;
    }

    public String getKFSLF_CHARGE() {
        return KFSLF_CHARGE;
    }

    public String getCOUPON() {
        return COUPON;
    }

    public String getCOUPON_RET_CHARGE() {
        return COUPON_RET_CHARGE;
    }

    public String getGOODS_NAME() {
        return GOODS_NAME;
    }

    public String getDISP_SITE_ID() {
        return DISP_SITE_ID;
    }

    public String getDATA_RESOURCE() {
        return DATA_RESOURCE;
    }

    public String getDISPATCH_SMS_ID() {
        return DISPATCH_SMS_ID;
    }

    public String getDISPATCH_CENTER_ID() {
        return DISPATCH_CENTER_ID;
    }

    public String getRETURN_TIME_ID() {
        return RETURN_TIME_ID;
    }

    public String getORDER_WEIGHT() {
        return ORDER_WEIGHT;
    }

    public String getSMS_RECIPIENTS_ID() {
        return SMS_RECIPIENTS_ID;
    }
}
