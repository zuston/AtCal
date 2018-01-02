package io.github.zuston.ane.Ewb;

/**
 * Created by zuston on 2017/12/29.
 */
public class EwbRecordParser {

    /**
     *
     EWB_NO 				   NOT NULL VARCHAR2(30 CHAR)
     OPERATION_ID					    NUMBER(12)
     EWB_DATE					    DATE
     SEND_CUSTOMER_ID				    NUMBER(8)
     SEND_CUSTOMER_ADDRESS_ID			    NUMBER(8)
     RECEIVE_CUSTOMER_ID				    NUMBER(8)
     RECEIVE_CUSTOMER_ADDRESS_ID			    NUMBER(8)
     PIECE						    NUMBER(5)
     WEIGHT 					    NUMBER(8,2)
     CALC_WEIGHT					    NUMBER(8,2)
     LENGTH 					    NUMBER(6,2)
     WIDTH						    NUMBER(6,2)
     HIGH						    NUMBER(6,2)
     VOL						    NUMBER(12,4)
     VOL_WEIGHT					    NUMBER(12,2)
     GOODS_TYPE_ID					    NUMBER(8)
     GOODS_EXPLAIN					    VARCHAR2(60 CHAR)
     PAY_SIDE_ID					    NUMBER(8)
     PAY_MODE_ID					    NUMBER(8)
     CLASS_ID					    NUMBER(8)
     PICK_GOODS_MODE_ID				    NUMBER(8)
     SMS_MODE_ID					    NUMBER(8)
     FREIGHT_CHARGE 				    NUMBER(8,2)
     FIRST_FREIGHT_CHARGE				    NUMBER(8,2)
     FREIGHT_CURRENCY_ID				    NUMBER(8)
     INSURED_AMOUNT 				    NUMBER(8,2)
     INSURED_CURRENCY_ID				    NUMBER(8)
     COD_CHARGE					    NUMBER(8,2)
     COD_CURRENCY_ID				    NUMBER(8)
     COD_PAY_MODE_ID				    NUMBER(8)
     REWB_NO					    VARCHAR2(30 CHAR)
     INPUT_SITE_ID					    NUMBER(8)
     SEND_SITE_ID					    NUMBER(8)
     SALE_EMPLOYEE_ID				    NUMBER(8)
     RECEIVE_EMPLOYEE_ID				    NUMBER(8)
     DISPATCH_SITE_ID				    NUMBER(8)
     DISPATCH_EMPLOYEE_ID				    NUMBER(8)
     EC_ID						    NUMBER(8)
     EC_WAREHOUSE_ID				    NUMBER(8)
     ORDER_NO					    VARCHAR2(256 CHAR)
     BL_SIGN					    NUMBER(1)
     REFUND_FLAG					    NUMBER(1)
     WITHDRAW_FLAG					    NUMBER(1)
     COD_FLAG_3					    NUMBER(1)
     COD_FLAG_4					    NUMBER(1)
     COD_FLAG_5					    NUMBER(1)
     COD_FLAG_6					    NUMBER(1)
     REMARK 					    VARCHAR2(100 CHAR)
     REMARK_CLOB_ID 				    NUMBER(8)
     CREATED_BY					    NUMBER(8)
     CREATED_TIME					    DATE
     MODIFIED_BY					    NUMBER(8)
     MODIFIED_TIME					    DATE
     RD_STATUS					    NUMBER(1)
     CHECK_STATUS					    NUMBER(1)
     CHECK_TOTAL_AMOUNT				    NUMBER(8,2)
     PICKUP_CHARGE					    NUMBER(8,2)
     ZDBXL_CHARGE					    NUMBER(8,2)
     FUEL_ADD_CHARGE				    NUMBER(8,2)
     EC_IN_CHARGE					    NUMBER(8,2)
     PREMIUM_RATE					    NUMBER(8,2)
     KFSLF_CHARGE					    NUMBER(8,2)
     COUPON 					    VARCHAR2(50 CHAR)
     COUPON_RET_CHARGE				    NUMBER(8,2)
     GOODS_NAME					    VARCHAR2(256 CHAR)
     DISP_SITE_ID					    VARCHAR2(30 CHAR)
     DATA_RESOURCE					    NUMBER(2)
     DISPATCH_SMS_ID				    NUMBER(8)
     DISPATCH_CENTER_ID				    NUMBER(8)
     RETURN_TIME_ID 				    NUMBER(8)
     ORDER_WEIGHT					    NUMBER(8,2)
     SMS_RECIPIENTS_ID			   NOT NULL NUMBER(8)
     * @param record
     */

    public void parser(String record){
        String [] splitArr = record.split("'");

    }

}
