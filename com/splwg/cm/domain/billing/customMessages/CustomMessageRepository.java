/*
 ******************************************************************************
 *                 Confidentiality Information:
 *
 * This module is the confidential and proprietary information of
 * Abjayon Consulting; it is not to be copied, reproduced, or
 * transmitted in any form, by any means, in whole or in part,
 * nor is it to be used for any purpose other than that for which
 * it is expressly provided without the written permission of
 * Abjayon Consulting.
 *
 ******************************************************************************
 *
 * PROGRAM DESCRIPTION:
 *
 * Custom Messages Created for Billing
 * 
 ******************************************************************************
 *
 *
 * CHANGE HISTORY:
 *
 * Date:       by:       Reason:
 * 2019-01-16  SANDEEP   Messages Billing Customizations
 ******************************************************************************
 */
package com.splwg.cm.domain.billing.customMessages;

import com.splwg.base.domain.common.message.MessageParameters;
import com.splwg.shared.common.ServerMessage;

/**
 * CustomMessageRepository contains methods to return Server errors to be raised while processing
 * @author sandeep
 */
public class CustomMessageRepository
 extends CustomMessages {

    // CustomMessageRepositoryGDC object declaration
    private static CustomMessageRepository instance;

    // Constants
    private static final int MAX_INDEX = 30;

    /**
     * Creation of single CustomMessageRepositoryGDC object.
     * 
     * @return CustomMessageRepositoryIDC
     */
    private static CustomMessageRepository getInstance() {

        if (instance == null) {
            instance = new CustomMessageRepository();
        }
        return instance;
    }

    /**
     * Splits the message parms. This can split message parameter consisting of 300 characters.
     * @param message
     * @return
     */
    public static String[] splitMessageParmString(String message) {
        int counter = 0;
        String[] msg = new String[] { "", "", "", "", "", "", "", "", "", "" };

        for (int i = 0; i <= message.length() && counter < 10; i = i + 29) {

            if (i + 29 > message.length()) {
                msg[counter] = (message.substring(i, message.length()));
            } else {
                msg[counter] = (message.substring(i, i + 29));
                counter++;
            }
        }

        return msg;
    }
    
    /*
     * Start - Common Messages (1 - 1000)
     */
    /**
     * Reusable method for splitting a long string into several different
     * message parameters.
     * 
     * @param parms
     *            -- Message Parameters
     * @param longString
     *            -- string to be split
     * @param counter
     *            -- no of parameter spaces(%x) alloted to the long string
     */
    private static void splitAndAddLongString(MessageParameters parms, String longString, int counter) {
        String[] stringArray = new String[counter];
        Integer splits = (longString.length() / MAX_INDEX) + 1;
        for (int i = 0; i < splits && i < counter; i++) {
            if (i == splits - 1) {
                stringArray[i] = longString.substring(i * MAX_INDEX, longString.length());
            } else {
                stringArray[i] = longString.substring(i * MAX_INDEX, i * MAX_INDEX + MAX_INDEX);
            }
        }
        for (int i = 0; i < counter; i++) {
            String parmString = "";
            if (stringArray[i] != null) {
                parmString = stringArray[i];
            }
            parms.addRawString(parmString);
        }
    }

    // Feature Configuration of Feature Type %1%2 is missing.
    public static ServerMessage billingRetailerBillGenerated(String retailerAccountId, String billId, String date) {

        MessageParameters parms = new MessageParameters();
        parms.addRawString(retailerAccountId);
        parms.addRawString(billId);
        parms.addRawString(date);
        return getInstance().getMessage(BILLING_RETAIL_BILL_GENERATED, parms);
    }
    
    // Email Bill Id Assignment Batch Aborted.
    public static ServerMessage assignEmailBillIdBatchAborted(String batchControl, String batchNumber,String billId, String exception) {

        MessageParameters parms = new MessageParameters();
        parms.addRawString(batchControl);
        parms.addRawString(batchNumber);
        parms.addRawString(billId);
        splitAndAddLongString(parms, exception, 6);
        return getInstance().getMessage(ASN_EMAIL_BILLID_BATCH_ABORTED, parms);
    }
    
 // Postal Bill Id Assignment Batch Aborted.
    public static ServerMessage assignPostalBillIdBatchAborted(String batchControl, String batchNumber, String billId, String exception) {

        MessageParameters parms = new MessageParameters();
        parms.addRawString(batchControl);
        
        parms.addRawString(batchNumber);
        parms.addRawString(billId);
        splitAndAddLongString(parms, exception, 6);
        return getInstance().getMessage(ASN_POSTAL_BILLID_BATCH_ABORTED, parms);
    }
    
    
    // email Bill Id Assigned to Bill
    public static ServerMessage emailBillIdAssignedtoBill(String emailBillId, String billId) {

        MessageParameters parms = new MessageParameters();
        parms.addRawString(emailBillId);
        parms.addRawString(billId);
        return getInstance().getMessage(ASN_EMAIL_BILLID_BATCH_ABORTED, parms);
    }
    
    // Postal Bill Id Assigned to Bill
    public static ServerMessage postalBillIdAssignedtoBill(String postalBillId, String billId) {

        MessageParameters parms = new MessageParameters();
        parms.addRawString(postalBillId);
        parms.addRawString(billId);
        return getInstance().getMessage(ASN_POSTAL_BILLID_BATCH_ABORTED, parms);
    }
    
 // Email Bill Id Assignment Service Error.
    public static ServerMessage assignEmailBillIdServiceError(String billId, String exception) {

        MessageParameters parms = new MessageParameters();
        parms.addRawString(billId);
        splitAndAddLongString(parms, exception, 8);
        return getInstance().getMessage(ASN_EMAIL_BILLID_SERVICE_ERROR, parms);
    }
    
 //Postal Bill Id Assignment Service Error
    public static ServerMessage assignPostalBillIdServiceError(String billId, String exception) {

        MessageParameters parms = new MessageParameters();
        parms.addRawString(billId);
        splitAndAddLongString(parms, exception, 8);
        return getInstance().getMessage(ASN_POSTAL_BILLID_SERVICE_ERROR, parms);
    }
    
 // Feature Configuration of Feature Type %1%2 is missing.
    public static ServerMessage billingServiceProviderBillGenerated(String retailerAccountId, String billId, String date) {

        MessageParameters parms = new MessageParameters();
        parms.addRawString(retailerAccountId);
        parms.addRawString(billId);
        parms.addRawString(date);
        return getInstance().getMessage(BILLING_SP_BILL_GENERATED, parms);
    }

    
}
