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

import com.splwg.base.domain.common.message.AbstractMessageRepository;
/**
 * CustomMessages contains methods to return Server errors to be raised while processing
 */
public class CustomMessages
 extends AbstractMessageRepository {

	public static final int MESSAGE_CATEGORY = 91000; 
	
	public static final int BILLING_RETAIL_BILL_GENERATED = 20001; 
	public static final int BILLING_SP_BILL_GENERATED = 20002; 
	public static final int ASN_EMAIL_BILLID_BATCH_ABORTED = 10001;
	public static final int ASN_POSTAL_BILLID_BATCH_ABORTED = 10002;
	public static final int EMAIL_BILLID_ASSIGNED_TO_BILL = 10003;
	public static final int POSTAL_BILLID_ASSIGNED_TO_BILL = 10004;
	public static final int ASN_EMAIL_BILLID_SERVICE_ERROR = 10005;
	public static final int ASN_POSTAL_BILLID_SERVICE_ERROR = 10006;
	
	
	
    
    /**
     * Custom Messages
     */
    public CustomMessages() {
        super(MESSAGE_CATEGORY);
    }
}
