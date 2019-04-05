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
 * This Class is used to assign Email Bill Id to Bill. This exposes in static
 * synchronised method for this, as we want to make sure that no duplicate 
 * bill id are assigned and no sequence is missed
 * 
 ******************************************************************************
 *
 *
 * CHANGE HISTORY:
 *
 * Date:       by:       Reason:
 * 2019-01-16  SANDEEP   Assign Email Bill Id to Bill
 ******************************************************************************
 */
package com.splwg.cm.domain.billing.batch.customBusinessComponent;

import com.splwg.base.domain.common.characteristicType.CharacteristicType;
import com.splwg.ccb.domain.billing.bill.entity.Bill;

/**
  * Class for the cmAddEmailBillIdToBill component
  *
  * @author sandeep
  */
public class CmAddEmailBillIdToBill{
	
	public synchronized static String addEmailBillId(Bill bill, CharacteristicType emailBillIdCharType, String billIdPrefix) throws Exception{
		CmAddEmailBillIdToBillComp bc  = CmAddEmailBillIdToBillComp.Factory.newInstance();
		return bc.addEmailBillId(bill, emailBillIdCharType,billIdPrefix);
		
	}
}
