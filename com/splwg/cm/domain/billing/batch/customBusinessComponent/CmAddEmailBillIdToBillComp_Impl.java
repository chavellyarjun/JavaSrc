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
 * This Business Component is used to assign Email Bill Id to Bill. 
 * If gets the Max Email Bill Id in the year and if found increment the sequence 
 * by 1 for new Id and if not found assign the starting sequence 
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

import java.math.BigInteger;

import com.splwg.base.api.GenericBusinessComponent;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.characteristicType.CharacteristicType;
import com.splwg.ccb.domain.billing.bill.entity.Bill;
import com.splwg.ccb.domain.billing.bill.entity.BillCharacteristic_DTO;
import com.splwg.ccb.domain.billing.bill.entity.BillCharacteristics;

/**
 * @author sandeep
 *
@BusinessComponent (customizationCallable = true, customizationReplaceable = true)
 */
public class CmAddEmailBillIdToBillComp_Impl extends GenericBusinessComponent
		implements CmAddEmailBillIdToBillComp {
	
	
	public String addEmailBillId(Bill bill, CharacteristicType emailBillIdCharType, String billIdPrefix) {
		
		int billYear = bill.getBillDate().getYear();
		String sql = "select MAX(ADHOC_CHAR_VAL) AS ADHOC_CHAR_VAL FROM CI_BILL B, CI_BILL_CHAR BC "
				+ "WHERE B.BILL_ID = BC.BILL_ID  AND BC.CHAR_TYPE_CD = :emailBillIdCharType AND EXTRACT(YEAR FROM B.BILL_DT) =  :billYear";
		PreparedStatement ps = createPreparedStatement(sql,"Get Max Postal Bill Id");
		ps.bindEntity("emailBillIdCharType", emailBillIdCharType);
		ps.bindBigInteger("billYear", BigInteger.valueOf(billYear));
		SQLResultRow row = ps.firstRow();
		String currentNumber;
		String currentEmailBillId;
		
		String lastEmailBillId = row.getString("ADHOC_CHAR_VAL");
		
		if(isNull(lastEmailBillId)){
			currentEmailBillId = billIdPrefix + billYear + "000000001";
			
		}else{
			String yearNumber = lastEmailBillId.replace(billIdPrefix, "");
			String lastNumber = yearNumber.substring(4, yearNumber.length());
			currentNumber = String.format("%09d", (Long.parseLong(lastNumber) + 1));
			currentEmailBillId = billIdPrefix + billYear + currentNumber;
		}
				
		//Add Email Bill Id to Bill 
		
		BillCharacteristics billchars = bill.getCharacteristics();
		BillCharacteristic_DTO billCharDto = billchars.newChildDTO();
		billCharDto.setAdhocCharacteristicValue(currentEmailBillId);
		billchars.add(billCharDto, emailBillIdCharType.getId(), BigInteger.ONE);
		return currentEmailBillId;
		
	}

}
