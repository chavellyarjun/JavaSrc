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
 * This Business Component is used to assign Postal Bill Id to Bill. 
 * If gets the Max Postal Bill Id in the year and if found and max digit is 
 * reached increment the letter to next and use starting sequence else
 * increment the sequence by 1 for new Id and 
 * if Max not found assign the starting sequence with letter A. 
 * 
 ******************************************************************************
 *
 *
 * CHANGE HISTORY:
 *
 * Date:       by:       Reason:
 * 2019-01-16  SANDEEP   Assign Postal Bill Id to Bill
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
public class CmAddPostalBillIdToBillComp_Impl extends GenericBusinessComponent
		implements CmAddPostalBillIdToBillComp {
	
	
	public String addPostalBillId(Bill bill, CharacteristicType postalBillIdCharType) {
		
		int billYear = bill.getBillDate().getYear();
		String sql = "select MAX(ADHOC_CHAR_VAL) AS ADHOC_CHAR_VAL FROM CI_BILL B, CI_BILL_CHAR BC "
				+ "WHERE B.BILL_ID = BC.BILL_ID  AND BC.CHAR_TYPE_CD = :postalBillIdCharType AND extract(year from B.BILL_DT) =  :billYear";
		PreparedStatement ps = createPreparedStatement(sql,"Get Max Postal Bill Id");
		ps.bindEntity("postalBillIdCharType", postalBillIdCharType);
		ps.bindBigInteger("billYear", BigInteger.valueOf(billYear));
		SQLResultRow row = ps.firstRow();
		char currentLetter;
		String currentNumber;
		String currentPostalBillId;
		String lastPostalBillId = row.getString("ADHOC_CHAR_VAL");
		
		if(isNull(lastPostalBillId)){
			currentPostalBillId = "A000001";
		}else{
			
			char lastLetter = lastPostalBillId.charAt(0);
			String lastNumber = lastPostalBillId.substring(1, lastPostalBillId.length());
			if(lastNumber == "999999"){
				currentLetter =  lastLetter++;
				currentPostalBillId = currentLetter + "000001";
			}else{
				currentNumber = String.format("%06d", (Long.parseLong(lastNumber) + 1));
				currentPostalBillId = String.valueOf(lastLetter) + currentNumber;
			}
				
		}
		
		//Add Postal Id to Bill 
		
		BillCharacteristics billchars = bill.getCharacteristics();
		BillCharacteristic_DTO billCharDto = billchars.newChildDTO();
		billCharDto.setAdhocCharacteristicValue(currentPostalBillId);
		billchars.add(billCharDto, postalBillIdCharType.getId(), BigInteger.ONE);
		return currentPostalBillId;
		
	}

}
