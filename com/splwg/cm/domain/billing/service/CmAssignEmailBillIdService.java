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
 * This Service Program is used to assign Email Bill Id to Bill from online. 
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
package com.splwg.cm.domain.billing.service;

import com.splwg.base.api.service.DataElement;
import com.splwg.base.api.service.PageHeader;
import com.splwg.base.domain.common.characteristicType.CharacteristicType;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.ccb.domain.billing.bill.entity.Bill;
import com.splwg.ccb.domain.billing.bill.entity.Bill_Id;
import com.splwg.cm.domain.billing.batch.customBusinessComponent.CmAddEmailBillIdToBill;
import com.splwg.cm.domain.billing.customMessages.CustomMessageRepository;
import com.splwg.shared.common.ApplicationError;

/**
 * 
 *  @author sandeep
 * 
 *  @PageMaintenance (secured = false, service = CM-ASN-EMAIL-BILLID,
 *      
 *      header = { @DataField (name = BILL_ID)
 *            , @DataField (name = CHAR_TYPE_CD)
 *            , @DataField (name = CM_CUSTOM_BILLID_PREFIX)
 *            , @DataField (name = ADHOC_CHAR_VAL)},
 *      
 *      body = @DataElement (contents = {  @DataField (name = BILL_ID)
 *            , @DataField (name = CHAR_TYPE_CD)
 *            , @DataField (name = CM_CUSTOM_BILLID_PREFIX)
 *            , @DataField (name = ADHOC_CHAR_VAL)}),
 *      headerFields = { @DataField (name = BILL_ID)
 *            , @DataField (name = CHAR_TYPE_CD)
 *            , @DataField (name = CM_CUSTOM_BILLID_PREFIX)
 *            , @DataField (name = ADHOC_CHAR_VAL)},      
 *      actions = {"read","add"},
 *      modules = { "billing"})
 */
public class CmAssignEmailBillIdService extends  CmAssignEmailBillIdService_Gen{
	
	@Override
	protected PageHeader add(DataElement dataElement) throws ApplicationError {
		
		PageHeader page = new PageHeader();
		String billIdStr = dataElement.get(STRUCTURE.BILL_ID);
		String emailBillIdCharTypeStr = dataElement.get(STRUCTURE.CHAR_TYPE_CD);
		String billIdPrefix = dataElement
				.get(STRUCTURE.CM_CUSTOM_BILLID_PREFIX);
		Bill bill = new Bill_Id(billIdStr).getEntity();
		CharacteristicType emailBillIdCharType = new CharacteristicType_Id(
				emailBillIdCharTypeStr).getEntity();

		try {
			String customBillId = CmAddEmailBillIdToBill.addEmailBillId(bill,
					emailBillIdCharType, billIdPrefix);
			logInfo(CustomMessageRepository.emailBillIdAssignedtoBill(
					customBillId, bill.getId().getTrimmedValue()));
			dataElement.put(STRUCTURE.ADHOC_CHAR_VAL, customBillId);
		} catch (Exception e) {
			addError(CustomMessageRepository.assignEmailBillIdServiceError(
					billIdStr, e.getMessage()));
		}
		copyBodyToHeader(dataElement, page);
		return page;

	}

	/**
     * Copies pageBody to pageHeader for Read
     * @param pageBody DataElement
     * @param pageHeader PageHeader
     */
    private void copyBodyToHeader(DataElement pageBody, PageHeader pageHeader) {
        pageHeader.putAll(pageBody.getAttributes());
    }
    
    /**
     * Returns the PageBody
     * @param header PageHeader
     * @return DataElement pageBody
     * @throws ApplicationError ae
     */
    @Override
    protected DataElement read(PageHeader header) throws ApplicationError {
        DataElement dataElement = new DataElement();
        dataElement.putAll(header.getAttributes());
        return  dataElement;
    }
}
