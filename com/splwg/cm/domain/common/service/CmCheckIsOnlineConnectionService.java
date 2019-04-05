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
 * This Service Program is used to Check If call is through online connection.
 * This is designed to be used in Config Tool Scripts, in future if framework
 * provide any global variable for online scripting this will not be required
 ******************************************************************************
 *
 *
 * CHANGE HISTORY:
 *
 * Date:       by:       Reason:
 * 2019-02-17  SANDEEP   check If Online Connection
 ******************************************************************************
 */
package com.splwg.cm.domain.common.service;

import com.splwg.base.api.datatypes.Bool;
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
 *  @PageMaintenance (secured = false, service = CM-CHK-ONLINE-CONN,
 *      body = @DataElement (contents = { @DataField (name = CM_ONLINE_CONNECTION_SW)}),
 *      actions = { "read"},
 *      modules = { "developmentTools"})
 */
public class CmCheckIsOnlineConnectionService extends  CmCheckIsOnlineConnectionService_Gen{
	
	
    /**
     * Returns the PageBody
     * @param header PageHeader
     * @return DataElement pageBody
     * @throws ApplicationError ae
     */
    @Override
    protected DataElement read(PageHeader header) throws ApplicationError {
    	DataElement dataElement = new DataElement();
        if(isOnlineConnection()){
        	dataElement.put(STRUCTURE.CM_ONLINE_CONNECTION_SW, Bool.TRUE);
        }else{
        	dataElement.put(STRUCTURE.CM_ONLINE_CONNECTION_SW, Bool.FALSE);
        }
        return  dataElement;
    }
}
