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
 * This Batch Process is used to assign Email Bill Id to Bill.
 * It Should be Scheduled after Billing Batch 
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

package com.splwg.cm.domain.billing.batch;

import com.splwg.base.api.Query;
import com.splwg.base.api.batch.CommitEveryUnitStrategy;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.lookup.MessageSeverityLookup;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.ccb.api.lookup.BillStatusLookup;
import com.splwg.ccb.domain.admin.billRouteType.entity.BillRouteType_Id;
import com.splwg.ccb.domain.billing.bill.entity.Bill;
import com.splwg.ccb.domain.billing.bill.entity.Bill_Id;
import com.splwg.cm.domain.billing.batch.customBusinessComponent.CmAddEmailBillIdToBill;
import com.splwg.cm.domain.billing.batch.customBusinessComponent.CmAddPostalBillIdToBill;
import com.splwg.cm.domain.billing.customMessages.CustomMessageRepository;

/**
 * @author sandeep
 *
@BatchJob (rerunnable = false,
 *      modules = { "developmentTools"},
 *      softParameters = { @BatchJobSoftParameter (entityName = billRouteType, name = emailBillRouteType, required = true, type = entity)
 *            , @BatchJobSoftParameter (entityName = characteristicType, name = emailBillIdCharType, required = true, type = entity)
 *            , @BatchJobSoftParameter (name = billIdPrefix, required = true, type = string)})
 */
public class CmAssignEmailBillIdBatchProcess extends
		CmAssignEmailBillIdBatchProcess_Gen {
	
	 private static final com.splwg.shared.logging.Logger logger = com.splwg.shared.logging.LoggerFactory
	            .getLogger(CmAssignEmailBillIdBatchProcess.class);

	public JobWork getJobWork() {
		
		Query unassignedBillQry = createQuery("from AccountPerson ap, Bill bill where ap.id.account.id = bill.account.id and "
				+ "ap.billRouteTypeId = :billRouteTypeId and bill.billStatus = :billStatus and not exists ( select bc from BillCharacteristic bc where bc.id.bill.id = bill.id  and bc.id.characteristicType.id = :emailBillIdCharTypeId )","");
		unassignedBillQry.bindId("billRouteTypeId", getParameters().getEmailBillRouteType().getId());
		unassignedBillQry.bindLookup("billStatus", BillStatusLookup.constants.COMPLETE);
		unassignedBillQry.bindId("emailBillIdCharTypeId", getParameters().getEmailBillIdCharType().getId());
		unassignedBillQry.addResult("bill", "bill");
		unassignedBillQry.selectDistinct(true);
		//unassignedBillQry.setMaxResults(1000);
		return createJobWorkForEntityQuery(unassignedBillQry);

	}

	public Class<CmAssignEmailBillIdBatchProcessWorker> getThreadWorkerClass() {
		return CmAssignEmailBillIdBatchProcessWorker.class;
	}

	public static class CmAssignEmailBillIdBatchProcessWorker extends
			CmAssignEmailBillIdBatchProcessWorker_Gen {

		public ThreadExecutionStrategy createExecutionStrategy() {
			// TODO Auto-generated method stub
			return new CommitEveryUnitStrategy(this);
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			Bill bill = ((Bill_Id)unit.getPrimaryId()).getEntity();
			try{
				String customBillId = CmAddEmailBillIdToBill.addEmailBillId(bill, getParameters().getEmailBillIdCharType(),getParameters().getBillIdPrefix());
				logMessage(CustomMessageRepository.emailBillIdAssignedtoBill(customBillId, bill.getId().getTrimmedValue()), MessageSeverityLookup.constants.INFORMATIONAL);
				return true;
			}catch(Exception e){
			    logger.error("", e);
				throw new RunAbortedException(CustomMessageRepository.assignEmailBillIdBatchAborted(getBatchControlId().getTrimmedValue(),getBatchNumber().toString() ,bill.getId().getTrimmedValue(), e.getMessage()));
			}
			
		}

	}
	
/*	GET JOB WORK QUERY 
	SELECT * FROM CI_ACCT_PER A, CI_BILL B  where A.ACCT_ID = B.ACCT_ID AND A.BILL_RTE_TYPE_CD = 'POSTAL' AND B.BILL_STAT_FLG = 'C'
	AND NOT EXISTS (SELECT 1 FROM CI_BILL_CHAR BC1 WHERE B.BILL_ID = BC1.BILL_ID AND  BC1.CHAR_TYPE_CD = 'X')


	GET PREVIOUS BILL ID Query
	select MAX(ADHOC_CHAR_VAL) FROM CI_BILL B, CI_BILL_CHAR BC WHERE B.BILL_ID = BC.BILL_ID  AND BC1.CHAR_TYPE_CD like '%' AND YEAR(B.BILL_DT) = 2017
	
	SELECT BS.BSEG_ID,
    SA.CIS_DIVISION,
    SA.SA_TYPE_CD,
    FTGL.DST_ID, FTGL.AMOUNT, 
    FT.CUR_AMT,
    BSL.DST_ID, UOM_CD, TOU_CD, SQI_CD, CALC_AMT, BASE_AMT, BILL_SQ,FT_TYPE_FLG
FROM 
    CI_BSEG BS,
    CI_BSEG_CALC_LN BSL,
    CI_SA SA,
    CI_FT FT,
    CI_FT_GL FTGL
WHERE FT.SIBLING_ID = BS.BSEG_ID
    AND FTGL.FT_ID = FT.FT_ID
    AND BS.BSEG_ID = BSL.BSEG_ID
    AND BS.SA_ID = SA.SA_ID 
    AND (FT_TYPE_FLG ='BS' OR FT_TYPE_FLG ='BX')
    AND BSL.DST_ID = FTGL.DST_ID;
*/

}
