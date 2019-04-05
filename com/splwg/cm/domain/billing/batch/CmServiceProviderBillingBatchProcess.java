package com.splwg.cm.domain.billing.batch;

import com.splwg.base.api.Query;
import com.splwg.base.api.batch.CommitEveryUnitStrategy;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Date;
import com.splwg.ccb.api.lookup.UnableToCompleteBillActionLookup;
import com.splwg.ccb.domain.billing.bill.entity.Bill;
import com.splwg.ccb.domain.billing.data.BillCompletionInputData;
import com.splwg.ccb.domain.billing.data.BillGenerationData;
import com.splwg.ccb.domain.customerinfo.account.entity.Account;
import com.splwg.ccb.domain.customerinfo.account.entity.Account_Id;
import com.splwg.cm.domain.billing.customMessages.CustomMessageRepository;
import com.splwg.shared.common.ServerMessage;

/**
 * @author Pervacio
 *
@BatchJob (rerunnable = false,
 *      modules = { },
 *      softParameters = { @BatchJobSoftParameter (entityName = customerClass, name = supplierCustomerClass, required = true, type = entity)})
 */
public class CmServiceProviderBillingBatchProcess extends
		CmServiceProviderBillingBatchProcess_Gen {

	public JobWork getJobWork() {
		//Account.properties.customerClassId
		Query query = createQuery("from Account a where a.customerClass = :supplierCustomerClass","Get Service Providers Accounts");
		query.bindEntity("supplierCustomerClass",getParameters().getSupplierCustomerClass());
		return createJobWorkForEntityQuery(query);
	}

	public Class<CmServiceProviderBillingBatchProcessWorker> getThreadWorkerClass() {
		return CmServiceProviderBillingBatchProcessWorker.class;
	}

	public static class CmServiceProviderBillingBatchProcessWorker extends
			CmServiceProviderBillingBatchProcessWorker_Gen {

		public ThreadExecutionStrategy createExecutionStrategy() {
			return new CommitEveryUnitStrategy(this);
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			Account account = ((Account_Id)unit.getPrimaryId()).getEntity();
			
			 BillGenerationData billGenerateData = BillGenerationData.Factory.newInstance();
             Date accountingDate = getSystemDateTime().getDate();
		     Date cutoffDate = accountingDate;
		     Date billDate = accountingDate;
		     billGenerateData.setAccountingDate(accountingDate);
		     billGenerateData.setCutoffDate(cutoffDate);
		    
		     Bill newBill = account.generateAndFreezeBill(billGenerateData);
		     
		     BillCompletionInputData billCompleteInputData = BillCompletionInputData.Factory.newInstance();
		     billCompleteInputData.setAccountingDate(accountingDate);
		     billCompleteInputData.setBillDate(billDate);
		     billCompleteInputData.setUnableToCompleteBillAction(UnableToCompleteBillActionLookup.constants.SHOW_ERROR);
		     newBill.complete(billCompleteInputData);
		     ServerMessage sm = CustomMessageRepository.billingServiceProviderBillGenerated(account.getId().getTrimmedValue(),newBill.getId().getTrimmedValue() , accountingDate.toLocalizedString());
		     logInfo(sm);
		     return true;
		}
	}
}