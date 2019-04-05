package com.splwg.cm.domain.batch;

import com.splwg.base.api.ListFilter;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.shared.common.ServerMessage;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.splwg.ccb.domain.adjustment.adjustment.entity.AdjustmentCharacteristic_DTO;
import com.splwg.ccb.domain.adjustment.adjustment.entity.Adjustment_DTO;
import com.splwg.ccb.domain.adjustment.adjustment.entity.Adjustment_Id;
import com.splwg.ccb.domain.admin.approvalProfile.entity.ApprovalProfile_Id;
import com.splwg.ccb.domain.admin.customerClass.entity.CustomerClass;
import com.splwg.ccb.domain.customerinfo.account.entity.Account_Id;
import com.splwg.ccb.domain.customerinfo.person.entity.Person_Id;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.entity.ServiceAgreement_Id;














//import com.splwg.ccb.domain.customerinfo.serviceAgreement.entity.S;
import org.dom4j.Element;

import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.businessObject.COTSInstanceList;
import com.splwg.base.api.businessObject.COTSInstanceListNode;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.LookupHelper;
import com.splwg.base.api.datatypes.Money;
import com.splwg.base.api.lookup.OutboundMessageProcessingMethodLookup;
import com.splwg.base.api.serviceScript.ServiceScriptDispatcher;
import com.splwg.base.api.serviceScript.ServiceScriptInstance;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.ccb.domain.customerinfo.account.entity.AccountCharacteristic;
import com.splwg.ccb.domain.customerinfo.account.entity.AccountCharacteristics;
import com.splwg.ccb.domain.customerinfo.account.entity.AccountPerson;
import com.splwg.ccb.domain.customerinfo.account.entity.AccountPersons;
import com.splwg.ccb.domain.customerinfo.account.entity.Account_Id;
import com.splwg.base.api.businessService.BusinessServiceDispatcher;
import com.splwg.base.api.businessService.BusinessServiceInstance;
import com.splwg.base.domain.common.businessObject.BusinessObject_Id;
import com.splwg.base.domain.common.businessService.BusinessService_Id;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.base.domain.common.currency.Currency_Id;
import com.ibm.icu.math.BigDecimal;
import com.splwg.base.domain.common.message.MessageParameters;
import com.splwg.base.domain.common.script.Script_Id;

/**
 * @author Ramya
 *
@BatchJob (rerunnable = false,modules = { })
 */
public class CMMonitorDepositRefund extends CMMonitorDepositRefund_Gen {
	private static final Logger log = LoggerFactory.getLogger(CMMonitorDepositRefund.class);
	

	public JobWork getJobWork() {
		
		log.info("in get job work");
		String charType;//=getParameters().getCHAR() ;
		List<ThreadWorkUnit> workUnits = new ArrayList<ThreadWorkUnit>();
		
		
        StringBuilder  saQuery=new StringBuilder();
        		saQuery.append("SELECT SA_ID,SA.SA_TYPE_CD,SA.ACCT_ID,SA.END_DT FROM CI_SA SA ");
        		saQuery.append("WHERE SA.SA_TYPE_CD IN ('E-DEP','E-GENDP') ");
        		saQuery.append("AND SA.SA_STATUS_FLG='40'");
        		PreparedStatement saQuerySt = createPreparedStatement(saQuery.toString(), "getSAs");
                //System.out.println("before  execute");

        //PreparedStatement saQuerySt=createPreparedStatement(saQuery, "getSAs");
        saQuerySt.execute();
        //System.out.println("post execute");
        try {
			List<SQLResultRow> saQueryList = saQuerySt.list();
			if(saQueryList.size()==0){
				log.info("Their is no stopped deposit SA ");
			}
			System.out.println(saQueryList.size());
			for (SQLResultRow row : saQueryList)
            {
				ServiceAgreement_Id id = new ServiceAgreement_Id(row.getString("SA_ID"));
				log.info(id);
				//System.out.println(id);
				ThreadWorkUnit workUnit = new ThreadWorkUnit(id);
              workUnit.addSupplementalData("saId", row.getString("SA_ID"));
              workUnit.addSupplementalData("saTypeCd", row.getString("SA_TYPE_CD"));
              workUnit.addSupplementalData("accountId", row.getString("ACCT_ID"));
              workUnit.addSupplementalData("saEndDate", row.getString("END_DT"));
              workUnits.add(workUnit);
            }
			              
		} 
		finally {
			if (notNull(saQuerySt)) {
				saQuerySt.close();
			}
		}
        
      
        return createJobWorkForThreadWorkUnitList(workUnits);  
	 
		
		
		// TODO Auto-generated method stub

	}

	public Class<CMMonitorDepositRefundWorker> getThreadWorkerClass() {
		return CMMonitorDepositRefundWorker.class;
	}

	public static class CMMonitorDepositRefundWorker extends
			CMMonitorDepositRefundWorker_Gen {
		Date notifyDate;
		Date today;
		Date saEndDate;
		String accountId;
		Account_Id account_Id;
		ServiceAgreement_Id sa_id;
		Adjustment_Id adjustment_Id;
		String adjustmentId;
		Person_Id person_Id;
		String personId;
		String saId;
		String adjustmentType;
		String refundAmount;
		Money adjustmentAmount;
		String saStatus;
		String saType;
		String iban=null;
		String sapResponse=null;
		BusinessServiceInstance businessServiceInstance1;
		BusinessServiceInstance businessServiceInstance;
		

		public ThreadExecutionStrategy createExecutionStrategy() {
			// TODO Auto-generated method stub
			return new StandardCommitStrategy(this);
		}

		@SuppressWarnings("deprecation")
		public boolean executeWorkUnit(ThreadWorkUnit unit)
		//write the logic here
				throws ThreadAbortedException, RunAbortedException {
			  // TODO Auto-generated method stub
			log.info("in execute work unit");
			//System.out.println("in execute work unit");
			today = getProcessDateTime().getDate();
			
			saEndDate = Date.fromIso(unit.getSupplementallData("saEndDate").toString());
			notifyDate=saEndDate.addDays(15);
			log.info("saEndDate--"+saEndDate);
			log.info("NotifyDate--"+notifyDate);
			log.info("CompareDate--"+today);
			accountId=(String) unit.getSupplementallData("accountId");
			log.info(accountId);
			String serviceAgreementId = (String) unit.getSupplementallData("saId");
			//System.out.println(serviceAgreementId);
			
			if(notifyDate.compareTo(today)==0)
			{
				
				//Check Account class ; if not eligible terminate;
				account_Id=new Account_Id(accountId);
				CustomerClass custClass=account_Id.getEntity().getCustomerClass();
				String cc = custClass.getId().getIdValue().toString().trim();
				//System.out.println(cc);
				log.info("Customer class is"+ cc);
				if(cc.equals("ULG") || cc.equalsIgnoreCase("LG") || cc.equals("S")){
				log.info("customer class matched");
				//System.out.println("SA date after 15 days");
				log.info("account---"+accountId);	
				//System.out.println("account---"+accountId);
				
				String caseQuery="SELECT CAS.ACCT_ID FROM CI_CASE CAS"
    					+" WHERE CAS.ACCT_ID= :accountId"
    					+" AND CAS.CASE_TYPE_CD='CM-DEPREFUND'";
				PreparedStatement caseQuerySt=createPreparedStatement(caseQuery, "getCases");
				caseQuerySt.bindId("accountId", account_Id);
				caseQuerySt.execute();
				List<SQLResultRow> caseQueryList = caseQuerySt.list();
				log.info("----refund cases on the account are"+caseQueryList.size()+"-----");
				//System.out.println("----refund cases on the account "+caseQueryList.size()+"-----");
				try{
				if (caseQueryList.size()>0){
					log.info("----refund case exists on the account -----");
				}
				else {
					//System.out.println(caseQueryList.size());
					//System.out.println("Inside batch process for creating adjustments");
					//transfer refund amount along with iban number and SAP integration will be initiated
					//create to do if iban number not available
					BusinessService_Id bsID = new BusinessService_Id("CM-GETSDSA");
					BusinessServiceInstance businessServiceInstance = BusinessServiceInstance.create(bsID.getEntity());
					businessServiceInstance.set("accountId", accountId);
					BusinessServiceInstance bsInstance = BusinessServiceDispatcher.execute(businessServiceInstance);
					log.info("----Invoked Business Service-----");
					COTSInstanceList depSaList = bsInstance.getList("results");
					if(depSaList.getSize()==0){
						log.info("No deposit SA exists for this account");
					}
					else{
					Iterator<COTSInstanceListNode> depSA=depSaList.iterator();
					while(depSA.hasNext()){
						//depSA.next();
						COTSInstanceListNode depSANode = depSA.next();
						refundAmount=depSANode.getString("AMOUNT");
						saId=depSANode.getString("SA_ID");
					}
					log.info("Refund process for SA "+saId);
					log.info("refund amount is "+refundAmount);
					Currency_Id currencyId = new Currency_Id("TRY");
					Money refundMoney = new Money(new BigDecimal(refundAmount), currencyId);
					if(new BigDecimal(refundAmount).intValue()>0 || new BigDecimal(refundAmount).intValue()==0){
						log.info("No refund amount exists on this SA");
					}
					else{
					adjustmentAmount=refundMoney.negate();
					//System.out.println(adjustmentAmount);
					
					ListFilter<AccountCharacteristic>  acctCharsFilter =  account_Id.getEntity().getCharacteristics().createFilter("where this.id.characteristicType = :chrTypeCode", "");//.createFilter("where this.id.characteristicType = :chrTypeCode order by this.id.sequence DESC", "");
					CharacteristicType_Id chrSftId0 =  new CharacteristicType_Id("CM-IBAN");
					acctCharsFilter.bindId("chrTypeCode", chrSftId0);  
					AccountCharacteristic acctChar= acctCharsFilter.firstRow(); 
					if (isNull(acctChar)) {
						
					
                                        	  //create to -do 
                                        	  log.info("Iban not found , hence creating Todo");
                                        	 BusinessService_Id busSrvIDToDo = new BusinessService_Id("F1-AddToDoEntry"); 
                                  			BusinessServiceInstance busSrvToDo = BusinessServiceInstance.create(busSrvIDToDo.getEntity());
                                  			busSrvToDo.set("toDoType", "CM-IBAN");
                                  			busSrvToDo.set("toDoRole", "F1_DFLT");
                                  			busSrvToDo.set("drillKey1",accountId);
                                  			busSrvToDo.set("messageCategory", BigDecimal.valueOf(90000));
                                  			busSrvToDo.set("messageNumber", BigDecimal.valueOf(99311));
                                  			
                                  			
                                  			BusinessServiceInstance busSrvToDo1=BusinessServiceDispatcher.execute(busSrvToDo);
                                  			log.info("ToDoId : "+busSrvToDo1.getString("toDoEntryId"));
                                          }
                                          else{
                                        	  iban=acctChar.getAdhocCharacteristicValue().trim().toString();
                                        	  log.info("Iban number is "+ iban);
                                        	  adjustmentType="CM-DEREF";
                                        	  //add adjustment freeze it and send for SAP
                                        	  BusinessService_Id bsId12 = new BusinessService_Id("C1-CreateAdjustmentWithAppr");
                                        	BusinessServiceInstance businessServiceInstance1 = BusinessServiceInstance.create(bsId12.getEntity());
                          					businessServiceInstance1.set("saId", saId);
                          					businessServiceInstance1.set("adjustmentType",adjustmentType);
                          					businessServiceInstance1.set("adjustmentAmount",adjustmentAmount);
                          					//System.out.println("dfv"+businessServiceInstance.getList("").iterator().next());							
                          					//COTSInstanceListNode saNode = depSA.
                          					BusinessServiceInstance adjBS=BusinessServiceDispatcher.execute(businessServiceInstance1);
                          					
                          					adjustmentId=adjBS.getString("adjustmentId");
                          					
                          					//get Approval Profile Id
                          					
                          					
                          					if(adjustmentId!=null){
                          					log.info("Adjustment created is "+adjustmentId);
                          					//Invke Adjustment Bo and get approval profile
                          					BusinessObject_Id boId=new BusinessObject_Id("C1-Adjustment");
                          					BusinessObjectInstance boIns= BusinessObjectInstance.create(boId.getEntity());
                          					boIns.set("adjustmentId", adjustmentId);
                          					BusinessObjectInstance adjBS1=BusinessObjectDispatcher.read(boIns);
                          					//adjustmentId=adjBS1.getString("adjustmentId");
                          					String approvalProfile = adjBS1.getString("approvalRequestId");
                          					ApprovalProfile_Id approvalProfile_Id = new ApprovalProfile_Id(approvalProfile);
                          					
                          					//freeze adjustment
                          					
                          					//get To Do Id for freezing Adjustment.
                          					BusinessService_Id toDoBSrv = new BusinessService_Id("CM-GetTDIdForDrlKey");
                                        	BusinessServiceInstance businessServiceInstanceTodo = BusinessServiceInstance.create(toDoBSrv.getEntity());
                                        	businessServiceInstanceTodo.set("drlKeyValue", adjustmentId);
                          					BusinessServiceInstance adjToDoBS=BusinessServiceDispatcher.execute(businessServiceInstanceTodo);
                          					//log.info("");
                          					COTSInstanceList toDoList =adjToDoBS.getList("results");
                          					Iterator<COTSInstanceListNode> toDoItr=toDoList.iterator();
                          					String toDoId=null;
                          					while(toDoItr.hasNext()){
                        						//depSA.next();
                        						COTSInstanceListNode toDoValue = toDoItr.next();
                        						toDoId=toDoValue.getString("todoId");
                        						//saId=depSANode.getString("SA_ID");
                        					}
                          					
                          					ServiceScriptInstance serviceScriptAdjApr=ServiceScriptInstance.create("C1-AdjAprovS");
                          					serviceScriptAdjApr.getGroupFromPath("approvalRequest").set("approvalRequestId", approvalProfile);
                          					serviceScriptAdjApr.getGroupFromPath("approvalRequest").getGroupFromPath("approvalInfo").set("currentApprovalToDoId", toDoId);
                          					serviceScriptAdjApr.getGroupFromPath("approvalRequest").set("bo", "C1-AdjustmentApprovalRequest");
                          					serviceScriptAdjApr.set("boStatus", "APPROVED");
                          					//add date
                          					Date date = getProcessDateTime().getDate();
                          					serviceScriptAdjApr.getGroupFromPath("approvalRequest").getGroupFromPath("approvalInfo").set("accountingDate", getProcessDateTime().getDate());
                          					ServiceScriptDispatcher.invoke(serviceScriptAdjApr);

                          					
                          					// Invoke the service script for SAP integration
                          					DateTime documentDate = getProcessDateTime().getSystemDateTime();
                          					OutboundMessageProcessingMethodLookup transactionType=LookupHelper.getLookupInstance(OutboundMessageProcessingMethodLookup.class,"01");
                          					OutboundMessageProcessingMethodLookup status=LookupHelper.getLookupInstance(OutboundMessageProcessingMethodLookup.class,"01");
                          					OutboundMessageProcessingMethodLookup companyCodeLookup=LookupHelper.getLookupInstance(OutboundMessageProcessingMethodLookup.class,"1100"); 
                          					String transactionId=iban.toString()+documentDate.toString()+accountId;
                          					BusinessService_Id bsId1=new BusinessService_Id("WX-ACPER");
                          					BusinessServiceInstance businessServiceInstance13 = BusinessServiceInstance.create(bsId1.getEntity());
                          					businessServiceInstance13.set("accountId", accountId);
                          					businessServiceInstance13.set("isMainCustomer","Y");
                          					BusinessServiceInstance bsInstance2=BusinessServiceDispatcher.execute(businessServiceInstance13);
                          					log.info("Invoked account person BS");
                          					COTSInstanceList perList = bsInstance2.getList("results");
                        					if(perList.getSize()==0){
                        						//terminate with error here
                        					}
                        					else{
                        					Iterator<COTSInstanceListNode> per=perList.iterator();
                        					while(per.hasNext()){
                        						//depSA.next();
                        						COTSInstanceListNode perNode = per.next();
                        						personId=perNode.getString("personId");
                        					}
                        					}
                        					//Script_Id scrId=new Script_Id("CM-SAPBankIn");
                        					ServiceScriptInstance serviceScript=ServiceScriptInstance.create("CM-SAPBankIn");
                          					serviceScript.getGroupFromPath("sendDetails").getGroupFromPath("requestMessage").set("transactionId", transactionId);
                          					serviceScript.getGroupFromPath("sendDetails").getGroupFromPath("requestMessage").set("companyCode", companyCodeLookup);
                          					serviceScript.getGroupFromPath("sendDetails").getGroupFromPath("requestMessage").set("transactionType",transactionType);
                          					serviceScript.getGroupFromPath("sendDetails").getGroupFromPath("requestMessage").set("status",status);
                          					serviceScript.getGroupFromPath("sendDetails").getGroupFromPath("requestMessage").set("documentNumber", iban.toString());
                          					serviceScript.getGroupFromPath("sendDetails").getGroupFromPath("requestMessage").set("documentDate",documentDate);
                          					serviceScript.getGroupFromPath("sendDetails").getGroupFromPath("requestMessage").set("postingDate",documentDate);
                          					serviceScript.getGroupFromPath("sendDetails").getGroupFromPath("requestMessage").set("accountId",accountId);
                          					serviceScript.getGroupFromPath("sendDetails").getGroupFromPath("requestMessage").set("tcId",personId);
                          					serviceScript.getGroupFromPath("sendDetails").getGroupFromPath("requestMessage").set("city","Diyarbakir");
                          					serviceScript.getGroupFromPath("sendDetails").getGroupFromPath("requestMessage").set("dueDate","99991231");
                          					serviceScript.getGroupFromPath("sendDetails").getGroupFromPath("requestMessage").set("region","Diyarbakir");
                          					serviceScript.getGroupFromPath("sendDetails").getGroupFromPath("requestMessage").set("amount",adjustmentAmount.getAmount());
                          					ServiceScriptDispatcher.invoke(serviceScript);
                          					COTSInstanceList sapList = serviceScript.getGroupFromPath("responseDetail").getGroupFromPath("responseMessage").getGroupFromPath("messages").getList("item");
                        					
                          					if(sapList.getSize()==0){
                        						//terminate with error here
                        						log.info("SAP response not received");
                        						sapResponse="Success";
                        					}
                        					else{
                        						log.info("Record sent to SAP");
                        						//sapResponse="Success";
                        						Iterator<COTSInstanceListNode> sap=sapList.iterator();
                        						while(sap.hasNext()){
                        						
                        						COTSInstanceListNode sapNode = sap.next();
                        						sapResponse = sapNode.getString("messageText");
                        						//sapResponse="Success";
                        					}
                        					//}
                        					
                        					//Add SAP resopnse on adjustment here
                        					//sapResponse
                        					log.info("----"+sapResponse+"-----");
                        					//Invoke BS C1AdjustmentUpdate
                        					BusinessService_Id sapBS=new BusinessService_Id("C1AdjustmentUpdate");
                          					BusinessServiceInstance sapIns = BusinessServiceInstance.create(sapBS.getEntity());
                          					sapIns.set("adjustmentId", adjustmentId);
                          				   COTSInstanceList charListForBS = sapIns.getList("adjChar"); 
                                  		  COTSInstanceListNode nodeChar = charListForBS.newChild();
                                  		  log.info("Adding char on adjustment");
                                  		
                                  		nodeChar.set("characteristicType2","CM-SAPIN");
                                  		nodeChar.set("adhocCharacteristicValue", sapResponse);
                                      	BusinessServiceInstance sapIns2 = BusinessServiceDispatcher.execute(sapIns); 
                                      	log.info("char Added on adjustment");
                          					
                        					}
                          					
                                          }  
                          					}

				}
					}
				}
				}
				finally {
					if (notNull(caseQuerySt)) {
						caseQuerySt.close();
					}
				}
				}
				else
				{
					log.info("Invalid customer class, so skipping this record");
					
					
				}
				
				
				}
				
						return true;
		}

}
}
