package com.splwg.cm.domain.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import org.dom4j.Element;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.ListFilter;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.businessObject.COTSFieldDataAndMD;
import com.splwg.base.api.businessObject.COTSInstanceList;
import com.splwg.base.api.businessObject.COTSInstanceListNode;
import com.splwg.base.api.businessObject.COTSInstanceNode;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.LookupHelper;
import com.splwg.base.api.lookup.OutboundMessageProcessingMethodLookup;
import com.splwg.base.api.service.Header;
import com.splwg.base.api.serviceScript.ServiceScriptDispatcher;
import com.splwg.base.api.serviceScript.ServiceScriptInstance;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.characteristicType.CharacteristicType;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
import com.splwg.ccb.domain.admin.customerClass.entity.CustomerClass_Id;
import com.splwg.ccb.domain.customerinfo.account.entity.Account_Id;
import com.splwg.ccb.domain.customerinfo.customerContact.entity.CustomerContact;
import com.splwg.ccb.domain.customerinfo.customerContact.entity.CustomerContactCharacteristic;
import com.splwg.ccb.domain.customerinfo.customerContact.entity.CustomerContactCharacteristic_DTO;
import com.splwg.ccb.domain.customerinfo.customerContact.entity.CustomerContactCharacteristics;
import com.splwg.ccb.domain.customerinfo.customerContact.entity.CustomerContact_DTO;
import com.splwg.ccb.domain.customerinfo.customerContact.entity.CustomerContact_Id;
import com.splwg.shared.common.ApplicationError;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Koushik
 *
 @BatchJob (rerunnable = false,modules = { }, softParameters = { @BatchJobSoftParameter
 *           (name = SMS_STATUS, required = true, type = string) , @BatchJobSoftParameter
 *           (name = PACKET_ID, required = true, type = string)})
 */
public class CMMonitorSMS extends CMMonitorSMS_Gen {
	private static Logger logger = LoggerFactory.getLogger(CMMonitorSMS.class);
	private static COTSInstanceNode bo1Group = null;
	
	public JobWork getJobWork() {
		logger.info("in getJobWork");

		
		List<ThreadWorkUnit> workUnits = new ArrayList<ThreadWorkUnit>();
		String cc_ids = "SELECT CC_ID FROM ci_cc_char where char_type_cd='CM-SMSTA' and ADHOC_CHAR_VAL !='DELIVERED'";
		//4290521103
		//9676198136
		PreparedStatement cc_idsst = createPreparedStatement(cc_ids,
				"getcc_ids");
		cc_idsst.execute();
		List<SQLResultRow> cc_idsList = cc_idsst.list();
		logger.info("in getJobWork COUNT" + cc_idsList.size());
		if (cc_idsList.size() > 0) {
			for (SQLResultRow row : cc_idsList) {
				ThreadWorkUnit workUnit = new ThreadWorkUnit();
				workUnit.addSupplementalData("cc_id", row.getString("CC_ID"));

				workUnits.add(workUnit);
			}

		}
		return createJobWorkForThreadWorkUnitList(workUnits);
	}

	public Class<CMMonitorSMSWorker> getThreadWorkerClass() {
		return CMMonitorSMSWorker.class;
	}

	public static class CMMonitorSMSWorker extends CMMonitorSMSWorker_Gen {
		String cc_id;
		private static COTSInstanceList boList = null;

		public ThreadExecutionStrategy createExecutionStrategy() {
			// TODO Auto-generated method stub
			return new StandardCommitStrategy(this);
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			logger.info("PRINTING HERE");
			System.out.println("in JAVA main");
			String packetId = "transactionId";
			String SmsStatus = null;
			cc_id = (String) unit.getSupplementallData("cc_id");
			System.out.println("cc id is::::"+cc_id);
			logger.info("cc id is!!!!::::"+cc_id);

			ListFilter<QueryResultRow> ccId = new CustomerContact_Id(cc_id)
					.getEntity().getCharacteristics()
					.createFilter("where char_type_Cd='CM-PACID' ", "");
			System.out.println("ccId" + ccId.firstRow());
			CustomerContactCharacteristic ccCharsPacketIdAdhocValue = (CustomerContactCharacteristic) ccId
					.firstRow();
			System.out.println(ccCharsPacketIdAdhocValue
					.getAdhocCharacteristicValue());

			String ccCharsPacketIdAdhocVal = ccCharsPacketIdAdhocValue
					.getAdhocCharacteristicValue();

			try {
				 System.out.println("inside try 1");
				
				 BusinessObjectInstance  businessObjectInstance1= BusinessObjectInstance.create("CM-SMSStatus");
				 
				 businessObjectInstance1.set("externalSystem", "CM-SMS");
				 businessObjectInstance1.set("outboundMessageType", "CM-SMSSTATU");
				 
				 OutboundMessageProcessingMethodLookup addressTypeLookup=LookupHelper.getLookupInstance(OutboundMessageProcessingMethodLookup.class,"F1RT"); 
				 logger.info("lookup::::::::::::::::"+addressTypeLookup);
				 businessObjectInstance1.set("processingMethod", addressTypeLookup);

				bo1Group = businessObjectInstance1.getGroup("sendDetails").getGroup("requestMessage");
				System.out.println("transaction ID is::::"+new BigDecimal(ccCharsPacketIdAdhocVal));
				
				bo1Group.set("transactionId", new BigDecimal(ccCharsPacketIdAdhocVal));
				
				System.out.println("schema is tid"+businessObjectInstance1.getDocument().asXML());

				logger.info("before calling BusinessObjectDispatcher.add");
			 	BusinessObjectInstance bsResult1 = BusinessObjectDispatcher.add(businessObjectInstance1);
			 	
			 	
			 	
				logger.info("bsResult1.getGroup(responseDetail)"+ bsResult1.getGroup("responseDetail"));

				logger.info("bsResult1.getGroup(responseDetail).getGroup(responseMessage)"+ bsResult1.getGroup("responseDetail").getGroup("responseMessage"));

				logger.info("bsResult1.getGroup(responseDetail).getGroup(responseMessage).getBoolean(success)"+ bsResult1.getGroup("responseDetail").getGroup("responseMessage").getBoolean("success"));
				logger.info(".getString(errorCode)::"+ bsResult1.getGroup("responseDetail").getGroup("responseMessage").getString("errorCode"));
				logger.info(".getString(errorDesc)::"+ bsResult1.getGroup("responseDetail").getGroup("responseMessage").getString("errorDesc"));

			 	Bool successResult = bsResult1.getGroup("responseDetail").getGroup("responseMessage").getBoolean("success");
			 	System.out.println("successResult is:::"+successResult);
			 	logger.info("successResult is:::"+successResult);
			 	if(successResult!=null){
			 	if(successResult.isTrue()){
				 	logger.info("Inside successResult.isTrue()");

				boList= bsResult1.getGroup("responseDetail").getGroup("responseMessage").getList("data");
				System.out.println("this.boList.getSize() is:: "+this.boList.getSize());
				if((notNull(this.boList)) && (this.boList.getSize() > 0)){
					 
                     Iterator<COTSInstanceListNode> sqIterator = this.boList.iterator();
                     while (sqIterator.hasNext()) {
                           COTSInstanceListNode sqListNode = (COTSInstanceListNode)sqIterator.next();
                           
                           COTSInstanceNode statusNode= sqListNode.getGroup("deliverStatus");
                         
                           if(notNull(statusNode.getNumber("status"))){
                        	   BigDecimal  statusCodeB=  statusNode.getNumber("status");
                          int statusCode = statusCodeB.intValue();
                        	   if(statusCode==0){
                        		   SmsStatus="Servis sağlayıcıya iletilmeyi bekliyor"; 
                        	   }
                        	   if(statusCode==1){
                        		   SmsStatus="Beklemede"; 

                        	   }
                        	   if(statusCode==2){
                        		   SmsStatus="DELIVERED"; 

                        	   }
                        	   if(statusCode==3){
                        		   SmsStatus="İletilmedi"; 

                        	   }
                        	   if(statusCode==10){
                        		   SmsStatus="Mükerrer"; 

                        	   }
                        	   if(statusCode==11){
                        		   SmsStatus="Kara listede"; 

                        	   }
                        	   if(SmsStatus!=null){
                        	   ServiceScriptInstance scriptInstance = ServiceScriptInstance
           							.create("CM-SMSStatus");
           					scriptInstance.getElement().addElement("ccId")
           					.setText(cc_id);
           					scriptInstance.getElement().addElement("statusDesc")
           					.setText(SmsStatus);
           					ServiceScriptDispatcher.invoke(scriptInstance);

                        	   }
                        	                              
                           }
                      }
			 	 }
				 
			 }
			 	
			 	else if(successResult.isFalse()){
				 	logger.info("Inside successResult.isFalse()");

			 		String errorCodeResult = bsResult1.getGroup("responseDetail").getGroup("responseMessage").getString("errorCode");
				 	String errorDescResult = bsResult1.getGroup("responseDetail").getGroup("responseMessage").getString("errorDesc");
				 	
				 	SmsStatus = errorDescResult;
				 	
				 	System.out.println("errorCodeResult is:::"+errorCodeResult);
				 	logger.info("errorCodeResult is:::"+errorCodeResult);
				 	
				 	
				 	
				 	System.out.println("errorDescResult is:::"+errorDescResult);
				 	logger.info("errorDescResult is:::"+errorDescResult);
				 	
				 	ServiceScriptInstance scriptInstance = ServiceScriptInstance
							.create("CM-SMSStatus");
					scriptInstance.getElement().addElement("ccId")
					.setText(cc_id);
					scriptInstance.getElement().addElement("statusDesc")
					.setText(errorDescResult);
					ServiceScriptDispatcher.invoke(scriptInstance);

			 	}
			 	
			 	
			}
			}
			catch (ApplicationError e) {
				System.out.println("in catch block");
				e.printStackTrace();
				
				
			}

			return true;
		}
		public void finalizeThreadWork() throws ThreadAbortedException,
		RunAbortedException {
		}

	}

}
