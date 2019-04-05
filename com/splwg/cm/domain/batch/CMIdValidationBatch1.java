package com.splwg.cm.domain.batch;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import tr.gov.nvi.kpsv2.model.BilesikKutukModel;
import tr.gov.nvi.kpsv2.sample.beans.AyarlarBean;

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
import com.splwg.base.api.businessObject.COTSInstanceList;
import com.splwg.base.api.businessObject.COTSInstanceListNode;
import com.splwg.base.api.businessObject.COTSInstanceNode;
import com.splwg.base.api.businessService.BusinessServiceInstance;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.LookupHelper;
import com.splwg.base.api.lookup.OutboundMessageProcessingMethodLookup;
import com.splwg.base.api.serviceScript.ServiceScriptDispatcher;
import com.splwg.base.api.serviceScript.ServiceScriptInstance;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.businessService.BusinessService;
import com.splwg.base.domain.common.businessService.BusinessService_Id;
import com.splwg.ccb.domain.customerinfo.customerContact.entity.CustomerContactCharacteristic;
import com.splwg.ccb.domain.customerinfo.customerContact.entity.CustomerContact_Id;
import com.splwg.ccb.domain.customerinfo.person.entity.PersonCharacteristics;
import com.splwg.ccb.domain.customerinfo.person.entity.PersonId;
import com.splwg.ccb.domain.customerinfo.person.entity.PersonId_Id;
import com.splwg.ccb.domain.customerinfo.person.entity.PersonIds;
import com.splwg.ccb.domain.customerinfo.person.entity.Person_Id;
import com.splwg.shared.common.ApplicationError;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Arnab
 *
@BatchJob (rerunnable = false,modules = { },
 *      softParameters = { @BatchJobSoftParameter (name = STATUS, type = string)
 *            , @BatchJobSoftParameter (name = USER_NAME, type = string)
 *            , @BatchJobSoftParameter (name = PASSWORD, type = string)})
 */

public class CMIdValidationBatch1 extends CMIdValidationBatch1_Gen{
	private static Logger logger = LoggerFactory.getLogger(CMIdValidationBatch.class);
	private static COTSInstanceNode bo1Group = null;
	
	public JobWork getJobWork() {
		logger.info("in getJobWork");
		String status = getParameters().getSTATUS();
		
		
		List<ThreadWorkUnit> workUnits = new ArrayList<ThreadWorkUnit>();
		String per_ids = "SELECT * FROM CI_PER_CHAR where char_type_cd='CM-IDVAL' and (ADHOC_CHAR_VAL ='"+status+"' or CHAR_VAL ='"+status+"')";
		//4290521103
		//9676198136
		PreparedStatement per_idsst = createPreparedStatement(per_ids,
				"getper_ids");
		per_idsst.execute();
		List<SQLResultRow> per_idsList = per_idsst.list();
		logger.info("in getJobWork COUNT" + per_idsList.size());
		if (per_idsList.size() > 0) {
			for (SQLResultRow row : per_idsList) {
				ThreadWorkUnit workUnit = new ThreadWorkUnit();
				workUnit.addSupplementalData("per_id", row.getString("PER_ID"));

				workUnits.add(workUnit);
			}

		}
		return createJobWorkForThreadWorkUnitList(workUnits);
	}

	public Class<CMIdValidationBatch1Worker> getThreadWorkerClass() {
		return CMIdValidationBatch1Worker.class;
	}

	public static class CMIdValidationBatch1Worker extends CMIdValidationBatch1Worker_Gen {
		String per_id;
		private static COTSInstanceList boList = null;

		public ThreadExecutionStrategy createExecutionStrategy() {
			// TODO Auto-generated method stub
			return new StandardCommitStrategy(this);
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			logger.info("PRINTING HERE");
			logger.info("in JAVA main");
			
			String 	loginName = getParameters().getUSER_NAME(); //KRM-12131640
			//String 	loginName = "KRM-12131640";

			String 	password = getParameters().getPASSWORD(); //.dep632334saS
			// String 	password = ".dep632334saS"; //.dep632334saS

			per_id = (String) unit.getSupplementallData("per_id");
			Person_Id perid = new Person_Id(per_id);
			logger.info("per_id is::::"+per_id);
			logger.info("per_id is!!!!::::"+per_id);
			
			PersonIds perIds = perid.getEntity().getIds();
			
            for(Iterator<PersonId> personId = perIds.iterator();personId.hasNext();)
            {
                            
            	PersonId pid = personId.next();
            	
            	String IdType = pid.fetchIdIdType().getId().getIdValue().toString().trim();
            	String IdVal = pid.getPersonIdNumber().trim();
            	String personIdS=pid.getId().toString();
            	logger.info("pid is:::"+pid);

            	logger.info("person ID is:::"+per_id);
            	logger.info("IdType is::::: "+IdType);
            	
            	
            	logger.info("IdVal is::::: "+IdVal);
            	 long turkishId = Long.parseLong(IdVal);
            		
            	if(IdType.equalsIgnoreCase("VKN")){
            		
            		ServiceScriptInstance scriptInstance = ServiceScriptInstance
   							.create("CM-IDBatVal");
   					scriptInstance.getElement().addElement("idnumber")
   					.setText(IdVal);
   					scriptInstance.getElement().addElement("IdType")
   					.setText("VKN");
   					scriptInstance.getElement().addElement("perId")
   					.setText(per_id);
   					ServiceScriptDispatcher.invoke(scriptInstance);
            		
            	}
            	else if(IdType.equalsIgnoreCase("TCN")){
            		ServiceScriptInstance scriptInstance = ServiceScriptInstance
   							.create("CM-IDBatVal");
   					scriptInstance.getElement().addElement("idnumber")
   					.setText(IdVal);
   					scriptInstance.getElement().addElement("IdType")
   					.setText("TCN");
   					scriptInstance.getElement().addElement("perId")
   					.setText(per_id);
   					ServiceScriptDispatcher.invoke(scriptInstance);
            	}
			
            }
            return true;

		}
		public void finalizeThreadWork() throws ThreadAbortedException,
		RunAbortedException {
		}

	}

}
