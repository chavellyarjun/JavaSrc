package com.splwg.cm.domain.batch;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.batch.ThreadWorker;
import com.splwg.base.api.batch.WorkUnitResult;
import com.splwg.base.api.serviceScript.ServiceScriptDispatcher;
import com.splwg.base.api.serviceScript.ServiceScriptInstance;


/**
 * @author Arnab
 *
@BatchJob (rerunnable = false,modules = { },
 *      softParameters = { @BatchJobSoftParameter (name = START_DATE, type = string)
 *            , @BatchJobSoftParameter (name = END_DATE, type = string)
 *            , @BatchJobSoftParameter (name = CORPORATE_CODE, type = string)
 *            , @BatchJobSoftParameter (name = LOGIN_NAME, type = string)
 *            , @BatchJobSoftParameter (name = PASSWORD, type = string)})
 */
public class GetEinvoiceReceiverListUpdates extends GetEinvoiceReceiverListUpdates_Gen{
	
//	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
//
//	String dateString = format.format( new Date());
//	 

public JobWork getJobWork() {
		
		List<ThreadWorkUnit> dummyThreadWorkUnitsList = new ArrayList<ThreadWorkUnit>();
		ThreadWorkUnit dummyThreadWorkUnit = new ThreadWorkUnit();
		dummyThreadWorkUnitsList.add(dummyThreadWorkUnit);
			
		return createJobWorkForThreadWorkUnitList(dummyThreadWorkUnitsList);
		
	}
public Class<GetEinvoiceReceiverListUpdatesWorker> getThreadWorkerClass() {
	return GetEinvoiceReceiverListUpdatesWorker.class;
}

public static class GetEinvoiceReceiverListUpdatesWorker extends
GetEinvoiceReceiverListUpdatesWorker_Gen {
	
	
	public boolean executeWorkUnit(ThreadWorkUnit unit)
			throws ThreadAbortedException, RunAbortedException {
		String corporateCode = getParameters().getCORPORATE_CODE();
		String 	loginName = getParameters().getLOGIN_NAME();
		String 	password = getParameters().getPASSWORD();
	   	
		String startDate = getParameters().getSTART_DATE();
		String endDate=getParameters().getEND_DATE();

		try{
			System.out.println("inside try");
			ServiceScriptInstance scriptInstance = ServiceScriptInstance
					.create("CM-EInvRcv");
			scriptInstance.getElement().addElement("startDate")
			.setText(startDate);
			scriptInstance.getElement().addElement("endDate")
			.setText(endDate);
			scriptInstance.getElement().addElement("password")
			.setText(password);
			scriptInstance.getElement().addElement("userName")
			.setText(loginName);
			scriptInstance.getElement().addElement("corporateCode")
			.setText(corporateCode);
			
			ServiceScriptDispatcher.invoke(scriptInstance);
		}
		catch(Exception e){
			e.printStackTrace();
		}
				return true;
		
	}
	private String addSupplementalData(String string, String string2) {
		// TODO Auto-generated method stub
		return null;
	}
	public ThreadExecutionStrategy createExecutionStrategy() {
		// TODO Auto-generated method stub
		return new StandardCommitStrategy(this);
	}
	}
	
}
	

