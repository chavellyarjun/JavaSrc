package com.splwg.cm.domain.installation.servicePointMeasurementCycleScheduleRoute.batch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.tools.ant.taskdefs.SQLExec.DelimiterType;

import com.splwg.base.api.batch.CommitEveryUnitStrategy;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.SingleTransactionStrategy;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.batch.WorkUnitResult;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.businessObject.COTSInstanceNode;
import com.splwg.base.api.file.reader.DelimitedFileReader;
import com.splwg.base.api.file.reader.GenericFileReader;
import com.splwg.base.api.file.reader.GenericFileReaderHelper;
import com.splwg.base.api.file.reader.SimpleDelimitedRecord;
import com.splwg.cm.domain.businessService.CmDifferenceLatLong;
import com.splwg.cm.domain.customMessages.CmMessageRepository;
import com.splwg.d1.domain.admin.measurementCycle.entities.MeasurementCycle_Id;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author
 *
 @BatchJob (modules = {},
 *            softParameters = {@BatchJobSoftParameter (name = fileName, required = true,type = string)
 *            ,@BatchJobSoftParameter (name = filePath, required = true,type = string)
 *            ,@BatchJobSoftParameter (name = activityId, required = true,type = string)})
 */

public class CmFileReadingProcess extends
CmFileReadingProcess_Gen {
	public static Logger logger = LoggerFactory.getLogger(CmFileReadingProcess.class);
	public static final String ROUTE_IMPORT_FACT_BO = "CM-RouteImportFact";
	public static final String ROUTE_IMPORT_DATA = "routeImportData";
	public static final String SP_ID = "servicePoint";
	public static final String OLD_ROUTE = "oldRoute";
	public static final String NEW_ROUTE = "newRoute";
	public static final String ERROR_DESC = "errorDesc";
	public static final String STATUS = "status";
	public static final String ROW_NUM = "rowNo";
	public static final String ACTIVITY_ID = "activityId";
	private static String filePath = null;
	private static String fileName = null;
	private static String activityId = null;
	
	public static MeasurementCycle_Id measurementCycleId = new MeasurementCycle_Id("ONSP");
	
	public void validateSoftParameters(boolean isNewRun) {
		File file = null;
		File readFile = null;
		//Initializing soft parameters
		filePath =  getParameters().getFilePath();
   	    fileName = getParameters().getFileName();
   	    activityId = getParameters().getActivityId();	
		
		//Validating Soft Parameters
		if(!filePath.endsWith("\\")){
			filePath = filePath+'\\';}
		file =new File(filePath);
		//Validating File Path
		if(file.isAbsolute() || file.isDirectory()){
			readFile =new File(filePath+fileName);
			//Validating File Name
			if(!readFile.exists()){
				addError(CmMessageRepository.invalidFileName(fileName));
			}
		}
		else{
			addError(CmMessageRepository.invalidFilePath(fileName));
		}
		
		
	}
	
	
	public JobWork getJobWork() {
		List<ThreadWorkUnit> workUnitList= new ArrayList<ThreadWorkUnit>();
		logger.info("inside job worker");	
		ThreadWorkUnit workUnit = new ThreadWorkUnit();
        workUnit.addSupplementalData("filePath",getParameters().getFilePath() );
        workUnit.addSupplementalData("fileName",getParameters().getFileName());
        workUnit.addSupplementalData("activityId",getParameters().getActivityId());
        workUnitList.add(workUnit);
		return createJobWorkForThreadWorkUnitList(workUnitList);
	}
	
	@Override
	public Class<CmFileReadingProcessWorker> getThreadWorkerClass() {
		return CmFileReadingProcessWorker.class;
		
	}
	
	public static class CmFileReadingProcessWorker extends
	CmFileReadingProcessWorker_Gen {
		
		public BusinessObjectInstance factInstance = null;		
		public void initializeThreadWork(boolean initializationPreviouslySuccessful) throws ThreadAbortedException,
	 	RunAbortedException {
			//JobParameters parameters = getParameters();
	    	 logger.info(filePath+"  "+fileName+"  "+activityId);
	    }
		public ThreadExecutionStrategy createExecutionStrategy() 
        {
    		return new CommitEveryUnitStrategy(this);
        }
		public boolean executeWorkUnit(ThreadWorkUnit unit) throws ThreadAbortedException, RunAbortedException {
			// TODO Auto-generated method stub	  
		  logger.info("inside exceute work unit");
		  String spRouteData = null; 
	      List<String> spList = new ArrayList<String>();
	      List<String> tempSpList = new ArrayList<String>();
	      List<String> routeList = new ArrayList<String>();
	      try{ 
	    	  GenericFileReader reader;
	    	  reader = GenericFileReaderHelper.openFileReader(filePath+fileName, StandardCharsets.UTF_8.name());
	    	  //FileReader karneFileReader = new FileReader(filePath+fileName); 
	    	 // BufferedReader br = new BufferedReader(karneFileReader); 
	    	  char delimiter = ';';
	    	  DelimitedFileReader delimitedFileReader = reader.createDelimitedFileReader(delimiter);
	    	  SimpleDelimitedRecord record = delimitedFileReader.getNextRecord();
	    	  while (null != record) {  
	    	       // String spId = record.getString(0);
	    	       // String routeId = record.getString(1);
	    	         
	    	        if(notBlank(record.getString(0))){
     		    		  spList.add(record.getString(0));
     		    		  tempSpList.add(record.getString(0));
     		    	  }else
     		    	  {
     		    		  spList.add("Missing");
     		    		  tempSpList.add("Missing");
     		    	  }
     		    	  if(notBlank(record.getString(1))){
     		    		 routeList.add(record.getString(1));
     		    	  }
     		    	  else{
     		    		  routeList.add("Missing");
     		    	  }	                  
	    	        record = delimitedFileReader.getNextRecord();
	    	  }
	    	  
	    	  for(int i=0;i<spList.size();i++){
	    		  String spId = spList.get(i);
	    		  String route= routeList.get(i);	
	    		  tempSpList.remove(spId);
		    	  BusinessObjectInstance factInstace = BusinessObjectInstance.create(ROUTE_IMPORT_FACT_BO);
		    	  factInstace.getFieldAndMD("bo").setXMLValue(ROUTE_IMPORT_FACT_BO);
		    	  COTSInstanceNode routeImportDataGroup = factInstace.getGroupFromPath(ROUTE_IMPORT_DATA);
		    	  routeImportDataGroup.getFieldAndMD(ROW_NUM).setXMLValue(String.valueOf(i));
		    	  routeImportDataGroup.getFieldAndMD(SP_ID).setXMLValue(spId);
		    	  routeImportDataGroup.getFieldAndMD(NEW_ROUTE).setXMLValue(route);
		    	  factInstace.getFieldAndMD(ACTIVITY_ID).setXMLValue(activityId);
		    	  if(tempSpList.contains(spId)){
		    		  routeImportDataGroup.getFieldAndMD(STATUS).setXMLValue("ERROR");
		    		  routeImportDataGroup.getFieldAndMD(ERROR_DESC).setXMLValue("Duplicate Service point Id exits in the file");
		    	  }
		    	  else{
		    		  routeImportDataGroup.getFieldAndMD(STATUS).setXMLValue("OK");
		    	  }
		    	  logger.info(factInstace.getDocument().asXML());
			      BusinessObjectDispatcher.fastAdd(factInstace.getDocument());
			      
	    	  }    
		    } 
		    catch (IOException e) 
		    { 
		      e.printStackTrace(); 
		    } 
	      return true;
		}  
	}	
}
