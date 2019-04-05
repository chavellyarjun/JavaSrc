package com.splwg.cm.domain.installation.servicePointMeasurementCycleScheduleRoute.batch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import com.splwg.base.api.batch.CommitEveryUnitStrategy;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.batch.file.MessageRepository;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.businessObject.COTSInstanceNode;
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

public class RouteImportFactCreationProcess extends RouteImportFactCreationProcess_Gen {

	
                public static Logger logger = LoggerFactory.getLogger(RouteImportFactCreationProcess.class);
                //constants
            	public static final String ROUTE_IMPORT_FACT_BO = "CM-RouteImportFact";
            	public static final String ROUTE_IMPORT_DATA = "routeImportData";
            	public static final String SP_ID = "servicePoint";
            	public static final String OLD_ROUTE = "oldRoute";
            	public static final String NEW_ROUTE = "newRoute";
            	public static final String ERROR_DESC = "errorDesc";
            	public static final String STATUS = "status";
            	public static final String ROW_NUM = "rowNo";
            	public static final String ACTIVITY_ID = "activityId";
            	public static MeasurementCycle_Id measurementCycleId = new MeasurementCycle_Id("ONSP");
            	
            	public void validateSoftParameters() {
            		logger.info("inside validation");	
            		File file = null;
            		File readFile = null;
            		//Initializing soft parameters
            		String filePath =  getParameters().getFilePath();
               	    String fileName = getParameters().getFileName();
            		
            		//Validating Soft Parameters
            		if(!filePath.endsWith("\\")){
            			filePath = filePath+'\\';}
            		file =new File(filePath);
            		//Validating File Path
            		if(file.isAbsolute() || file.isDirectory()){
            			readFile =new File(filePath+fileName);
            			//Validating File Name
            			if(!readFile.exists()){
            				addError(MessageRepository.invalidFileName(fileName));
            			}
            		}
            		else{
            			addError(MessageRepository.invalidFileName(filePath));
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
                public Class<RouteImportFactCreationProcessWorker> getThreadWorkerClass() {
                                return RouteImportFactCreationProcessWorker.class;
                }
                
                public static class RouteImportFactCreationProcessWorker extends RouteImportFactCreationProcessWorker_Gen { 
                	
                	public void initializeThreadWork(boolean initializationPreviouslySuccessful) throws ThreadAbortedException, RunAbortedException
            		{
                		logger.info("hello");
            		}
                	
                	public ThreadExecutionStrategy createExecutionStrategy() 
                    {
                		return new CommitEveryUnitStrategy(this);
                    }

                    public boolean executeWorkUnit(ThreadWorkUnit unit) throws ThreadAbortedException, RunAbortedException 
                    {
                    	logger.info("inside executeWorkUnit");
                    	String filePath = unit.getSupplementallData("filePath").toString();
                        String fileName = unit.getSupplementallData("fileName").toString();
                        String activityId = unit.getSupplementallData("activityId").toString();
                    	List<String> spList = new ArrayList<String>();
              	        List<String> tempSpList = new ArrayList<String>();
              	        List<String> routeList = new ArrayList<String>();

                        try {
                            File karneFile= new File(filePath +"/"+ fileName);
                            logger.info("karneFile" + karneFile.getPath());
                            BufferedReader bufferedReader = null;
                            try{
                                bufferedReader = new BufferedReader(new FileReader(karneFile));
                                String fileLine;
                                logger.info("buffer reader");
                                while((fileLine = bufferedReader.readLine()) != null) {
                                    if(notBlank(fileLine)) {
                                    	 String[] data = fileLine.split(";");
                       		    	  for(String s : data){
                       		    		  logger.info("delimiter value "+s);  
                       		    	  }
                       		    	  if(notBlank(data[0])){
                       		    		  spList.add(data[0]);
                       		    		  tempSpList.add(data[0]);
                       		    	  }else
                       		    	  {
                       		    		  spList.add("Missing");
                       		    		  tempSpList.add("Missing");
                       		    	  }
                       		    	  if(notBlank(data[1])){
                       		    		 routeList.add(data[1]);
                       		    	  }
                       		    	  else{
                       		    		  routeList.add("Missing");
                       		    	  }	
                                    }
                                }
                                for(int i=0;i<spList.size();i++){
                                	logger.info("inside for loop");
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
                            catch (Exception ex){
                                            logger.info("Throwing an error while reading a file " + fileName + " in CMNDCreateUsageTransactionBatchProcess ");
                            }
                            finally {
                                 if(notNull(bufferedReader))  {
                                	 bufferedReader.close();
                                 } 	 
                            }
                            
                        } catch(Exception e) {
                               e.printStackTrace();
                        }
                        
                        return true;
                    }
                }

}
