package com.splwg.cm.domain.installation.servicePointMeasurementCycleScheduleRoute.batch;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import com.splwg.base.api.QueryIterator;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadIterationStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.businessObject.COTSInstanceNode;
import com.splwg.base.api.datatypes.StringId;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.fact.fact.Fact;
import com.splwg.base.domain.fact.fact.Fact_Id;
import com.splwg.d1.domain.admin.measurementCycle.entities.MeasurementCycleRoute;
import com.splwg.d1.domain.admin.measurementCycle.entities.MeasurementCycleRouteCharacteristic;
import com.splwg.d1.domain.admin.measurementCycle.entities.MeasurementCycleRoute_Id;
import com.splwg.d1.domain.admin.measurementCycle.entities.MeasurementCycle_Id;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author
 *
 @BatchJob (modules = {},
 *            softParameters = {@BatchJobSoftParameter (name = fileName, required = true,type = string)
 *            ,@BatchJobSoftParameter (name = filePath, required = true,type = string)
 *            ,@BatchJobSoftParameter (name = activityId, required = true,type = string)
 *            ,@BatchJobSoftParameter (name = maxErrors, type = integer)})
 */

public class RouteImportValidationProcess extends
RouteImportValidationProcess_Gen {
	public static Logger logger = LoggerFactory.getLogger(RouteImportValidationProcess.class);
	public static final String ROUTE_IMPORT_FACT_BO = "CM-RouteImportFact";
	public static final String ROUTE_IMPORT_DATA = "routeImportData";
	public static final String SP_ID = "servicePoint";
	public static final String OLD_ROUTE = "oldRoute";
	public static final String NEW_ROUTE = "newRoute";
	public static final String ERROR_DESC = "errorDesc";
	public static final String BO_STATUS = "boStatus";
 	public static final String STATUS = "status";
	public static final String ROW_NUM = "rowNo";
	public static final String ACTIVITY_ID = "activityId";
	public static final String MDM_SERVICEPOINT = "mdmServicePoint";
	public static MeasurementCycle_Id measurementCycleId = new MeasurementCycle_Id("ONSP");
	public Class<RouteImportValidationProcessWorker> getThreadWorkerClass() {
		return RouteImportValidationProcessWorker.class;
		
	}

	public static class RouteImportValidationProcessWorker extends
	RouteImportValidationProcessWorker_Gen {
		
		public BusinessObjectInstance factInstance = null;
		private COTSInstanceNode routeImportDataGroup  = null;
		private File masterKarneFile = null;
		private static FileWriter writer = null;
		private String filePath = null;
		private String fileName = null;
		private String activityId = null;
		private String separator = ";" ;
		private int rowNum =0;
		private String status = "";
		private String errorDescription = "";
		private String sp = "";
		private String newRoute = "";
		private String oldRoute = "";
		private String mdmServicePoint = "";
		
		public ThreadExecutionStrategy createExecutionStrategy() {
			logger.info("ThreadExecutionStrategy");
			// TODO Auto-generated method stub
			return new ThreadIterationStrategy(this);
		}
		
		/**
	 	 * Initiate an *entity* query iterator for the business entities that are to be selected in
	 	 * getQueryIteratorForThread(StringId, StringId) below.
	 	 */
		
		public void initializeThreadWork(boolean initializationPreviouslySuccessful) throws ThreadAbortedException,
	 	RunAbortedException {
			JobParameters parameters = getParameters();
	    	 filePath =  parameters.getFilePath();
	    	 fileName = parameters.getFileName();
	    	 activityId = parameters.getActivityId();
	    	 //factCreation();
	    	 createMasterkarneFile();
	    	 startResultRowQueryIteratorForThread(Fact_Id.class);
	     }
		
		/**
	     * Create an iterator for a simple query that selects a single business entity.
	 	 * Use lowId and highId to confine the selection to the current thread.  These two
	 	 * arguments are calculated and supplied by the framework.
	 	 */
		
		@Override
	     protected QueryIterator getQueryIteratorForThread(StringId lowId, StringId highId) {
			logger.info("QueryIterator");
			 
			StringBuilder queryString = new StringBuilder();
			queryString.append("select fact_id from F1_fact_Char where Char_type_cd='CM-RTACT' and adhoc_char_val = '"+activityId+"' ");
			queryString.append("AND fact_id BETWEEN :lowId AND :highId");
			PreparedStatement statement = createPreparedStatement(queryString.toString(), "Retrieve SP ID's");
			statement.bindId("lowId", lowId);
			statement.bindId("highId", highId);   
			//logger.info(statement.list());
			return statement.iterate();
		 
		 }
		
		/*
	      * Create and return a ThreadWorkUnit from the QueryResultRow instance that was
	      * selected in getQueryForThreadIterator(StringId, StringId) above.  This method must be implemented if
	      * startResultRowQueryIteratorForThread(StringId) was used to initiate the query iterator.
	      * This method's job is to create a ThreadWorkUnit, which will be passed to
	      * executeWorkUnit(ThreadWorkUnit) below.
	      * The returned ThreadWorkUnit must contain a primaryId.
        */
		
		@Override
	     public ThreadWorkUnit getNextWorkUnit(QueryResultRow row) {
			logger.info("getNextWorkUnit");
	    	 ThreadWorkUnit unit = new ThreadWorkUnit(row.getId("FACT_ID", Fact.class));	 
	    	 return unit;
	     }

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			// TODO Auto-generated method stub
			Fact_Id factid = (Fact_Id) unit.getPrimaryId();
			logger.info("Inside Execute Work Unit"+factid);
			validateFact(factid);
			return true;
		}  
		public void validateFact(Fact_Id factId){
			newRoute = "";
			oldRoute = "";
			sp = "";
			rowNum = rowNum+1;
			errorDescription = "";
			mdmServicePoint = "";
			String spArea = null;
			String routeArea = null;
			logger.info("fact ID "+factId.getIdValue());
			if(notNull(factId.getEntity())){
				factInstance = BusinessObjectInstance.create(ROUTE_IMPORT_FACT_BO);
				factInstance.set("factId", factId.getIdValue());
				factInstance.set("bo",ROUTE_IMPORT_FACT_BO);
		    	factInstance = BusinessObjectDispatcher.read(factInstance);
		    	factInstance.getFieldAndMD(BO_STATUS).setXMLValue("VALIDATED");
				routeImportDataGroup = factInstance.getGroupFromPath(ROUTE_IMPORT_DATA);
				sp = routeImportDataGroup.getElement().element(SP_ID).getText();
				newRoute = routeImportDataGroup.getElement().element(NEW_ROUTE).getText();
				status =  routeImportDataGroup.getElement().element(STATUS).getText();
				logger.info("spId "+sp+" routeId "+newRoute+" status "+status);
		    	if(status.equals("OK")){
		    		spArea = retriveSpArea(sp);
		    		routeArea = retriveRouteArea(newRoute);
		    		logger.info(spArea+"  "+routeArea);
		    		if(spArea.equals("spDoesNotExits") && routeArea.equals("routeDoesNotExits")){
		    			status="ERROR";
		    			errorDescription = "ServicePoint and Route Does not exits";
		    			routeImportDataGroup.getFieldAndMD(STATUS).setXMLValue(status);
		    			routeImportDataGroup.getFieldAndMD(ERROR_DESC).setXMLValue(errorDescription);
		    			factInstance.getFieldAndMD(BO_STATUS).setXMLValue("ERROR");
		    		}
		    		else if(spArea.equals("spDoesNotExits")){
		    			status="ERROR";
		    			errorDescription = "ServicePoint Does not exits";
		    			routeImportDataGroup.getFieldAndMD(STATUS).setXMLValue("ERROR");
		    			routeImportDataGroup.getFieldAndMD(ERROR_DESC).setXMLValue(errorDescription);	
		    			factInstance.getFieldAndMD(BO_STATUS).setXMLValue("ERROR");
		    		}
		    		else if(routeArea.equals("routeDoesNotExits")){
		    			status="ERROR";
		    			errorDescription = "Route Does not exits";
		    			routeImportDataGroup.getFieldAndMD(STATUS).setXMLValue("ERROR");
		    			routeImportDataGroup.getFieldAndMD(ERROR_DESC).setXMLValue(errorDescription);
		    			factInstance.getFieldAndMD(BO_STATUS).setXMLValue("ERROR");
		    		}
		    		else if(!spArea.trim().equalsIgnoreCase(routeArea.trim())){
		    			logger.info(spArea);
		    			logger.info(routeArea);
		    			status="ERROR";
		    			errorDescription = "ServicePoint area does not match with Route area";
	    				routeImportDataGroup.getFieldAndMD(STATUS).setXMLValue("ERROR");
		    			routeImportDataGroup.getFieldAndMD(ERROR_DESC).setXMLValue(errorDescription);
		    			factInstance.getFieldAndMD(BO_STATUS).setXMLValue("ERROR");
		    		}
		    		
		    	}
		    	else{
		    		factInstance.getFieldAndMD(BO_STATUS).setXMLValue("ERROR");
		    		errorDescription = routeImportDataGroup.getElement().element(ERROR_DESC).getText();
		    	}
		    	if(notNull(mdmServicePoint)){
		    		routeImportDataGroup.getFieldAndMD(MDM_SERVICEPOINT).setXMLValue(mdmServicePoint);
		    	}
		    	factInstance = BusinessObjectDispatcher.update(factInstance);
		    	logger.info(factInstance.getDocument().asXML());
		    	appendValuesInFile();
		 	}	    
		} 
		private String retriveSpArea(String spId){
			String spArea = null;
			StringBuffer query = new StringBuffer();
			query.append("select sp.D1_SP_ID,sp.MSRMT_CYC_RTE_CD,sp.STATE from d1_sp_identifier spIdentifier, d1_sp sp"
					+ " where spIdentifier.id_value = '"+spId+"'"
					+ " and spIdentifier.sp_id_type_flg = 'D1EI' "
					+ "and spIdentifier.d1_sp_id = sp.d1_sp_id");
    		PreparedStatement statement = createPreparedStatement(query.toString(),"");
    		SQLResultRow result = statement.firstRow();
    		if(notNull(result)){
    			mdmServicePoint = result.getString("D1_SP_ID");
    			oldRoute = result.getString("MSRMT_CYC_RTE_CD");
    			spArea = result.getString("STATE");
    			routeImportDataGroup.getFieldAndMD(MDM_SERVICEPOINT).setXMLValue(mdmServicePoint);
    			if(notNull(oldRoute)){
    				 routeImportDataGroup.getFieldAndMD(OLD_ROUTE).setXMLValue(oldRoute);
    			}
        	}
    		else{
    			return "spDoesNotExits";
    		}	
        	return spArea;   		
		}
		private String retriveRouteArea(String routeId){
			
			logger.info(sp);
			String area = null;
			MeasurementCycleRoute mcRoute = new MeasurementCycleRoute_Id(measurementCycleId,routeId).getEntity();
    		if(notNull(mcRoute)){
    			 MeasurementCycleRouteCharacteristic measrCycleRouteCharList =  (MeasurementCycleRouteCharacteristic) mcRoute.getMeasurementCycleRouteCharacteristics()
    					 .createFilter("where msrmt_cyc_rte_cd= '"+routeId+"' and char_type_cd= 'CM-RAREA' and MSRMT_CYC_cd='"+measurementCycleId.getIdValue().trim()+"'","").firstRow(); 
    			 area = measrCycleRouteCharList.getAdhocCharacteristicValue();
    		}
    		else{
    			return "routeDoesNotExits";
    		}
    		return area;
		}
		public void createMasterkarneFile(){
			if(!filePath.endsWith("\\")){
				filePath = filePath+'\\';
			}	
	        masterKarneFile =new File(filePath+activityId+"_"+fileName);  
	        try {
	        	//Set Headers for the File
				writer = new FileWriter(masterKarneFile, true);
				writer.append("SATIR NO"+separator+"DURUMU"+separator+"HATA ACIKLAMA"+separator+"TESiSAT NO"+separator+"KARNE NO"+separator+"ESKi KARNE"); 
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	  
		}
		 public void appendValuesInFile(){
			try{
				writer.append("\n");
				writer.append(rowNum+separator);
				writer.append(status+separator);
				writer.append(errorDescription+separator);
				writer.append(sp+separator);
				writer.append(newRoute+separator);
				writer.append(oldRoute+separator);
				writer.flush();
			}
			catch(IOException ioExcep){
				ioExcep.printStackTrace();
			}
		}
		@Override
	    public void finalizeThreadWork() throws ThreadAbortedException,
	    		RunAbortedException {
	    	try {
				//writer.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	super.finalizeThreadWork();
	    } 
 
		public void factCreation(){
			if(!filePath.endsWith("\\")){
				filePath = filePath+'\\';
			}	
			
		  List<String> spRouteList = new ArrayList<String>(); 
	      List<String> spList = new ArrayList<String>();
	      List<String> tempSpList = new ArrayList<String>();
	      List<String> routeList = new ArrayList<String>();	
	      File karneFile = new File(filePath + fileName);
	      try{    
		      spRouteList = Files.readAllLines(karneFile.toPath()); 
		      for(String spRouteData : spRouteList){
		    	  String[] data = spRouteData.split(";");
		    	  for(String s : data){
		    		  logger.info(s);  
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
		}

	}	
}
