package com.splwg.cm.domain.measurement.measurement.batch;

import com.splwg.base.api.QueryIterator;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadIterationStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.batch.file.fixedPosition.MessageRepository;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.StringId;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.d1.api.lookup.DeviceIdentifierTypeLookup;
import com.splwg.d1.api.lookup.IntervalScalarLookup;
import com.splwg.d1.domain.admin.measuringComponentType.entities.MeasuringComponentTypeValueIdentifier;
import com.splwg.d1.domain.admin.measuringComponentType.entities.MeasuringComponentType_Id;
import com.splwg.d1.domain.deviceManagement.device.entities.DeviceIdentifier;
import com.splwg.d1.domain.deviceManagement.device.entities.DeviceIdentifier_Id;
import com.splwg.d1.domain.deviceManagement.measuringComponent.entities.MeasuringComponent;
import com.splwg.d1.domain.deviceManagement.measuringComponent.entities.MeasuringComponent_Id;
import com.splwg.d1.domain.measurement.initialMeasurementData.entities.InitialMeasurementData;
import com.splwg.d1.domain.measurement.measurement.entities.Measurement;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.dom4j.Element;

import com.ibm.icu.math.BigDecimal;

/**
 * @author Abjayon Consulting
 *
 @BatchJob (modules = {},
 *            softParameters = {@BatchJobSoftParameter (name = FILE_PATH, required = true, type = string)
 *            , @BatchJobSoftParameter (name = FILE_NAME, required = true, type = string)
 *            ,@BatchJobSoftParameter (name = maxErrors, type = integer)})
 */
public class DailyConsumptionExtractBatchProcess extends
		DailyConsumptionExtractBatchProcess_Gen {
		
	
	    private static final String DEDAS_DEPSAS_MASTER_CONFIG_BO = "CM-DedasDepsasMasterConfig";
		private static String intervalFileName;
		private static String filePath;
		private static DateTime startDatetime;
		private static DateTime endDatetime;
		private static String scalarFileName;
		
		private static File newFile = null;
		private static File scalarNewFile = null;
		private static FileWriter writer = null;
		private static FileWriter scalarWriter = null;
		private static String delimiter = ",";
		
		private static DateTime measurementDateTime = null;
		private static String dataSource;
		private static String deviceIdentifier = null;
		private static BigDecimal intervalDuration = null;
		private static String externalUOM = null;
		private static String readingVal = null;
		private static String measurementVal1 = null;
		private static String measurementVal2 = null;
		private static String measurementDateTimeString = null;
		

	public Class<DailyConsumptionExtractBatchProcessWorker> getThreadWorkerClass() {
		return DailyConsumptionExtractBatchProcessWorker.class;
	}

	public static class DailyConsumptionExtractBatchProcessWorker extends
			DailyConsumptionExtractBatchProcessWorker_Gen {
		
		public static Logger logger = LoggerFactory.getLogger(DailyConsumptionExtractBatchProcess.class);

		public ThreadExecutionStrategy createExecutionStrategy() {
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
			File file = null;
			filePath = getParameters().getFILE_PATH();
			intervalFileName = getParameters().getFILE_NAME()+"-Interval"+(getProcessDateTime())+".csv";
			scalarFileName = getParameters().getFILE_NAME()+"-Scalar"+(getProcessDateTime())+".csv";
			
			//Validating Soft Parameters
			if(!filePath.endsWith("/")){
				filePath = filePath+'/';}
			    
			try{
					file =new File(filePath);
					
					//Validating File Path
					if(file.isAbsolute() || file.isDirectory()){						
						newFile =new File(filePath+intervalFileName);
						scalarNewFile = new File(filePath+scalarFileName);
						//Validating File Name
						newFile.createNewFile();
					} 
					
			}
			catch(IOException e){
				if(!file.isAbsolute() || !file.isDirectory()){
					addError(MessageRepository.invalidFileName(filePath));}
				else if(!newFile.isFile()){
					addError(MessageRepository.invalidFileName(intervalFileName));}
				else if(!scalarNewFile.isFile()){
					addError(MessageRepository.invalidFileName(scalarFileName));}
			}
			
			try {
				writer = new FileWriter(newFile, true);
				scalarWriter = new FileWriter(scalarNewFile,true);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			startResultRowQueryIteratorForThread(MeasuringComponent_Id.class);
			
		}
			
		/**
	     * Create an iterator for a simple query that selects a single business entity.
	 	 * Use lowId and highId to confine the selection to the current thread.  These two
	 	 * arguments are calculated and supplied by the framework.
	 	 */
	     @Override
	     protected QueryIterator getQueryIteratorForThread(StringId lowId, StringId highId) {
	    	 
	    	 StringBuilder queryString = new StringBuilder();
				queryString.append("SELECT DISTINCT MC.MEASR_COMP_ID FROM ");
				queryString.append("CI_ACCT_CHAR ACTCHAR,D1_US_IDENTIFIER USI,D1_US_SP USSP, ");
				queryString.append("D1_INSTALL_EVT IE,D1_MEASR_COMP MC ");
				queryString.append("WHERE ACTCHAR.CHAR_TYPE_CD = 'CM-CUSTY' ");
				queryString.append("AND (ACTCHAR.CHAR_VAL = 'K1' ");
				queryString.append("OR ACTCHAR.CHAR_VAL = 'K2') ");
				queryString.append("and ACTCHAR.EFFDT = (select max(actChar.EFFDT) ");
				queryString.append("from CI_ACCT_CHAR acctChar ");
				queryString.append("where acctChar.ACCT_ID = ACTCHAR.ACCT_ID ");
				queryString.append("and acctChar.CHAR_TYPE_CD = 'CM-CUSTY' ");
				queryString.append("AND acctChar.CHAR_VAL in ('K1','K2') ");
				queryString.append("and acctChar.EFFDT <= SYSDATE) ");
				queryString.append("AND ACTCHAR.ACCT_ID = USI.ID_VALUE ");
				queryString.append("AND USI.US_ID = USSP.US_ID ");
				queryString.append("AND USI.US_ID_TYPE_FLG = 'D2EA' ");
				queryString.append("AND IE.D1_SP_ID = USSP.D1_SP_ID ");
				queryString.append("AND MC.DEVICE_CONFIG_ID = IE.DEVICE_CONFIG_ID ");
				queryString.append("AND MC.MEASR_COMP_ID BETWEEN :lowId AND :highId");
			
				PreparedStatement statement = createPreparedStatement(queryString.toString(), "Retrieve Measuring Component ID's");
				statement.bindId("lowId", lowId);
				statement.bindId("highId", highId);
				
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
	    	 ThreadWorkUnit unit = new ThreadWorkUnit(row.getId("MEASR_COMP_ID", MeasuringComponent.class));
	    	 return unit;
	     }
	

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
	    	
	    	MeasuringComponent_Id mcId = (MeasuringComponent_Id) unit.getPrimaryId();
	    	insertingDataIntoFile(mcId);
	    	return true;
		}
		
		public void insertingDataIntoFile(MeasuringComponent_Id mcId) {
			
			Boolean validMcType = false;
			
			logger.info("Measuring Component ID"+mcId.getEntity().getId());
			MeasuringComponent_Id measuringComponentId = new MeasuringComponent_Id(mcId.getIdValue());
			String mcType = measuringComponentId.getEntity().getMeasuringComponentType().getId().getIdValue();
		
			BusinessObjectInstance masterConfgiBOInstance = BusinessObjectInstance
					.create(DEDAS_DEPSAS_MASTER_CONFIG_BO);
			masterConfgiBOInstance.set("bo",
					DEDAS_DEPSAS_MASTER_CONFIG_BO);
			masterConfgiBOInstance = BusinessObjectDispatcher.read(
					masterConfgiBOInstance, true);
			if (isNull(masterConfgiBOInstance)) {
				return;
			}
			Element validMeasuringComponentTypesGroup = masterConfgiBOInstance
					.getElement().element("validMeasuringComponentTypes");
			if (isNull(validMeasuringComponentTypesGroup)) {
				return;
			}
			
			List<Element> validMeasuringComponentTypesList = validMeasuringComponentTypesGroup
					.elements();
			if (isNull(validMeasuringComponentTypesList)
					|| validMeasuringComponentTypesList.isEmpty()) {
				return;
			}
			
			Iterator<Element> measuringComponentTypeIterator = validMeasuringComponentTypesList
					.iterator();
			while (measuringComponentTypeIterator.hasNext()) {
				String masterConfigMcType = measuringComponentTypeIterator.next().element("measuringComponentType").getData().toString();
				if(mcType.compareTo(masterConfigMcType) == 0){
					validMcType = true;
					break;
				}
			}
			if(validMcType){
				BusinessObjectInstance boInstance = BusinessObjectInstance.create(measuringComponentId.getEntity().getBusinessObject().getId().getIdValue());
	        	boInstance.set("measuringComponentId",mcId.getEntity());
	        	boInstance = BusinessObjectDispatcher.read(boInstance);
	        	DateTime consumptionExtractDateTime = boInstance.getDateTime("consumptionExtractDateTime");
	        	
	        	if(isNull(consumptionExtractDateTime)){
	        		consumptionExtractDateTime = boInstance.getDateTime("creationDateTime");
	      
	        	}
				//Retrieve Start and End Date Time
	        	startDatetime = consumptionExtractDateTime;
			    endDatetime = getProcessDateTime();
			  		  		    
			    if(startDatetime.compareTo(endDatetime) < 0)
				{
			   
				//Retrieve Device Identifier
			    DeviceIdentifier_Id deviceIdBadgeNumber = new DeviceIdentifier_Id(mcId.getEntity().getDeviceConfigurationId().getEntity().getDevice(), DeviceIdentifierTypeLookup.constants.BADGE_NUMBER);
			    if(notNull(deviceIdBadgeNumber)){
			    	deviceIdentifier = deviceIdBadgeNumber.getEntity().getIdValue();
			    	logger.info("Device Identifier"+deviceIdentifier);
			    }
			 
				//Retrive Interval Duration
	
				if(mcId.getEntity().getMeasuringComponentType().getIntervalScalar().value().contentEquals(IntervalScalarLookup.constants.SCALAR.trimmedValue()))
				{
					intervalDuration = BigDecimal.ZERO;
				}else{
				intervalDuration = new BigDecimal(mcId.getEntity().getMeasuringComponentType().getSecondsPerInterval().toSecondsString());
				}
			
				//Retrive External UOM
				MeasuringComponentTypeValueIdentifier mcValIdentifier = mcId.getEntity().getMeasuringComponentType().determinePrimaryMeasurementValueIdentifier();
				externalUOM = mcValIdentifier.fetchUnitOfMeasure().getId().getIdValue();
				if(notNull(mcValIdentifier.fetchTimeOfUse())){
					String tou = mcValIdentifier.fetchTimeOfUse().getId().getIdValue();
					externalUOM = externalUOM + "-" + tou;
				}
				if(notNull(mcValIdentifier.fetchServiceQuantityIdentifier())){
					String sqi = mcValIdentifier.fetchServiceQuantityIdentifier().getId().getIdValue();
					externalUOM = externalUOM + "-" + sqi;
				}
				try {
					List<InitialMeasurementData> imdList = mcId.getEntity().getInitialMeasurementDataForPeriod(startDatetime, endDatetime);
					    if(notNull(imdList))
					    {
					    	if(mcId.getEntity().getMeasuringComponentType().getIntervalScalar().value().contentEquals(IntervalScalarLookup.constants.SCALAR.trimmedValue()))
					    	{
				            	Iterator<InitialMeasurementData> imdIterator = imdList.iterator();
				                
				            	while(imdIterator.hasNext())
				            	{
				            		InitialMeasurementData imd = imdIterator.next();
				            		dataSource = imd.getDataSource().toString();
		
				            		if(notNull(imd.retrieveMeasurements(Boolean.TRUE)))
				            		{
					            		Iterator<Measurement> measurementIterator = imd.retrieveMeasurements(Boolean.TRUE).iterator();
					            		while(measurementIterator.hasNext())
					            		{
					            			Measurement measurement = measurementIterator.next();
						            		readingVal = measurement.getReadingValue().toString()+":"+measurement.getMeasurementCondition();
						            		measurementDateTime = measurement.getMeasurementLocalDateTime();
						            		measurementDateTimeString = retriveDateTimeString(measurementDateTime);
						            		if(notNull(measurement.getMeasurementValue1())){
						            			measurementVal1 = measurement.getMeasurementValue1().toString();
						            		}
						            		if(notNull(measurement.getMeasurementValue2())){
						            			measurementVal2 = measurement.getMeasurementValue2().toString();
						            		}
						            		appendValuesInFile(scalarWriter);
					            		}
				            		}
				            		
				            	}
						    
						     }
					    	else {
					    		
					    		Iterator<InitialMeasurementData> imdIterator = mcId.getEntity().getInitialMeasurementDataForPeriod(startDatetime, endDatetime).iterator();
				                
				            	while(imdIterator.hasNext())
				            	{
				            		InitialMeasurementData imd = imdIterator.next();
				            		dataSource = imd.getDataSource().toString();
		
				            		if(notNull(imd.retrieveMeasurements(Boolean.TRUE)))
				            		{
					            		Iterator<Measurement> measurementIterator = imd.retrieveMeasurements(Boolean.TRUE).iterator();
					            		while(measurementIterator.hasNext())
					            		{
					            			Measurement measurement = measurementIterator.next();
						            		readingVal = measurement.getReadingValue().toString()+":"+measurement.getMeasurementCondition();
						            		measurementDateTime = measurement.getMeasurementLocalDateTime();
						            		measurementDateTimeString = retriveDateTimeString(measurementDateTime);
						            		if(notNull(measurement.getMeasurementValue1())){
						            			measurementVal1 = measurement.getMeasurementValue1().toString();
						            		}
						            		if(notNull(measurement.getMeasurementValue2())){
						            			measurementVal2 = measurement.getMeasurementValue2().toString();
						            		}
						            		appendValuesInFile(writer);
					            		}
				            		}
				            		
				            	}
					    		
					    	}
					    	
					    }
					    
				    boInstance.set("consumptionExtractDateTime",endDatetime);
					boInstance = BusinessObjectDispatcher.update(boInstance);
			        writer.flush();
			        scalarWriter.flush();
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
				}
			}
		}
		
		 /**
	     * @ Method for appending data to the file.
	     * 
	     * Input
		 * - FileWriter
	     */
	     private void appendValuesInFile(FileWriter writer){
	           try{
	        	   writer.append(measurementDateTimeString);
	        	   writer.append(delimiter+deviceIdentifier);
	        	   writer.append(delimiter+intervalDuration.toString());
	        	   writer.append(delimiter+dataSource);
	        	   writer.append(delimiter+externalUOM);
	        	   writer.append(delimiter+readingVal);
				   writer.append(delimiter+measurementVal1);
				   writer.append(delimiter+measurementVal2);
				   writer.append("\n");
	           }
	           catch(IOException ioExcep){
	                 ioExcep.printStackTrace();
	           }
	     }
	     
	     public String retriveDateTimeString(DateTime dateTime) {
		 		// TODO Auto-generated method stub
		    	 
		    	String dateTimeString = dateTime.toString().replace(".", ":");
		    	if(notNull(dateTimeString)){
					String dateString = dateTimeString.substring(0, 10);
					String timeString = dateTimeString.substring(11, 19);
					dateTimeString = dateString + "T" + timeString;
		    	}
				return dateTimeString;
		 	}
	     
	     @Override
	    public void finalizeThreadWork() throws ThreadAbortedException,
	    		RunAbortedException {
	    	try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	super.finalizeThreadWork();
	    }

	}

		
}
