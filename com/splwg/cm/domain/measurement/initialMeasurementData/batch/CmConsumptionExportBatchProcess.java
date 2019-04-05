package com.splwg.cm.domain.measurement.initialMeasurementData.batch;

import com.ibm.icu.math.BigDecimal;
import com.ibm.icu.util.Measure;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.dom4j.Element;

import com.splwg.base.api.QueryIterator;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadIterationStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.businessObject.COTSFieldDataAndMD;
import com.splwg.base.api.businessObject.COTSInstanceList;
import com.splwg.base.api.businessObject.COTSInstanceListNode;
import com.splwg.base.api.businessObject.COTSInstanceNode;
import com.splwg.base.api.businessService.BusinessServiceDispatcher;
import com.splwg.base.api.businessService.BusinessServiceInstance;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.DateFormat;
import com.splwg.base.api.datatypes.DateFormatParseException;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.StringId;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.businessService.BusinessService;
import com.splwg.ccb.domain.customerinfo.premise.entity.Premise;
import com.splwg.ccb.domain.customerinfo.premise.entity.PremiseCharacteristic;
import com.splwg.ccb.domain.customerinfo.premise.entity.Premise_Id;
import com.splwg.cm.api.lookup.ConsumptionTypeFlagLookup;
import com.splwg.d1.api.lookup.DeviceIdentifierTypeLookup;
//import com.splwg.cm.api.lookup.PowerTypeFlagLookup;
import com.splwg.d1.api.lookup.IntervalScalarLookup;
import com.splwg.d1.api.lookup.ServicePointIdentifierTypeLookup;
import com.splwg.d1.api.lookup.ValueIdentifierTypeLookup;
import com.splwg.d1.domain.admin.measuringComponentType.entities.MeasuringComponentTypeValueIdentifier;
import com.splwg.d1.domain.admin.measuringComponentType.entities.MeasuringComponentTypeValueIdentifier_Id;
import com.splwg.d1.domain.admin.unitOfMeasure.entities.UnitOfMeasureD1;
import com.splwg.d1.domain.deviceManagement.device.entities.DeviceIdentifier;
import com.splwg.d1.domain.deviceManagement.device.entities.DeviceIdentifier_Id;
import com.splwg.d1.domain.deviceManagement.deviceConfiguration.entities.DeviceConfiguration;
import com.splwg.d1.domain.deviceManagement.measuringComponent.entities.MeasuringComponent;
import com.splwg.d1.domain.installation.installEvent.entities.InstallEvent;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointD1;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointD1_Id;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointIdentifier_Id;
import com.splwg.d1.domain.measurement.initialMeasurementData.entities.InitialMeasurementData;
import com.splwg.d1.domain.measurement.measurement.entities.Measurement;
import com.splwg.d1.domain.measurement.measurement.entities.Measurement_Id;
import com.splwg.d1.domain.measurement.measurementServices.axisConversion.AxisConversionOutputData_Impl;
import com.splwg.d1.domain.measurement.measurementServices.axisConversion.AxisConversionService;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author
 *
 @BatchJob (modules = {},
 *            softParameters = {@BatchJobSoftParameter (name = requestType, required = true,type = string)
 *            ,@BatchJobSoftParameter (name = consumptionImportHorizonDays, type = integer)
 *            ,@BatchJobSoftParameter (name = maxErrors, type = integer)})
 */

public class CmConsumptionExportBatchProcess extends
		CmConsumptionExportBatchProcess_Gen {
	public static Logger logger = LoggerFactory.getLogger(CmConsumptionExportBatchProcess.class);    
	
	public Class<CmConsumptionExportBatchProcessWorker> getThreadWorkerClass() {
		return CmConsumptionExportBatchProcessWorker.class;
		
	}

	public static class CmConsumptionExportBatchProcessWorker extends
			CmConsumptionExportBatchProcessWorker_Gen {
		
		private String meteringPointEIC = null;
		private String requestType = null;
		private String customerType = null;
		private BigInteger consumptionHorizonDays = null;   
		private Date periodEndDate = null;
		private DateTime periodEndDttm = null;
		private DateTime periodStartDttm = null;
		private DateTime lastMeasureDate = null;
		private DateTime tempStartDateTime = null;
		private DateTime firstMeasureDate = null;
		private DateTime consumptionExportDttm =  null;
		private List<MeasuringComponent> scalarMcList = null;
		private DateFormat dataFormat = new DateFormat("yyyy-MM-dd-HH:mm:ss");
		private Boolean eligibility = false;
	    private String tou = null;
	    private String meteringPointID = null;
	    
		//Index Add Details
	    private String deviceId = null;
	    private BigDecimal factor = null;
	    private BigInteger digitCount = null;
	    private String firstLoadType = null;
	    private String firstMeasureType = null;
	    private String lastLoadType = null;
	    private String lastMeasureType = null;
	    private String meterManufacturer = null;
	    private String meterSerialNumber = null;
	    private String meterType = null;
	    private BigDecimal firstT0 = BigDecimal.ZERO;
	    private BigDecimal firstT1 = BigDecimal.ZERO;
	    private BigDecimal firstT2 = BigDecimal.ZERO;
	    private BigDecimal firstT3 = BigDecimal.ZERO;
	    private BigDecimal lastT0 = BigDecimal.ZERO;
	    private BigDecimal lastT1 = BigDecimal.ZERO;
	    private BigDecimal lastT2 = BigDecimal.ZERO;
	    private BigDecimal lastT3 = BigDecimal.ZERO;
	    private BigDecimal dayTimeConsumption = BigDecimal.ZERO;
	    private BigDecimal nightTimeConsumption = BigDecimal.ZERO;
	    private BigDecimal peakConsumption = BigDecimal.ZERO;
	    private String powerTypeValue = null;
	    private List<MeasuringComponent> activeMcList =  new ArrayList<MeasuringComponent>();
	    private List<MeasuringComponent> inductiveMcList =  new ArrayList<MeasuringComponent>();
	    private List<MeasuringComponent> capacitiveMcList =  new ArrayList<MeasuringComponent>();
	    private List<MeasuringComponent> demandMcList =  new ArrayList<MeasuringComponent>();
	    
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
	    	 if (notNull(parameters.getRequestType())) {
	    		 requestType =  parameters.getRequestType();
	            }
	    	 if(notNull(parameters.getConsumptionImportHorizonDays()))
	    	 {
	    		  consumptionHorizonDays = parameters.getConsumptionImportHorizonDays();
	    	 }
	    	 startResultRowQueryIteratorForThread(ServicePointD1_Id.class);
	     }
		
		/**
	     * Create an iterator for a simple query that selects a single business entity.
	 	 * Use lowId and highId to confine the selection to the current thread.  These two
	 	 * arguments are calculated and supplied by the framework.
	 	 */
		
		@Override
	     protected QueryIterator getQueryIteratorForThread(StringId lowId, StringId highId) {
			
			 
			StringBuilder queryString = new StringBuilder();
			queryString.append("SELECT DISTINCT SPCHAR.D1_SP_ID, ACCTCHAR.CHAR_VAL AS CUSTOMER_TYPE,SPCHAR.ADHOC_CHAR_VAL AS EIC_CODE ");
			queryString.append("FROM CI_ACCT_CHAR ACCTCHAR,D1_US_IDENTIFIER USI, D1_US_SP USSP,D1_SP_IDENTIFIER SPI,D1_SP_CHAR SPCHAR ");
			queryString.append("WHERE ACCTCHAR.CHAR_TYPE_CD = 'CM-CUSTY' ");
			queryString.append("AND ACCTCHAR.CHAR_VAL in ('K1','K2','K3') ");
			queryString.append("and ACCTCHAR.EFFDT = (select max(actChar.EFFDT)" +
								                    " from CI_ACCT_CHAR actChar"+
								                    " where actChar.ACCT_ID = ACCTCHAR.ACCT_ID"+
								                    " and  actChar.CHAR_TYPE_CD = 'CM-CUSTY'"+  
								                    " AND actChar.CHAR_VAL in ('K1','K2','K3')"+
								                    " and actChar.EFFDT <= SYSDATE) "); 
			queryString.append("AND ACCTCHAR.ACCT_ID = USI.ID_VALUE ");
			queryString.append("AND USI.US_ID = USSP.US_ID ");
			queryString.append("AND USI.US_ID_TYPE_FLG = 'D2EA' ");
			queryString.append("AND USSP.D1_SP_ID = SPCHAR.D1_SP_ID ");
			queryString.append("AND SPCHAR.CHAR_TYPE_CD = 'CM-ETSO' ");
			queryString.append("AND SPCHAR.EFFDT = (select max(spChar.EFFDT) ");
			queryString.append("from D1_SP_CHAR spChar1 ");
			queryString.append("where spChar1.D1_SP_ID = SPCHAR.D1_SP_ID ");
			queryString.append("and spChar1.CHAR_TYPE_CD = 'CM-ETSO' ");
			queryString.append("and spChar1.EFFDT <= SYSDATE) ");
			queryString.append("AND SPCHAR.D1_SP_ID BETWEEN :lowId AND :highId");
			
			PreparedStatement statement = createPreparedStatement(queryString.toString(), "Retrieve SP ID's");
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
	    	 ThreadWorkUnit unit = new ThreadWorkUnit(row.getId("D1_SP_ID", ServicePointD1.class));
	    	 customerType = row.getString("CUSTOMER_TYPE");
	    	 meteringPointEIC = row.getString("EIC_CODE");
	    	 logger.info("meteringPointEIC "+meteringPointEIC);
	    	 return unit;
	     }

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			// TODO Auto-generated method stub
			logger.info("Inside Execute Work Unit"+unit.getPrimaryId());
			ServicePointD1_Id spId = (ServicePointD1_Id) unit.getPrimaryId();
			checkEligibility(spId);
			return true;
		}
		
		private void checkEligibility(ServicePointD1_Id spId) {
			logger.info("Customer Type"+customerType);
			eligibility = false;
			if((customerType.contains("K1"))){		
				logger.info("Inside K1 -Premise");
				ServicePointIdentifier_Id spIdentifier = new ServicePointIdentifier_Id(spId.getEntity(),ServicePointIdentifierTypeLookup.constants.EXTERNAL_PREMISE_ID);
				if(notNull(spIdentifier.getEntity())){
					Premise_Id premiseId = new Premise_Id(spIdentifier.getEntity().getIdValue());
					if(notNull(premiseId.getEntity())){
						Premise premise =  premiseId.getEntity();
	                    PremiseCharacteristic premiseChar =  (PremiseCharacteristic) premise.getCharacteristics().createFilter("where CHAR_TYPE_CD = 'CM-ELGPR' order by EFFDT ","").firstRow();
					    if(notNull(premiseChar)){
					    	String charValue = premiseChar.fetchCharacteristicValue().getId().getCharacteristicValue();
	                 		if(charValue.trim().equalsIgnoreCase("Y")){
	                 			eligibility = true;
	                 		}
	                 	}
					}						
				}
			}
			//Index Add		
			if((customerType.contains("K2")) || (customerType.contains("K3")) || (eligibility)){
				eligibility = true;
				logger.info("Inside Index Add");
				retriveConsumptionIndexAndConsumptionAdd(spId);	
			}
			logger.info("Eligibility"+eligibility);
		}
		
		private void retriveConsumptionIndexAndConsumptionAdd(ServicePointD1_Id spId){
			activeMcList.clear();
		    inductiveMcList.clear();
		    capacitiveMcList.clear();
		    demandMcList.clear();
		  	firstMeasureDate = null;
		  	lastMeasureDate = null;
			String powerType;
			InstallEvent installEvent = spId.getEntity().getCurrentDeviceInstallation(getSystemDateTime());
            if(notNull(installEvent))
            {            	
            	DeviceConfiguration deviceConfig = installEvent.getDeviceConfiguration(); 
            	if(notNull(deviceConfig)){
            		deviceId= deviceConfig.getId().getIdValue();
            	}
            	logger.info("Device Configuration"+deviceConfig.getId().getIdValue());
            	scalarMcList = deviceConfig.retrieveMeasuringComponents(IntervalScalarLookup.constants.SCALAR);
            	//Retrieve required Fields for every MC 
				if((notNull(scalarMcList)) && (scalarMcList.size() > 0))
		        {    
					if(notNull(meteringPointEIC) && meteringPointEIC.length()>=15){
					    Integer temp  = Integer.parseInt(meteringPointEIC.substring(4, 15));
					    meteringPointID = temp.toString();
					    logger.info("meteringPointID "+meteringPointID);
				    }
				    //logic to retrieve consumption export dttm
		            BusinessObjectInstance spBoInstance = BusinessObjectInstance.create("X1D-ServicePoint");//consumptionNettingDate
		            spBoInstance.set("spId", spId.getIdValue());
		            spBoInstance = BusinessObjectDispatcher.read(spBoInstance);
		            consumptionExportDttm = spBoInstance.getDateTime("consumptionExportDttm");
		            if(isNull(consumptionExportDttm)){
		            	consumptionExportDttm = spBoInstance.getDateTime("creationDateTime");
		            }
		            logger.info("Consumption Extraction Date Time"+consumptionExportDttm );
		            periodEndDate = getProcessDateTime().getDate();
					int year =  periodEndDate.getYear();
					int month = periodEndDate.getMonth();
					int days = periodEndDate.getMonthValue().getFirstDayOfMonth().getDay();
					try {
						lastMeasureDate = dataFormat.parseDateTime(year+"-"+month+"-"+days+"-00:00:00");
						periodEndDttm = lastMeasureDate;
					}	
					catch (DateFormatParseException e) { 
						e.printStackTrace();
					} 
					firstMeasureDate = consumptionExportDttm;
					if (notNull(consumptionHorizonDays) && consumptionHorizonDays.compareTo(BigInteger.ZERO) > 0) {
						tempStartDateTime = lastMeasureDate.addDays(-consumptionHorizonDays.intValue());
						if (notNull(consumptionExportDttm)) {
							if (tempStartDateTime.isBefore(consumptionExportDttm)) {
								firstMeasureDate = tempStartDateTime;
							}
						}
					} 
					periodStartDttm = firstMeasureDate;
			    	logger.info("First Measure Date"+firstMeasureDate);
			    	logger.info("Last Measure Date"+ lastMeasureDate);
			    	if(firstMeasureDate.compareTo(lastMeasureDate) <= 0)
			    	{
	            		BusinessObjectInstance deviceConfigBO = BusinessObjectInstance.create("CM-DeviceConfiguration");
	            		deviceConfigBO.set("deviceConfigurationId",deviceConfig.getId().getIdValue());
	            		deviceConfigBO = BusinessObjectDispatcher.read(deviceConfigBO, true);
	            		Element multiplierGroup = deviceConfigBO.getElement().element("multipliers");
	            		Element factorElement = multiplierGroup.element("deviceMultiplier");
	            		           		
	            		double factor1 = Double.valueOf(factorElement.getData().toString());
	            		factor = new BigDecimal(factor1);
	            		logger.info("Factor"+factor);
	            		
	            		//Retrieve Manufacturer 
	            		if(notNull(deviceConfig.getDevice().getManufacturerId())){
	            		   meterManufacturer = deviceConfig.getDevice().getManufacturerId().getIdValue();
	            		}
	            		//Retrieve Serial Number
	            		DeviceIdentifier deviceIdentifier = new DeviceIdentifier_Id(deviceConfig.getDevice(), DeviceIdentifierTypeLookup.constants.SERIAL_NUMBER).getEntity();
	            		meterSerialNumber = deviceIdentifier.getIdValue();
	            		logger.info("meterSerialNumber "+meterSerialNumber);
				    	//Retrieve Logic for First Load Type and First Measure Type.
				    	firstMeasureType = "PERIODIC";
				    	//Retrieve Logic for Last Load Type and Last Measure Type.
				    	lastMeasureType = "PERIODIC";
				    	BusinessObjectInstance meterTypeLookupInstance = BusinessObjectInstance.create("CM-MeterTypeLookup");
				        meterTypeLookupInstance.set("bo", "CM-MeterTypeLookup");
				        meterTypeLookupInstance.set("lookupValue",deviceConfig.getDeviceConfigurationType().getId().getIdValue());
				        meterTypeLookupInstance = BusinessObjectDispatcher.read(meterTypeLookupInstance);
				        if(notNull(meterTypeLookupInstance.getLookup("meterType"))){
					        meterType = meterTypeLookupInstance.getLookup("meterType").getConstantName();
					        logger.info("meterType "+meterType);
				        }   
						Iterator<MeasuringComponent> scalarMcListIterator = scalarMcList.iterator();
		                while(scalarMcListIterator.hasNext()){
		                	MeasuringComponent mc = scalarMcListIterator.next();
		                	UnitOfMeasureD1 unitOfMeasure = mc.retrieveUOM(ValueIdentifierTypeLookup.constants.MEASUREMENT);
		                	BusinessObjectInstance powerTypeLookupInstance =  BusinessObjectInstance.create("CM-UOMPowerTypeRelation");
		                	powerTypeLookupInstance.set("bo", "CM-UOMPowerTypeRelation");
		                	powerTypeLookupInstance.set("lookupValue",unitOfMeasure.getId().getIdValue());
		        			powerTypeLookupInstance = BusinessObjectDispatcher.read(powerTypeLookupInstance);
		        			if(notNull(powerTypeLookupInstance.getLookup("powerType"))){
			        			powerType = powerTypeLookupInstance.getLookup("powerType").getConstantName();
			        			logger.info("powerType "+powerType);
			        			if(powerType.equals("ACTIVE")){
			        				logger.info("inside sctive "+mc);
			        				activeMcList.add(mc);  
			        			}
			        			else if(powerType.equals("CAPACITIVE")){
			        				capacitiveMcList.add(mc);
			        			}
			        			else if(powerType.equals("INDUCTIVE")){
			        				inductiveMcList.add(mc);
			        			}
			        			else if(powerType.equals("DEMAND")){
			        				demandMcList.add(mc);
			        			}
		        			}	
		                }	
		                if(activeMcList.size()>0){
		                	powerTypeValue = "ACTIVE";
		                	retriveReadsandConsumption(activeMcList);	
		                }
		                if(capacitiveMcList.size()>0){
		                	powerTypeValue = "CAPACITIVE";
		                	retriveReadsandConsumption(capacitiveMcList);
		                }
		                if(inductiveMcList.size()>0){
		                	powerTypeValue = "INDUCTIVE";
		                	retriveReadsandConsumption(inductiveMcList);	
		                }
		                if(demandMcList.size()>0){
		                	powerTypeValue = "DEMAND";
		                	retriveReadsandConsumption(demandMcList);
		                }
			    	    	
		            }
			    	//update Service Point BO - Consumption Extract Date Time
	                spBoInstance.set("consumptionExportDttm",periodEndDttm);
	                spBoInstance = BusinessObjectDispatcher.update(spBoInstance);
	                logger.info("Updated SP BO"+spBoInstance.getDocument().asXML());	
		        }    
		    }			
		}
		private void retriveReadsandConsumption(List<MeasuringComponent> measuringComponent){
			logger.info("measuringComponent.size() "+measuringComponent.size());
			firstT0 = BigDecimal.ZERO;
			firstT1 = BigDecimal.ZERO;
			firstT2 = BigDecimal.ZERO;
			firstT3 = BigDecimal.ZERO;
			lastT0 = BigDecimal.ZERO;
			lastT1 = BigDecimal.ZERO;
			lastT2 = BigDecimal.ZERO;
			lastT3 = BigDecimal.ZERO;
			dayTimeConsumption = BigDecimal.ZERO;
			nightTimeConsumption = BigDecimal.ZERO;
			peakConsumption = BigDecimal.ZERO;
			List<SQLResultRow> sqlResultRow = retriveImdDates();
			if(sqlResultRow.size()>1){
				SQLResultRow row = sqlResultRow.get(0);
				firstMeasureDate = row.getDateTime("D1_TO_DTTM");
				Iterator<MeasuringComponent> mcItr = measuringComponent.iterator();
				logger.info("*****Data******");
				while(mcItr.hasNext()){
					MeasuringComponent scalarMc = mcItr.next();
					digitCount = scalarMc.getNumberOfDigitsLeft();
					logger.info("digit count "+digitCount);
					MeasuringComponentTypeValueIdentifier_Id mcValIdentifierId = new MeasuringComponentTypeValueIdentifier_Id(scalarMc.getMeasuringComponentType().getId(), ValueIdentifierTypeLookup.constants.MEASUREMENT);
	        		MeasuringComponentTypeValueIdentifier mcValIdentifier = mcValIdentifierId.getEntity();

					if(notNull(mcValIdentifier.fetchTimeOfUse())){
						tou = mcValIdentifier.fetchTimeOfUse().getId().getIdValue();
						logger.info("tou "+tou);
					}		
					logger.info("scalarMc  "+scalarMc);
					logger.info("firstMeasureDate "+firstMeasureDate );
					Measurement_Id msrmtId = new Measurement_Id(scalarMc,firstMeasureDate);
					logger.info("msrmtId 1 "+msrmtId.toString());
					if(notNull(msrmtId)){
						Measurement msrmt = msrmtId.getEntity();
						if(notNull(msrmt)){
							//retrieving consumption data for first IMD.
			        		//retrieving first measure type
				        	firstLoadType = retrieveLoadType(msrmt);
				        	logger.info("firstLoadType "+firstLoadType);
							firstMeasureType = retrieveMeasureType(msrmt);
							logger.info("first measure type "+firstMeasureType);
							if(tou.contentEquals("T1")){
								firstT1 = msrmt.getReadingValue();        					
		    				}
		    				else if (tou.contentEquals("T2")) {
		    					firstT2 = msrmt.getReadingValue();
		    				}
		    				else if (tou.contentEquals("T3")) {
		    					firstT3 = msrmt.getReadingValue();
		    				}
		    				else{ 
		    					firstT0 = msrmt.getReadingValue();
		    				}
							if (tou.contentEquals("T1")) {
		                		dayTimeConsumption = dayTimeConsumption.add(msrmt.getMeasurementValue());
	        				}
	        				else if (tou.contentEquals("T2")) {
		                		nightTimeConsumption = nightTimeConsumption.add(msrmt.getMeasurementValue());
	        				}
	        				else if (tou.contentEquals("T3")){ 
		                		peakConsumption = peakConsumption.add(msrmt.getMeasurementValue());
	        				}
						}	
					}	
				}	
				for(int i=1;i<sqlResultRow.size();i++){
					SQLResultRow lastrow = sqlResultRow.get(i);
					lastMeasureDate = lastrow.getDateTime("D1_TO_DTTM");
					mcItr = measuringComponent.iterator();
					logger.info("mcItr "+mcItr);
		        	while(mcItr.hasNext()){
		        		MeasuringComponent scalarMc = mcItr.next();
						//MeasuringComponentTypeValueIdentifier mcValIdentifier = scalarMc.getMeasuringComponentType().determinePrimaryMeasurementValueIdentifier();
		        		MeasuringComponentTypeValueIdentifier_Id mcValIdentifierId = new MeasuringComponentTypeValueIdentifier_Id(scalarMc.getMeasuringComponentType().getId(), ValueIdentifierTypeLookup.constants.MEASUREMENT);
		        		MeasuringComponentTypeValueIdentifier mcValIdentifier = mcValIdentifierId.getEntity();
						if(notNull(mcValIdentifier.fetchTimeOfUse())){
							tou = mcValIdentifier.fetchTimeOfUse().getId().getIdValue();
						}		
						Measurement_Id msrmtId = new Measurement_Id(scalarMc,lastMeasureDate);
						if(notNull(msrmtId)){							
							Measurement msrmt = msrmtId.getEntity();
							if(notNull(msrmt)){
								lastLoadType = retrieveLoadType(msrmt);
								lastMeasureType = retrieveMeasureType(msrmt);
								if(tou.contentEquals("T1")){
			    					lastT1 = msrmt.getReadingValue();        					
			    				}
			    				else if (tou.contentEquals("T2")) {
			    					lastT2 = msrmt.getReadingValue();
			    				}
			    				else if (tou.contentEquals("T3")) {
			    					lastT3 = msrmt.getReadingValue();
			    				}
			    				else{ 
			    					lastT0 = msrmt.getReadingValue();
			    				}
	                            //retrieving consumption data for first IMD.
					        	if (tou.contentEquals("T1")) {
					        		logger.info("dayTimeConsumption "+dayTimeConsumption);
					        		logger.info("msrmt.getMeasurementValue() "+msrmt.getMeasurementValue());
			                		dayTimeConsumption = dayTimeConsumption.add(msrmt.getMeasurementValue());
		        				}
		        				else if (tou.contentEquals("T2")) {
			                		nightTimeConsumption = nightTimeConsumption.add(msrmt.getMeasurementValue());
		        				}
		        				else if (tou.contentEquals("T3")){ 
			                		peakConsumption = peakConsumption.add(msrmt.getMeasurementValue());
		        				}
				        	}
							
						}	
					}	
		        	invokeRequestBusinessObject();
		            firstT0 = lastT0;
		            firstT1 = lastT1;
		            firstT2 = lastT2;
		            firstT3 = lastT3;
		            firstLoadType = lastLoadType;
		            firstMeasureType = lastMeasureType;
		            firstMeasureDate = lastMeasureDate;
		            dayTimeConsumption = BigDecimal.ZERO;
		            nightTimeConsumption = BigDecimal.ZERO;
		            peakConsumption = BigDecimal.ZERO;
		            
				}	
	        }	
	        else if(sqlResultRow.size()==1){
	        	SQLResultRow row = sqlResultRow.get(0);
				firstMeasureDate = row.getDateTime("D1_TO_DTTM");
				Iterator<MeasuringComponent> mcItr = measuringComponent.iterator();
				while(mcItr.hasNext()){
					MeasuringComponent scalarMc = mcItr.next();
					scalarMc.getNumberOfDigitsLeft();
					//MeasuringComponentTypeValueIdentifier mcValIdentifier = scalarMc.getMeasuringComponentType().determinePrimaryMeasurementValueIdentifier();
	        		MeasuringComponentTypeValueIdentifier_Id mcValIdentifierId = new MeasuringComponentTypeValueIdentifier_Id(scalarMc.getMeasuringComponentType().getId(), ValueIdentifierTypeLookup.constants.MEASUREMENT);
	        		MeasuringComponentTypeValueIdentifier mcValIdentifier = mcValIdentifierId.getEntity();

					if(notNull(mcValIdentifier.fetchTimeOfUse())){
						tou = mcValIdentifier.fetchTimeOfUse().getId().getIdValue();
					}		
					Measurement_Id msrmtId = new Measurement_Id(scalarMc,firstMeasureDate);
					if(notNull(msrmtId)){
						Measurement msrmt = msrmtId.getEntity();
						if(notNull(msrmt)){
	            			firstLoadType = retrieveLoadType(msrmt);
							firstMeasureType = retrieveMeasureType(msrmt);
							lastLoadType = firstLoadType;
							lastMeasureType = firstMeasureType;
							lastMeasureDate = firstMeasureDate;
							if(tou.contentEquals("T1")){
								firstT1 = msrmt.getReadingValue(); 
								lastT1 = firstT1;
		    				}
		    				else if (tou.contentEquals("T2")) {
		    					firstT2 = msrmt.getReadingValue();
		    					lastT2 = firstT2;
		    				}
		    				else if (tou.contentEquals("T3")) {
		    					firstT3 = msrmt.getReadingValue();
		    					lastT3 = firstT3;
		    				}
		    				else{ 
		    					firstT0 = msrmt.getReadingValue();
		    					lastT0 = firstT0;
		    				}
		                	if (tou.contentEquals("T1")) {
		                		logger.info("dayTimeConsumption "+dayTimeConsumption);
				        		logger.info("msrmt.getMeasurementValue() "+msrmt.getMeasurementValue());
		                		dayTimeConsumption = dayTimeConsumption.add(msrmt.getMeasurementValue());
	        				}
	        				else if (tou.contentEquals("T2")) {
		                		nightTimeConsumption = nightTimeConsumption.add(msrmt.getMeasurementValue());
	        				}
	        				else if (tou.contentEquals("T3")){ 
		                		peakConsumption = peakConsumption.add(msrmt.getMeasurementValue());
	        				}
		                	invokeRequestBusinessObject();
	        			}
					}	
        		} 
	        }
		}	
		private List<SQLResultRow> retriveImdDates(){
			StringBuffer query =  new StringBuffer();
			query = query.append("select distinct (D1_TO_DTTM) from D1_INIT_MSRMT_data where");
			query = query.append(" measr_comp_id in (select measr_comp_id from D1_Measr_comp where Device_config_id=:deviceId)");
            query = query.append(" and NVL(D1_FROM_DTTM,:periodStartDttm) >=:periodStartDttm");
            query = query.append(" and D1_TO_DTTM <= :periodEndDttm");
            query = query.append(" order by D1_TO_DTTM");
            PreparedStatement statement = createPreparedStatement(query.toString(),"IMD dates");
            statement.bindDateTime("periodStartDttm",firstMeasureDate);
            statement.bindDateTime("periodEndDttm",lastMeasureDate);
            statement.bindString("deviceId", deviceId, "");
            return statement.list();

		}
		private void invokeRequestBusinessObject(){
			 if(powerTypeValue.equals("ACTIVE")){
         		if(meterType.equals("ELECTRONIC")){
         			invokeIndexAddRequestBO(null,firstT1,firstT2,firstT3,null,lastT1,lastT2,lastT3);
         			invokeConsumptionAddRequestBO();
         		}
         		else if(meterType.equals("MECHANICAL")){
         			invokeIndexAddRequestBO(firstT0,null,null,null,lastT0,null,null,null);
         		}	
             }
			 else if(powerTypeValue.equals("CAPACITIVE")){
             	invokeIndexAddRequestBO(firstT0,null,null,null,lastT0,null,null,null);
	    		
             }
			 else if(powerTypeValue.equals("INDUCTIVE")){
             	invokeIndexAddRequestBO(firstT0,null,null,null,lastT0,null,null,null);	
             }
			 else if(powerTypeValue.equals("DEMAND")){
             	invokeIndexAddRequestBO(firstT0,null,null,null,null,null,null,null);
             }
		}
		private String retrieveLoadType(Measurement msrmt){
			logger.info("msrmt.getMeasurementCondition() "+msrmt.getMeasurementCondition());
			String conditionCode = msrmt.getMeasurementCondition().substring(0, 3);
			logger.info("conditionCode "+conditionCode);
			if(conditionCode.equals("301") || conditionCode.equals("401")|| conditionCode.equals("402")){
				return "PREDICTED";
			}
			else if(conditionCode.equals("501") || conditionCode.equals("502")){
				return "AUTOMATED";
			}
			else if(conditionCode.equals("503")){
				return "MANUALLY";
			}
			return null;
		}
		private String retrieveMeasureType(Measurement msrmt){
			//String loadType = null;
			logger.info("msrmt "+msrmt.toString());
			logger.info("msrmt "+msrmt);
			logger.info("msrmt.getMeasurementCondition() "+msrmt.getMeasurementCondition());
			BusinessObjectInstance imdLite = BusinessObjectInstance.create("CM-InitialMeasrDataLite");
			imdLite.set("initialMeasurementDataId", msrmt.fetchOriginalInitialMeasurement().getId().getIdValue());
			imdLite = BusinessObjectDispatcher.read(imdLite);
			logger.info("imdLite "+imdLite.getDocument());
			COTSInstanceNode syncIMDOtherInfo = imdLite.getGroup("syncIMDOtherInfo");
			logger.info("syncIMDOtherInfo "+syncIMDOtherInfo);
			if(notNull(syncIMDOtherInfo)){
				if(notNull(syncIMDOtherInfo.getFieldAndMD("measurementType"))){
					COTSFieldDataAndMD<?> measurementType = syncIMDOtherInfo.getFieldAndMD("measurementType");
					if(notNull(measurementType)){
						return measurementType.getXMLValue();
					}
					else{
						return null;
					}
				}
				else{
					return null;
				}
			}
			else{
				return null;
			}
					
		}
		private void retriveMeasureType(){
			
		}
		private void invokeIndexAddRequestBO(BigDecimal fT0,BigDecimal fT1,BigDecimal fT2,BigDecimal fT3,BigDecimal lT0,BigDecimal lT1,BigDecimal lT2,BigDecimal lT3){
			logger.info("inside invoke BO");
			BusinessObjectInstance boInstance = BusinessObjectInstance.create("CM-DEDASConExpRequest");
			boInstance.set("bo", "CM-DEDASConExpRequest");
			boInstance.set("requestType",requestType);
			boInstance.set("consumptionType",ConsumptionTypeFlagLookup.constants.INDEX);
			COTSInstanceNode sendDetailsGroup = boInstance.getGroupFromPath("sendDetails");
			COTSInstanceList indexesList = sendDetailsGroup.getList("indexes");			
			COTSInstanceListNode indexData = indexesList.newChild();
			indexData.set("factor",factor);
			indexData.set("digitCount", BigDecimal.valueOf(digitCount.intValue()));
			indexData.set("firstLoadType",firstLoadType);
			indexData.set("firstMeasureDate",firstMeasureDate);
			indexData.set("firstMeasureType",firstMeasureType);
			indexData.set("firstT0", fT0);
			indexData.set("firstT1", fT1);
			indexData.set("firstT2", fT2);
			indexData.set("firstT3", fT3);
			indexData.set("lastLoadType",lastLoadType);
			indexData.set("lastMeasureDate",lastMeasureDate);
			indexData.set("lastMeasureType",lastMeasureType);
			indexData.set("lastT0",lT0);
			indexData.set("lastT1",lT1);
			indexData.set("lastT2",lT2);
			indexData.set("lastT3",lT3);
			indexData.set("meterManufacturer",meterManufacturer);
			indexData.set("meterSerialNumber",meterSerialNumber);
			indexData.set("meteringPointEIC",meteringPointEIC);
			indexData.set("meterType",meterType);
			indexData.set("period",lastMeasureDate.toDate());
			indexData.set("meteringPointID",meteringPointID);
			indexData.set("powerType",powerTypeValue);
			logger.info(boInstance.getDocument().asXML());
			BusinessObjectDispatcher.add(boInstance);
			
		}
		
		private void invokeConsumptionAddRequestBO(){
			logger.info("inside consumption BO");
			BusinessObjectInstance boInstance = BusinessObjectInstance.create("CM-DEDASConExpRequest");
			boInstance.set("bo", "CM-DEDASConExpRequest");
			boInstance.set("requestType",requestType);
			boInstance.set("consumptionType",ConsumptionTypeFlagLookup.constants.CONSUMPTION);
			COTSInstanceNode sendDetailsGroup = boInstance.getGroupFromPath("sendDetails");
			COTSInstanceNode consumptionGroup = sendDetailsGroup.getGroupFromPath("consumption");
			consumptionGroup.set("meteringPointEIC",meteringPointEIC);
			consumptionGroup.set("setlementPeriod",periodStartDttm);
			COTSInstanceNode consumptionDataGroup = consumptionGroup.getGroupFromPath("consumptionData");
			consumptionDataGroup.set("dayTimeConsumption",dayTimeConsumption);
			consumptionDataGroup.set("nightTimeConsumption",nightTimeConsumption);
			consumptionDataGroup.set("peakConsumption",peakConsumption);
			logger.info(boInstance.getDocument().asXML());
			BusinessObjectDispatcher.add(boInstance);
			
		}
		
		
	}
}
