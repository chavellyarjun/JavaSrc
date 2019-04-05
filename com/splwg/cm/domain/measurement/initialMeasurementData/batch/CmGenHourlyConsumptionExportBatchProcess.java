  package com.splwg.cm.domain.measurement.initialMeasurementData.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.Element;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.QueryIterator;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadIterationStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.businessObject.COTSInstanceList;
import com.splwg.base.api.businessObject.COTSInstanceListNode;
import com.splwg.base.api.businessObject.COTSInstanceNode;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.DateFormat;
import com.splwg.base.api.datatypes.DateFormatParseException;
import com.splwg.base.api.datatypes.DateInterval;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.StringId;
import com.splwg.base.api.datatypes.TimeInterval;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.ccb.api.lookup.ConsumptionTypeLookup;
import com.splwg.ccb.domain.customerinfo.premise.entity.Premise;
import com.splwg.ccb.domain.customerinfo.premise.entity.PremiseCharacteristic;
import com.splwg.ccb.domain.customerinfo.premise.entity.Premise_Id;
import com.splwg.ccb.domain.customerinfo.servicePoint.entity.ServicePoint_Id;
import com.splwg.cm.api.lookup.ConsumptionTypeFlagLookup;
import com.splwg.cm.api.lookup.PowerTypeFlagLookup;
import com.splwg.cm.domain.admin.meterReader.MeterReader;
import com.splwg.cm.domain.admin.meterReader.MeterReader_Id;
import com.splwg.cm.domain.measurement.initialMeasurementData.batch.CmConsumptionExportBatchProcess.CmConsumptionExportBatchProcessWorker;
import com.splwg.d1.api.lookup.IntervalScalarLookup;
import com.splwg.d1.api.lookup.ServicePointIdentifierTypeLookup;
import com.splwg.d1.domain.admin.measuringComponentType.entities.MeasuringComponentTypeValueIdentifier;
import com.splwg.d1.domain.admin.serviceQuantityIdentifier.entities.ServiceQuantityIdentifierD1;
import com.splwg.d1.domain.deviceManagement.device.entities.DeviceIdentifier;
import com.splwg.d1.domain.deviceManagement.deviceConfiguration.entities.DeviceConfiguration;
import com.splwg.d1.domain.deviceManagement.measuringComponent.entities.MeasuringComponent;
import com.splwg.d1.domain.installation.installEvent.entities.InstallEvent;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointD1;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointD1_Id;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointIdentifier;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointIdentifier_Id;
import com.splwg.d1.domain.measurement.measurement.entities.Measurement;
import com.splwg.d1.domain.measurement.measurement.entities.Measurement_Id;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 *  @author
 *  
 @BatchJob (modules = {},
 *            softParameters = {@BatchJobSoftParameter (entityName = requestType, name = requestType, required = true, type = entity)
 *            ,@BatchJobSoftParameter (name = consumptionExportHorizon, type = integer)
 *            ,@BatchJobSoftParameter (name = maxErrors, type = integer)})
 *  
*/
public class CmGenHourlyConsumptionExportBatchProcess extends CmGenHourlyConsumptionExportBatchProcess_Gen{
public static Logger logger = LoggerFactory.getLogger(CmGenHourlyConsumptionExportBatchProcess.class);
	
	
	public Class<CmGenHourlyConsumptionExportBatchProcessWorker> getThreadWorkerClass() {
		return CmGenHourlyConsumptionExportBatchProcessWorker.class;
		
	}

	public static class CmGenHourlyConsumptionExportBatchProcessWorker extends
	CmGenHourlyConsumptionExportBatchProcessWorker_Gen {
		
		private static String meteringPointEIC = null;
		private static String requestType = null;
		private static String customerType = null;
		static int consumptionExportHorizon ; 
		static int conMissingMsrmtCount=0;
		static int genMissingMsrmtCount=0;
		static DateTime measrDttm = null;
		static DateTime tempMeasrDttm = null;
		static DateTime consumptionExtractDttm =  null;
        private static DateFormat dataFormat = new DateFormat("yyyy-MM-dd-HH:mm:ss");
        boolean isEligibility ;
        DateTime periodEndDttm;
        DateTime periodStartDttm;
        int interval;
        int numberOfIntervals;
        BigDecimal consumption = BigDecimal.ZERO;
        BigDecimal generation =  BigDecimal.ZERO;
        DateTime period = null;
        long spi;
        String batchCode;
        BusinessObjectInstance requestBoInstance;
        COTSInstanceNode sendDetailsGroup;
   		COTSInstanceList licencedGenHourlyConsumptionList;
		DateInterval periodDifference ;
		BusinessObjectInstance spBoInstance = null;
		boolean dataSetFlag = false ;
        
        
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
			
			requestType = getParameters().getRequestType().getId().getIdValue(); 
			if(notNull(getParameters().getConsumptionExportHorizon())){
				consumptionExportHorizon = getParameters().getConsumptionExportHorizon().intValue();
			}
	    	startResultRowQueryIteratorForThread(ServicePointD1_Id.class);
	     }
		
		/**
	     * Create an iterator for a simple query that selects a single business entity.
	 	 * Use lowId and highId to confine the selection to the current thread.  These two
	 	 * arguments are calculated and supplied by the framework.
	 	 */
		
		@Override
	     protected QueryIterator<SQLResultRow> getQueryIteratorForThread(StringId lowId, StringId highId) {
			StringBuilder queryString = new StringBuilder();
			queryString.append("Select DISTINCT SPCHAR1.D1_SP_ID,SPCHAR1.ADHOC_CHAR_VAL,ACCTCHAR.CHAR_VAL,ACCTCHAR.ACCT_ID ");
			queryString.append("FROM CI_ACCT_CHAR ACCTCHAR, D1_US_IDENTIFIER USI, D1_US_SP USSP, D1_SP_CHAR SPCHAR1,D1_SP_CHAR SPCHAR2, D1_SP SP ");
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
			queryString.append("AND SP.D1_SP_ID = USSP.D1_SP_ID ");
			queryString.append("AND SP.D1_SP_TYPE_CD = 'E-GEN' ");
			queryString.append("AND USSP.D1_SP_ID = SPCHAR1.D1_SP_ID ");
			queryString.append(" AND SPCHAR1.CHAR_TYPE_CD = 'CM-ETSO' ");
			queryString.append(" AND SPCHAR1.EFFDT in (select max(spChar.EFFDT)"+
													  " from D1_SP_CHAR spChar"+
													  " where spChar.D1_SP_ID = SPCHAR1.D1_SP_ID"+
													  " and spChar.CHAR_TYPE_CD = 'CM-ETSO'"+
													  " and spChar.EFFDT <= SYSDATE)");
			queryString.append(" and SPCHAR1.D1_SP_ID =  SPCHAR2.D1_SP_ID ");
			queryString.append(" and SPCHAR2.CHAR_TYPE_CD = 'CM-TNFIN' ");
			queryString.append(" and SPCHAR2.EFFDT in (select max(spChar.EFFDT) "+
													  " from D1_SP_CHAR spChar"+
													  " where spChar.D1_SP_ID = SPCHAR2.D1_SP_ID"+
													  " and spChar.CHAR_TYPE_CD = 'CM-TNFIN'"+
													  " and spChar.EFFDT <= SYSDATE)");
			queryString.append(" and SPCHAR2.ADHOC_CHAR_VAL >= '1' ");
			queryString.append(" AND SPCHAR1.D1_SP_ID BETWEEN :lowId AND :highId");
							
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
	    	 meteringPointEIC = row.getString("ADHOC_CHAR_VAL");
	    	 customerType = row.getString("CHAR_VAL");
	    	 logger.info("Metering point"+meteringPointEIC);
	    	 logger.info("spId "+row.getString("D1_SP_ID"));
	    	 return unit;
	     }

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			// TODO Auto-generated method stub
			ServicePointD1_Id spId = (ServicePointD1_Id) unit.getPrimaryId();
			retriveMeasuringComponents(spId);
			return true;
		}
		
		public void retriveMeasuringComponents(ServicePointD1_Id spId){
			
			/*String deleteQueryString = "delete from cm_mtr_rdr_rte where msrmt_cyc_cd='ONSP' and msrmt_cyc_rte_cd='160'"; 
			PreparedStatement deleteQuery = createPreparedStatement(deleteQueryString,"");
			deleteQuery.bindString("msrmt_cyc_cd", "ONSP", "");
						deleteQuery.bindString("msrmt_cyc_rte_cd", "160", "");
						//boolean status = deleteQuery.execute();
						deleteQuery.list(); 
		  logger.info(deleteQuery.toString());
			*/
			
			
			//set satrt and end periods
			isEligibility = false;
			setStartandEndPeriods(spId);
			ServicePointD1 sp = spId.getEntity();
			if((customerType.contains("K1"))){        
                logger.info("Inside K1 -Premise");
                ServicePointIdentifier_Id spIdentifier1 = new ServicePointIdentifier_Id(spId.getEntity(),ServicePointIdentifierTypeLookup.constants.EXTERNAL_PREMISE_ID);
                if(notNull(spIdentifier1)){
	                Premise_Id premiseId = new Premise_Id(spIdentifier1.getEntity().getIdValue());
	                if(notNull(premiseId)){
	                	Premise premise =  premiseId.getEntity();
	                	logger.info(premiseId.getIdValue());
	                	PremiseCharacteristic pre =  (PremiseCharacteristic) premise.getCharacteristics().createFilter("where CHAR_TYPE_CD = 'CM-ELGPR' order by EFFDT ","").firstRow();
	                 	if(notNull(pre)){
	                 		String charValue = pre.fetchCharacteristicValue().getId().getCharacteristicValue();
	                 		if(charValue.trim().equalsIgnoreCase("Y")){
	                 			isEligibility = true;
	                 		}
	                 		else{
	                 			isEligibility = false;
	                 		}
	                 	}
	                 	else{
	                 		isEligibility = false;
	                 	}
	                 }
			     }                 
			}
			else{
				isEligibility = true;
			}
			logger.info("isEligibility "+isEligibility);
			if(isEligibility == true){
				createRequestBusinessObject();
				InstallEvent ie = sp.getCurrentDeviceInstallation(getProcessDateTime());
				if(notNull(ie)){
					DeviceConfiguration dc = ie.getDeviceConfiguration();
					if(notNull(dc)){
						List<MeasuringComponent> intMcList  = dc.retrieveMeasuringComponents(IntervalScalarLookup.constants.INTERVAL);
						if(!intMcList.isEmpty() && notNull(intMcList)){
							List<MeasuringComponent>  finalMcList = new ArrayList<MeasuringComponent>();
							Iterator<MeasuringComponent> intervalMcListIterator = intMcList.iterator();
			                while(intervalMcListIterator.hasNext()){
			                	MeasuringComponent intervalMc = intervalMcListIterator.next();
			                	MeasuringComponentTypeValueIdentifier mcValIdentifier = intervalMc.getMeasuringComponentType().determinePrimaryMeasurementValueIdentifier();
			        			String uom = mcValIdentifier.fetchUnitOfMeasure().getId().getIdValue();			        			
			        			if(uom.contentEquals("KWH")){
			        				finalMcList.add(intervalMc);
			        			}
			                }
			                if(!finalMcList.isEmpty()){	
								calculateConsumptionAndGeneration(finalMcList);
								Document ins = BusinessObjectDispatcher.add(requestBoInstance.getDocument());
								logger.info("ins "+ins.asXML().toString());
								
			                }	
						}	
					}
				}
			}	
		}
		private void  calculateConsumptionAndGeneration(List<MeasuringComponent> mcList){
			measrDttm = periodStartDttm;
			while(measrDttm.isSameOrBefore(periodEndDttm)){
				generation = null;
    			consumption = null;
    			Iterator<MeasuringComponent> intervalMcListIterator = mcList.iterator();
                while(intervalMcListIterator.hasNext()){
	            	MeasuringComponent intervalMc = intervalMcListIterator.next();
	            	MeasuringComponentTypeValueIdentifier mcValIdentifier = intervalMc.getMeasuringComponentType().determinePrimaryMeasurementValueIdentifier();
	    			ServiceQuantityIdentifierD1 sqi = mcValIdentifier.fetchServiceQuantityIdentifier();
	    			spi = intervalMc.getMeasuringComponentType().getSecondsPerInterval().getAsSeconds(); 
	    			logger.info(" intervalMc "+intervalMc.getId().getIdValue());
	    			if(spi == 900){
	    				interval = 15; 
					}
					else if(spi == 1800){
						interval = 30;
					}
					else if(spi == 3600){
						interval = 60;  
					}				
    				if(notNull(sqi) && sqi.getId().getIdValue().equals("GEN")){
    					Measurement msrmt = new Measurement_Id(intervalMc,measrDttm).getEntity();
    					if(notNull(msrmt)){
    						generation = msrmt.getMeasurementValue();
    						logger.info("generation "+generation);
    					}
       				}
    				else{
    					Measurement msrmt = new Measurement_Id(intervalMc,measrDttm).getEntity();
    					if(notNull(msrmt)){
    						consumption = msrmt.getMeasurementValue();
    						logger.info("consumption "+consumption);
    					}
    				}
				}
                logger.info("measrDttm "+measrDttm);
                addChildElements(interval,consumption ,generation,measrDttm);
               // addChildElements();
                measrDttm = measrDttm.addMinutes(interval);
                logger.info("measrDttm "+measrDttm);
			}
		}
		private void setStartandEndPeriods( ServicePointD1_Id spId ){
			int hours = getProcessDateTime().getHours();
			try {
				logger.info("getProcessDateTime() "+getProcessDateTime());
				periodEndDttm = dataFormat.parseDateTime(getProcessDateTime().toDate()+"-"+hours+":00:00");
				if(consumptionExportHorizon >0){
					periodStartDttm = periodEndDttm.addHours(-consumptionExportHorizon);
				}
				else{
					periodStartDttm = periodEndDttm.addHours(-1);
				}	
			} catch (DateFormatParseException e) {
				// TODO Auto-generated catch block   
				e.printStackTrace();
			}
			logger.info("end dt "+periodEndDttm);
			logger.info("start dt "+periodStartDttm);
		}
		private void createRequestBusinessObject(){
			requestBoInstance = BusinessObjectInstance.create("CM-DEDASConExpRequest");
			requestBoInstance.set("bo", "CM-DEDASConExpRequest");
			requestBoInstance.set("requestType",requestType);
			requestBoInstance.set("consumptionType", ConsumptionTypeFlagLookup.constants.LICENCED_GEN_HOURLY_CON);
			sendDetailsGroup = requestBoInstance.getGroupFromPath("sendDetails");
			licencedGenHourlyConsumptionList = sendDetailsGroup.getList("licencedGenHourlyConsumption");
		}
		private void addChildElements(int interval,BigDecimal consmption,BigDecimal generation, DateTime measrDttm){
			COTSInstanceListNode consumptionData = licencedGenHourlyConsumptionList.newChild();
			consumptionData.set("meteringPointEIC", meteringPointEIC);
			consumptionData.set("readingDateTime",measrDttm );
			consumptionData.set("meteringType",BigDecimal.ONE);
			consumptionData.set("period",BigDecimal.valueOf(interval));
			if(notNull(generation)){
				consumptionData.set("generation",generation); 
			}
			if(notNull(consumption)){
			    consumptionData.set("consumption",consumption);
			}       
		}	
	}
}

