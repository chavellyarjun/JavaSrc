package com.splwg.cm.domain.measurement.initialMeasurementData.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.dom4j.Element;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.QueryIterator;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadIterationStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.DateFormat;
import com.splwg.base.api.datatypes.DateFormatParseException;
import com.splwg.base.api.datatypes.DateInterval;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.StringId;
import com.splwg.base.api.datatypes.TimeInterval;
import com.splwg.base.api.lookup.LogEntryTypeLookup;
import com.splwg.base.api.lookup.ToDoEntryStatusLookup;
import com.splwg.base.api.maintenanceObject.MaintenanceObjectLogHelper;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.businessObject.BusinessObject_Id;
import com.splwg.base.domain.common.characteristicType.CharacteristicType;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.base.domain.common.maintenanceObject.MaintenanceObject;
import com.splwg.base.domain.common.message.MessageCategory_Id;
import com.splwg.base.domain.common.message.MessageParameters;
import com.splwg.base.domain.common.message.Message_Id;
import com.splwg.base.domain.common.message.ServerMessageFactory;
import com.splwg.base.domain.todo.role.Role;
import com.splwg.base.domain.todo.role.Role_Id;
import com.splwg.base.domain.todo.toDoEntry.ToDoCreator;
import com.splwg.base.domain.todo.toDoEntry.ToDoDrillKeyValue;
import com.splwg.base.domain.todo.toDoEntry.ToDoEntry;
import com.splwg.base.domain.todo.toDoEntry.ToDoEntry_DTO;
import com.splwg.base.domain.todo.toDoType.ToDoDrillKeyType;
import com.splwg.base.domain.todo.toDoType.ToDoDrillKeyType_DTO;
import com.splwg.base.domain.todo.toDoType.ToDoDrillKeyType_Id;
import com.splwg.base.domain.todo.toDoType.ToDoSortKeyType;
import com.splwg.base.domain.todo.toDoType.ToDoType;
import com.splwg.base.domain.todo.toDoType.ToDoTypeDrillKeyTypes;
import com.splwg.base.domain.todo.toDoType.ToDoTypeSortKeyTypes;
import com.splwg.base.domain.todo.toDoType.ToDoType_Id;
import com.splwg.cm.domain.customMessages.CmMessageRepository;
import com.splwg.d1.api.lookup.IntervalScalarLookup;
import com.splwg.d1.api.lookup.ServicePointIdentifierTypeLookup;
import com.splwg.d1.api.lookup.ValueIdentifierTypeLookup;
import com.splwg.d1.domain.admin.unitOfMeasure.entities.UnitOfMeasureD1;
import com.splwg.d1.domain.deviceManagement.deviceConfiguration.entities.DeviceConfiguration;
import com.splwg.d1.domain.deviceManagement.deviceConfiguration.entities.DeviceConfiguration_Id;
import com.splwg.d1.domain.deviceManagement.measuringComponent.entities.MeasuringComponent;
import com.splwg.d1.domain.deviceManagement.measuringComponent.entities.MeasuringComponentLog;
import com.splwg.d1.domain.deviceManagement.measuringComponent.entities.MeasuringComponent_Id;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointD1;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointD1_Id;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointIdentifier_Id;
import com.splwg.d1.domain.measurement.measurement.entities.Measurement;
import com.splwg.d1.domain.measurement.measurement.entities.Measurement_DTO;
import com.splwg.d1.domain.measurement.measurement.entities.Measurement_Id;
import com.splwg.d1.domain.usage.usageSubscription.entities.UsageSubscription;
import com.splwg.shared.common.ServerMessage;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;
/**
 * @author Manasa
 *
@BatchJob (modules = {},
softParameters = { @BatchJobSoftParameter (name = nettingHorizonDays, type = integer)
 *            , @BatchJobSoftParameter (entityName = servicePointType, name = generationServicePointType, type = entity)
 *            , @BatchJobSoftParameter (name = toDoRequired, type = string)
 *            , @BatchJobSoftParameter (entityName = toDoType, name = toDoType, type = entity)
 *            , @BatchJobSoftParameter (entityName = role, name = toDoRole, type = entity)
 *            , @BatchJobSoftParameter (name = MAX_ERRORS, type = integer)})
 */
            


public class CmCalculateNetConsumptionBatchProcess extends CmCalculateNetConsumptionBatchProcess_Gen{
	private static Logger log = LoggerFactory.getLogger(CmCalculateNetConsumptionBatchProcess.class);
	
	private final static String NETTING_MASTER_CONFIG_BO = "CM-MCBO";
	private final static String VALID_IDENTIFIER_GRP = "valueIdentifiers";
	 private static final String TODO_CHARACTERISTIC_TYPE = "F1-TODO";
	private static long mdgCId = 90000;
	private static long msgID = 99305;
	private static int nettingHorizonDays;
	private static String toDoRequired;
	private static String spType;
	private static Role  toDoRole;
	private static ToDoType toDoType;
	private static int maxErrors;
	private static Date periodStartDate;
	private static Date periodEndDate;
	private static DateTime periodStartDttm;
	private static DateTime periodEndDttm;
	private static DateTime installationDttm = null;
	private static DateTime removalDttm = null;
	private static BigDecimal totalHourlyConsumption = BigDecimal.ZERO;
	private static BigDecimal hourlyGenerationAftConsumption = BigDecimal.ZERO;
	private static BigDecimal netConsumption = BigDecimal.ZERO;
	private static BigDecimal netGeneration = BigDecimal.ZERO;
	private static BigDecimal netConsumptionWithoutUS = BigDecimal.ZERO;
	private static DeviceConfiguration_Id generationDcId ;
	private static MeasuringComponent_Id generationMcId;
	private static BigDecimal hourlyGeneration;
	private static DateTime generationMsrmtDttm;
	private static String generationMcType; 
	private static BigDecimal hourlyConsumption;
	private static TimeInterval secondaPerInterval;
	//private static TimeInterval condaPerInterval;
	private static Boolean usageSubscriptionExits = true;
	private static String unitOfMeasure="";
	private static Boolean missingMsrmts = false; 
	private static int numberOfIntervals;
	private static String generationPremiseId;
	private static DateFormat dataFormat = new DateFormat("yyyy-MM-dd-HH:mm:ss");
	private static List<String> validMC = new ArrayList<String>();
	private static List<String> missingMeasurements = new ArrayList<String>();
	
	/**
	 * @ Method to validate Soft Parameters.
	 * 
	 * Input
	 * - Netting Horizon Days
	 * - Netting Lag Days
	 * - Max Error
	 * 
	 * Output
	 * - If the File Path and File Name are valid
	 *        - New file is created.
	 * - If the File Path or File Name is invalid
	 *        - Sends Error Message and terminates.
	 *
	 */
	public void validateSoftParameters(boolean isNewRun) {
		log.debug("Start of validateSoftParameters");
		if(notNull(getParameters().getMAX_ERRORS())){
			maxErrors = getParameters().getMAX_ERRORS().intValue();
		}
		toDoRequired = getParameters().getToDoRequired();
		if(!isBlankOrNull(toDoRequired)){
			if(toDoRequired.trim().equalsIgnoreCase("Y")|| toDoRequired.trim().equalsIgnoreCase("N")){
			}
			else{
				addError(CmMessageRepository.toDoRequiredValue());
				toDoRequired =null;
			}
		}
		else
		{
			toDoRequired = "Y";
		}
		toDoType = getParameters().getToDoType();
		if(isNull(toDoType)){
			toDoType = new ToDoType_Id("CM-MREAD").getEntity();
		}
		toDoRole = getParameters().getToDoRole();
		if(isNull(toDoRole)){
			toDoRole = new Role_Id("CM-MREADS").getEntity();
		}
		if(notNull(getParameters().getGenerationServicePointType()))
		spType = getParameters().getGenerationServicePointType().getId().getIdValue().toString().trim();
		else{
			spType = "E-GEN"; 
		}
		
	}
	
	public JobWork getJobWork() {
		log.debug("Start Job Work Method");
		List<ThreadWorkUnit> workUnits = new ArrayList<ThreadWorkUnit>();
		ThreadWorkUnit workUnit = new ThreadWorkUnit();
		workUnits.add(workUnit);
		log.debug("End Job Work Method");
		return createJobWorkForThreadWorkUnitList(workUnits);
	}

	public Class<CmCalculateNetConsumptionBatchProcessWorker> getThreadWorkerClass() {
		return CmCalculateNetConsumptionBatchProcessWorker.class;
	}

	public static class CmCalculateNetConsumptionBatchProcessWorker extends CmCalculateNetConsumptionBatchProcessWorker_Gen { 
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
    	 if(notNull(parameters.getNettingHorizonDays())){
    		 nettingHorizonDays = parameters.getNettingHorizonDays().intValue();
    	 } 
    	 periodEndDate = getProcessDateTime().getDate();
			int year =  periodEndDate.getYear();
			int month = periodEndDate.getMonth();
			int days = periodEndDate.getMonthValue().getFirstDayOfMonth().getDay();
			try {
				periodEndDttm = dataFormat.parseDateTime(year+"-"+month+"-"+days+"-00:00:00");
				if(notNull(nettingHorizonDays)&& nettingHorizonDays>0){
					periodStartDate = periodEndDttm.getDate().addDays(-nettingHorizonDays);
				}
				else{
					periodStartDate = periodEndDttm.getDate().addMonths(-1);
				}
				
				periodStartDttm = dataFormat.parseDateTime(periodStartDate.toString()+"-00:00:00");
				log.debug("periodEndDttm "+periodEndDttm);
				log.debug("periodStartDttm "+periodStartDttm);
			} catch (DateFormatParseException e) { 
				e.printStackTrace();
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
			 
			StringBuilder query = new StringBuilder();
			query.append("select sp.D1_SP_ID");
			query.append(" from D1_SP sp, D1_INSTALL_EVT ie");
			query.append(" where sp.D1_SP_TYPE_CD ='"+spType+"'");
			query.append(" and sp.BO_STATUS_CD='ACTIVE'");
			query.append(" and sp.D1_SP_ID = ie.D1_SP_ID");
			query.append(" and ie.D1_INSTALL_DTTM <= :periodEndDttm");
			query.append(" and NVL(ie.D1_REMOVAL_DTTM,:periodStartDttm) >= :periodStartDttm");
			query.append(" and sp.D1_SP_ID BETWEEN :lowId AND :highId");
		    PreparedStatement statement = createPreparedStatement(query.toString(),"Retrive SpID's ");
			statement.bindId("lowId", lowId);
			statement.bindId("highId", highId);
			statement.bindDateTime("periodEndDttm", periodEndDttm);
			statement.bindDateTime("periodStartDttm", periodStartDttm);
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
	    	 return unit;
	     }

	     public boolean executeWorkUnit(ThreadWorkUnit unit)
					throws ThreadAbortedException, RunAbortedException {
			    	ServicePointD1_Id spId = (ServicePointD1_Id) unit.getPrimaryId();
			    	calculateNettingValue(spId);
			    	return true;
			    }
	    
		private void calculateNettingValue(ServicePointD1_Id spId) {
			ServicePointIdentifier_Id spIdentifier1 = new ServicePointIdentifier_Id(spId.getEntity(),ServicePointIdentifierTypeLookup.constants.EXTERNAL_PREMISE_ID);
            generationPremiseId = spIdentifier1.getEntity().getIdValue();
            log.info("generationPremiseId "+generationPremiseId);
            log.info("spId "+spId);
			BusinessObjectInstance masterConfgiBOInstance = BusinessObjectInstance.create(NETTING_MASTER_CONFIG_BO);
            masterConfgiBOInstance.set("bo", NETTING_MASTER_CONFIG_BO);
            masterConfgiBOInstance = BusinessObjectDispatcher.read(masterConfgiBOInstance, true);
            Element mcValidIdentifierGroup = masterConfgiBOInstance.getElement().element(VALID_IDENTIFIER_GRP);
            if (notNull(mcValidIdentifierGroup)) {
                List<Element> mcValidIdentifierList = mcValidIdentifierGroup.elements();
		        if (notNull(mcValidIdentifierList) || !mcValidIdentifierList.isEmpty()) {	
			        Iterator<Element> mcIdentifierListIterator = mcValidIdentifierList.iterator();
			        while(mcIdentifierListIterator.hasNext()){
			        	Element e = mcIdentifierListIterator.next();
			        	validMC.add(e.element("netGenerationMeasuringCompType").getStringValue());      	
			        }
		         }
		     }
			 List<SQLResultRow> generationMeasuringComponentList = retrieveGenerationMeasuringComponent(spId.getIdValue().toString());
			 Iterator<SQLResultRow> generationMeasuringComponentItr = generationMeasuringComponentList.iterator();
			 while(generationMeasuringComponentItr.hasNext()){
				missingMsrmts = false;
				SQLResultRow generationMCRow = (SQLResultRow) generationMeasuringComponentItr.next();
				generationDcId = (DeviceConfiguration_Id) generationMCRow.getId("DEVICE_CONFIG_ID",DeviceConfiguration.class); 
			    generationMcId = (MeasuringComponent_Id) generationMCRow.getId("MEASR_COMP_ID" , MeasuringComponent.class);
			    generationMcType = generationMCRow.getString("MEASR_COMP_TYPE_CD");
			    secondaPerInterval = generationMCRow.getTimeInterval("SEC_PER_INTRVL");
			    installationDttm = generationMCRow.getDateTime("D1_INSTALL_DTTM");
			    removalDttm = generationMCRow.getDateTime("D1_REMOVAL_DTTM");
			    unitOfMeasure = generationMCRow.getString("D1_UOM_CD");
			    //retrieved MC is a valid MC(master config data) for calculating Netting.
			    if(validMC.contains(generationMcType)){
			    	//validConsumptionMcType = validMC.get(generationMcType);
			    	generationMsrmtDttm = periodStartDttm;
					DateInterval numofDays =  periodEndDttm.difference(generationMsrmtDttm);
					Measurement msrmt = null;
					numberOfIntervals = (int)(numofDays.getTotalDays()*24);
					log.info("periodEndDttm: "+periodEndDttm+" periodStartDate: "+periodStartDate);
					log.info("numberOfIntervals "+numberOfIntervals);
					DateTime genInternalIntervals;
					DateTime conInternalIntervals;
					long spi = secondaPerInterval.getAsSeconds();
					int interval = 0;
					//list used for calculating net value
					List<MeasuringComponent> consumptionMCListforDiffPremise = new ArrayList<MeasuringComponent>();
					List<ServicePointD1> consumptionSPListAtDiffPremise = new ArrayList<ServicePointD1>();
					List<MeasuringComponent> consumptionMCListAtSamePremise = new ArrayList<MeasuringComponent>();
					List<ServicePointD1> consumptionSPListAtSamePremise = new ArrayList<ServicePointD1>();
					List<BigDecimal> consumptionMCListQuantityforDiffPremise =  new ArrayList<BigDecimal>();
					List<BigDecimal> conMCListQuantityforSamePremise =  new ArrayList<BigDecimal>();
					List<MeasuringComponent> intervalMcList = generationDcId.getEntity().retrieveMeasuringComponents(IntervalScalarLookup.constants.INTERVAL);
					if(notNull(intervalMcList) && !intervalMcList.isEmpty()){
						intervalMcList.remove(generationMcId.getEntity());
						Iterator<MeasuringComponent> consumptionMCItr = intervalMcList.iterator();
						while(consumptionMCItr.hasNext()){
							MeasuringComponent mc = consumptionMCItr.next();
							UnitOfMeasureD1 uom = mc.retrieveUOM(ValueIdentifierTypeLookup.constants.MEASUREMENT);	
							if(notNull(uom)){
								if(uom.getId().getIdValue().equals(unitOfMeasure)){
									consumptionMCListAtSamePremise.add(mc);
									consumptionSPListAtSamePremise.add(spId.getEntity());
								}
							}
						}
					}
					//scenario 2 child service points with same premise.
					List<SQLResultRow> consumptionPointsforSamePremise = retrieveComsumptionPointsforSamePremise(spId.getIdValue().toString(),generationPremiseId);
					if(notNull(consumptionPointsforSamePremise)){
						Iterator<SQLResultRow> consumptionPointsforSamePremItr = consumptionPointsforSamePremise.iterator(); 
						while(consumptionPointsforSamePremItr.hasNext()){
							SQLResultRow consumptionPointDCforSamePrem = consumptionPointsforSamePremItr.next();
							DeviceConfiguration deviceCfg = consumptionPointDCforSamePrem.getEntity("DEVICE_CONFIG_ID",DeviceConfiguration.class);
							ServicePointD1 sp = consumptionPointDCforSamePrem.getEntity("D1_SP_ID",ServicePointD1.class);
							List<MeasuringComponent> intervalMcListforSamePremise = deviceCfg.retrieveMeasuringComponents(IntervalScalarLookup.constants.INTERVAL);
							if(notNull(intervalMcListforSamePremise)){
								Iterator<MeasuringComponent> intervalMcListforSamePremItr = intervalMcListforSamePremise.iterator();
								while(intervalMcListforSamePremItr.hasNext()){
									MeasuringComponent mc = intervalMcListforSamePremItr.next();
									if(notNull(mc)){
										UnitOfMeasureD1 uom = mc.retrieveUOM(ValueIdentifierTypeLookup.constants.MEASUREMENT);
										if(notNull(uom)){
											if(uom.getId().getIdValue().equals(unitOfMeasure)){
												consumptionMCListAtSamePremise.add(mc);
												consumptionSPListAtSamePremise.add(sp);
											}
										}	
									}
								}
							}	
						}
					}	
					//scenario 3 different service points with different premise.
					List<SQLResultRow> consumptionPoints = retrieveConsumptionPoints(spId.getIdValue().toString(),generationPremiseId);	
					if(notNull(consumptionPoints)){
						Iterator<SQLResultRow> consumptionPointsItr = consumptionPoints.iterator(); 
						while(consumptionPointsItr.hasNext()){
							SQLResultRow consumptionPointDC = consumptionPointsItr.next();
							DeviceConfiguration deviceCfg = consumptionPointDC.getEntity("DEVICE_CONFIG_ID",DeviceConfiguration.class);
							ServicePointD1 sp = consumptionPointDC.getEntity("D1_SP_ID",ServicePointD1.class);
							List<MeasuringComponent> intervalMcListforDiffPremise = deviceCfg.retrieveMeasuringComponents(IntervalScalarLookup.constants.INTERVAL);
							if(notNull(intervalMcListforDiffPremise) && !intervalMcListforDiffPremise.isEmpty()){
								Iterator<MeasuringComponent> intervalMcListforDiffPremItr = intervalMcListforDiffPremise.iterator();
								while(intervalMcListforDiffPremItr.hasNext()){
									MeasuringComponent mc = intervalMcListforDiffPremItr.next();
									if(notNull(mc)){
										UnitOfMeasureD1 uom = mc.retrieveUOM(ValueIdentifierTypeLookup.constants.MEASUREMENT);
										if(notNull(uom)){
											if(uom.getId().getIdValue().equals(unitOfMeasure)){
												consumptionMCListforDiffPremise.add(mc);
												consumptionSPListAtDiffPremise.add(sp);
											}
										}	
									}
								}
							}	
						}
					}	
					if(spi == 900){
						 interval = 15; 
					}
					else if(spi == 1800){
						 interval = 30;
					}
					else if(spi == 3600){
						 interval = 60;
					}
					for(int i=1;i<=numberOfIntervals; i++){
						hourlyGeneration =BigDecimal.ZERO;
						totalHourlyConsumption = BigDecimal.ZERO;
						hourlyConsumption = BigDecimal.ZERO;		
						missingMsrmts = false;
						consumptionMCListQuantityforDiffPremise.clear();
						conMCListQuantityforSamePremise.clear();
						genInternalIntervals = generationMsrmtDttm;
						conInternalIntervals = generationMsrmtDttm;
						generationMsrmtDttm = generationMsrmtDttm.addHours(1);
						genInternalIntervals = genInternalIntervals.addMinutes(interval);
						while(genInternalIntervals.isSameOrBefore(generationMsrmtDttm)){
							msrmt = new Measurement_Id(generationMcId,genInternalIntervals).getEntity();
							if(notNull(msrmt)){
								hourlyGeneration =  hourlyGeneration.add(msrmt.getMeasurementValue());
							}
							else{
								missingMsrmts = true;
								missingMeasurements.add(generationMcId.getIdValue()+"/"+genInternalIntervals.toString());
								break;
							}
							genInternalIntervals = genInternalIntervals.addMinutes(interval);
						}
						if(missingMsrmts.equals(false)){
							int conInterval = 0;
							List<MeasuringComponent_Id> tempMCListAtSamePremise = new ArrayList<MeasuringComponent_Id>();
							List<ServicePointD1> tempSPListAtSamePremise = new ArrayList<ServicePointD1>();
							DateTime tempDttm = conInternalIntervals;
							for(int mcCount=0;mcCount<consumptionMCListAtSamePremise.size();mcCount++){
								hourlyConsumption = BigDecimal.ZERO;
								conInternalIntervals = tempDttm;
								MeasuringComponent conMcId = consumptionMCListAtSamePremise.get(mcCount);
								Long conSpi = conMcId.getMeasuringComponentType().getSecondsPerInterval().getAsSeconds();
								if(conSpi == 900){
									conInterval = 15; 
								}
								else if(conSpi == 1800){
									conInterval = 30;
								}
								else if(conSpi == 3600){
									conInterval = 60;
								}
								conInternalIntervals = conInternalIntervals.addMinutes(conInterval);
								
								while(conInternalIntervals.isSameOrBefore(generationMsrmtDttm)){
									msrmt = new Measurement_Id(conMcId,conInternalIntervals).getEntity();
									if(notNull(msrmt)){
										missingMsrmts = false;
										hourlyConsumption =  hourlyConsumption.add(msrmt.getMeasurementValue());
									}
									else{
										missingMsrmts = true;
										missingMeasurements.add(conMcId.getId().getIdValue()+"/"+conInternalIntervals.toString());
										break;
									}
									conInternalIntervals = conInternalIntervals.addMinutes(conInterval);
								}
								if(missingMsrmts.equals(false)){
									tempMCListAtSamePremise.add(conMcId.getId());
									tempSPListAtSamePremise.add(consumptionSPListAtSamePremise.get(mcCount));
									conMCListQuantityforSamePremise.add(hourlyConsumption); 
								}
								else{
									break;
								}
								
							}
							//retrieving quantity for MC on consumption point
							List<MeasuringComponent_Id> tempMCListforDiffPremise = new ArrayList<MeasuringComponent_Id>();
							List<ServicePointD1> tempSPListForDiffPremise = new ArrayList<ServicePointD1>();
							if(missingMsrmts.equals(false)){
								for(int mcCount=0;mcCount<consumptionMCListforDiffPremise.size();mcCount++){
									hourlyConsumption = BigDecimal.ZERO;
									conInternalIntervals = tempDttm;
									MeasuringComponent conMcId = consumptionMCListforDiffPremise.get(mcCount);
									Long conSpi = conMcId.getMeasuringComponentType().getSecondsPerInterval().getAsSeconds();
									if(conSpi == 900){
										conInterval = 15; 
									}
									else if(conSpi == 1800){
										conInterval = 30;
									}
									else if(conSpi == 3600){
										conInterval = 60;
									}
									conInternalIntervals = conInternalIntervals.addMinutes(conInterval);
									while(conInternalIntervals.isSameOrBefore(generationMsrmtDttm)){
										msrmt = new Measurement_Id(conMcId,conInternalIntervals).getEntity();
										if(notNull(msrmt)){
											missingMsrmts = false;
											hourlyConsumption =  hourlyConsumption.add(msrmt.getMeasurementValue());
										}
										else{
											missingMsrmts = true;
											missingMeasurements.add(conMcId.getId().getIdValue()+"/"+conInternalIntervals.toString());
											break;
										}
										conInternalIntervals = conInternalIntervals.addMinutes(conInterval);
									}
									if(missingMsrmts.equals(false)){
										tempMCListforDiffPremise.add(conMcId.getId());
										tempSPListForDiffPremise.add(consumptionSPListAtDiffPremise.get(mcCount));
										consumptionMCListQuantityforDiffPremise.add(hourlyConsumption); 
									}
									else{
										break;
									}
								}
							}	
							//calculating net consumption for same SP.
							netConsumptionWithoutUS = BigDecimal.ZERO;
							hourlyConsumption = BigDecimal.ZERO;
							hourlyGenerationAftConsumption = hourlyGeneration;
							if(conMCListQuantityforSamePremise.size()>0){
								if(tempMCListAtSamePremise.size()>0){
									for(int j=0;j<tempMCListAtSamePremise.size();j++){
										//netConsumption = BigDecimal.ZERO;
										MeasuringComponent_Id mc = tempMCListAtSamePremise.get(j);
										ServicePointD1 sp = tempSPListAtSamePremise.get(j);
										List<UsageSubscription> usList = sp.retrieveActiveUsageSubscriptionsAsOfDateTime(periodEndDttm);
										if(usList.size()>0){
											usageSubscriptionExits = true;
										}
										else{
											usageSubscriptionExits = false;
										}
										hourlyConsumption = conMCListQuantityforSamePremise.get(j);
										totalHourlyConsumption = totalHourlyConsumption.add(hourlyConsumption);
										netConsumption = hourlyConsumption.subtract(hourlyGenerationAftConsumption);
										if(netConsumption.compareTo(BigDecimal.ZERO)<0){
											netConsumption = BigDecimal.ZERO;
										}	
										if(usageSubscriptionExits.equals(true) ){
											msrmt = new Measurement_Id(mc,generationMsrmtDttm).getEntity();
											Measurement_DTO measrDTO = msrmt.getDTO();
											measrDTO.setMeasurementValue2(netConsumption);
											msrmt.setDTO(measrDTO);  
										}	
										else{
											netConsumptionWithoutUS = netConsumption.add(netConsumptionWithoutUS) ;
										}	
										hourlyGenerationAftConsumption = hourlyGenerationAftConsumption.subtract(hourlyConsumption); 
										if(hourlyGenerationAftConsumption.compareTo(BigDecimal.ZERO)<0){
											hourlyGenerationAftConsumption = BigDecimal.ZERO;
										}
									}
									// setting net generation value.
									msrmt = new Measurement_Id(generationMcId,generationMsrmtDttm).getEntity();
									Measurement_DTO measrDTO = msrmt.getDTO();
									measrDTO.setMeasurementValue1(hourlyGenerationAftConsumption);
									msrmt.setDTO(measrDTO);  
								}
								else{
									missingMsrmts = true;
								}	
							}
							//calculating net consumption for child SP's
							Boolean usExits = usageSubscriptionExits;
							if(missingMsrmts.equals(false)){	
								if(consumptionMCListforDiffPremise.size()>0){
									if(tempMCListforDiffPremise.size()>0){
										for(int j=0;j<tempMCListforDiffPremise.size();j++){
											netConsumption = BigDecimal.ZERO;
											hourlyConsumption = BigDecimal.ZERO;
											MeasuringComponent_Id mc = tempMCListforDiffPremise.get(j);
											hourlyConsumption = consumptionMCListQuantityforDiffPremise.get(j);
											totalHourlyConsumption = totalHourlyConsumption.add(hourlyConsumption);
											netConsumption = hourlyConsumption.subtract(hourlyGenerationAftConsumption);
											if(netConsumption.compareTo(BigDecimal.ZERO)<0){
												netConsumption = BigDecimal.ZERO;
											}
											if(usExits.equals(true)){
												msrmt = new Measurement_Id(mc,generationMsrmtDttm).getEntity();
												Measurement_DTO measrDTO = msrmt.getDTO();
												measrDTO.setMeasurementValue2(netConsumption);
												msrmt.setDTO(measrDTO);
											}
											else{
												msrmt = new Measurement_Id(mc,generationMsrmtDttm).getEntity();
												Measurement_DTO measrDTO = msrmt.getDTO();
												measrDTO.setMeasurementValue2(netConsumption.add(netConsumptionWithoutUS));
			 									msrmt.setDTO(measrDTO);
												usExits = true;
											}
											hourlyGenerationAftConsumption = hourlyGenerationAftConsumption.subtract(hourlyConsumption); 
											if(hourlyGenerationAftConsumption.compareTo(BigDecimal.ZERO)<0){
												hourlyGenerationAftConsumption = BigDecimal.ZERO;
											}
										}	
									}	
								}
							}
							netGeneration = hourlyGeneration.subtract(totalHourlyConsumption);
							if(netGeneration.compareTo(BigDecimal.ZERO)<0){
								netGeneration = BigDecimal.ZERO;
							}
							Measurement genmsrmt = new Measurement_Id(generationMcId,generationMsrmtDttm).getEntity();
							Measurement_DTO measrDTO = genmsrmt.getDTO();
							measrDTO.setMeasurementValue2(netGeneration);
							genmsrmt.setDTO(measrDTO);
						}	
					}	
			    }
			} 
			if(toDoRequired.equals("Y")){ 
				if(missingMeasurements.size() >0){
			    	String mc  = "";
			    	String tempmc = "";
			    	new StringBuffer();
			    	String startdate = null;
			    	String enddate = null; 
			    	Collections.sort(missingMeasurements);
			    	//log.info("missingMeasurements "+missingMeasurements);
			    	tempmc =  missingMeasurements.get(0).split("/")[0];
			    	startdate = missingMeasurements.get(0).split("/")[1];
			    	Iterator<String> itr = missingMeasurements.iterator();
			    	while(itr.hasNext()){
			    		String missingMcDttm = itr.next();
			    		String missingMsrmt[] = missingMcDttm.split("/");
			    		mc = missingMsrmt[0];
			    		//log.info("tempmc: "+tempmc+" mc: "+mc);
			    		if(tempmc.equals(mc)){
			    			//msrmtDttm.append(missingMsrmt[1]+"\n");
			    			enddate = missingMsrmt[1];
			    		}
			    		else {
			    			createToDo(tempmc,startdate+" to "+enddate);
			    			tempmc = mc;
			    			startdate = missingMsrmt[1];
			    			enddate = startdate;
			    		}
			    	}
			    	if(missingMeasurements.size()>0){
			    		createToDo(mc,startdate+" to "+enddate);
			    	}
			    	missingMeasurements.clear();
				}	
			}	
		}
		private Measurement calculateNetConsumption(MeasuringComponent_Id mcId, DateTime msrmtDttm){		
			 Measurement_Id msrmtId = new Measurement_Id(mcId,msrmtDttm);
			 Measurement msrmt = msrmtId.getEntity();
			 if(notNull(msrmt)){
				    missingMsrmts = false;
					hourlyConsumption = msrmt.getMeasurementValue();
					totalHourlyConsumption = totalHourlyConsumption.add(hourlyConsumption);
					netConsumption = hourlyConsumption.subtract(hourlyGenerationAftConsumption);
					if(netConsumption.compareTo(BigDecimal.ZERO)<0){
						netConsumption = BigDecimal.ZERO;
					}				
					hourlyGenerationAftConsumption = hourlyGenerationAftConsumption.subtract(hourlyConsumption); 
					if(hourlyGenerationAftConsumption.compareTo(BigDecimal.ZERO)<0){
						hourlyGenerationAftConsumption = BigDecimal.ZERO;
					}
			 } 
			 else{
				 missingMsrmts = true;
				 missingMeasurements.add(mcId.getIdValue()+"/"+msrmtDttm.toString());	
			 }
			 return msrmt;	      
		}
		private List<SQLResultRow> retrieveGenerationMeasuringComponent(String spId)
		{  log.debug("inside retrieveGeneratedQuantity");
			//need to add date constrains to this query
			StringBuilder query = new StringBuilder();
			query.append("select mc.DEVICE_CONFIG_ID,instEvnt.D1_INSTALL_DTTM, instEvnt.D1_removal_DTTM , mc.MEASR_COMP_ID, mc.MEASR_COMP_TYPE_CD, mcType.SEC_PER_INTRVL,mcTypeIdentifier.D1_UOM_CD ");
			query.append(" from D1_INSTALL_EVT instEvnt, D1_MEASR_COMP mc, D1_MC_TYPE_VALUE_IDENTIFIER mcTypeIdentifier,D1_MEASR_COMP_TYPE mcType ");
			query.append(" where instEvnt.D1_SP_ID ='"+spId+"'");
			query.append(" and mc.DEVICE_CONFIG_ID = instEvnt.DEVICE_CONFIG_ID");
			query.append(" and mc.measr_comp_type_cd = mcType.measr_comp_type_cd");
			query.append(" and mcType.measr_comp_type_cd = mcTypeIdentifier.measr_comp_type_cd");
			query.append(" and mcTypeIdentifier.D1_SQI_CD = 'GEN'");
		    PreparedStatement resultQuery = createPreparedStatement(query.toString(),"Hourly generated ");
		    return resultQuery.list();
		  
		}   
		private List<SQLResultRow> retrieveComsumptionPointsforSamePremise(String spId, String premiseId) 
		{log.debug("inside retrieveComsumptionPoints");
			//need to add date constrains to this query
			StringBuilder query = new StringBuilder();
			query.append("select instEvnt.Device_config_ID,instEvnt.D1_SP_ID  ");  
			query.append(" from  D1_SP_CHAR spChar, D1_INSTALL_EVT instEvnt ,D1_SP_IDENTIFIER spIdentifier");
			query.append(" where spChar.D1_SP_ID = '"+spId+"'");
			query.append(" and spChar.CHAR_TYPE_CD = 'CM-CONPT'");
			query.append(" and spChar.CHAR_VAL_FK1 = instEvnt.D1_SP_ID");
			query.append(" and spIdentifier.D1_SP_ID = spChar.CHAR_VAL_FK1 ");
			query.append(" and spIdentifier.SP_ID_TYPE_FLG = 'D1EP'");
			query.append(" and spIdentifier.ID_VALUE  = :premiseId");
										
		    PreparedStatement resultQuery = createPreparedStatement(query.toString(),"Hourly consumed ");
		    resultQuery.bindString("premiseId", premiseId, "");
		    return resultQuery.list();
		}
		private List<SQLResultRow> retrieveConsumptionPoints(String spId, String premiseId) 
		{   //need to add date constrains to this query
			log.info("spId "+spId+" premiseId "+premiseId);
			StringBuilder query = new StringBuilder();
			query.append("select spIdentifier1.ID_VALUE ,instEvnt.Device_config_ID,instEvnt.D1_SP_ID  ");  
			query.append(" from D1_SP_CHAR spChar, D1_INSTALL_EVT instEvnt ,D1_SP_IDENTIFIER spIdentifier1, D1_SP_IDENTIFIER spIdentifier2,ci_sa_sp sasp, ci_sa sa,ci_acct acct ");
			query.append(" where spChar.D1_SP_ID = :spId");
			query.append(" and spChar.CHAR_TYPE_CD = 'CM-CONPT'");
			query.append(" and spChar.CHAR_VAL_FK1 = instEvnt.D1_SP_ID");
			query.append(" and spIdentifier1.D1_SP_ID = spChar.CHAR_VAL_FK1 ");
			query.append(" and spIdentifier1.SP_ID_TYPE_FLG = 'D1EI'");
			query.append(" and spIdentifier2.D1_SP_ID = spChar.CHAR_VAL_FK1 ");
			query.append(" and spIdentifier2.SP_ID_TYPE_FLG = 'D1EP'");
			query.append(" and spIdentifier2.ID_VALUE  !=:premiseId");
			query.append(" and spIdentifier2.D1_SP_ID = spIdentifier1.D1_SP_ID ");
			query.append(" and sasp.SP_ID = spIdentifier1.ID_VALUE ");
			query.append(" and sasp.START_DTTM = ( select max(sasp1.START_DTTM)");
									query.append(" from ci_sa_sp sasp1 ,ci_sa sa1, ci_sa_type saType1");
									query.append(" where sasp1.sp_id = spIdentifier1.ID_VALUE");
									query.append(" and NVL(sasp1.STOP_DTTM,:periodStartDttm) >= :periodStartDttm");
									query.append(" and sasp1.START_DTTM <= :periodEndDttm ");
									query.append(" and sasp1.sa_id = sa1.sa_id ");
									query.append(" and sa1.SA_STATUS_FLG='20' ");
									query.append(" and saType1.SA_TYPE_CD= sa1.SA_TYPE_CD ");
									query.append(" and saType1.SVC_TYPE_CD ='MS') ");
			query.append(" and sasp.sa_id = sa.sa_id "); 
			query.append(" and sa.START_DT= sasp.START_DTTM ");
			query.append(" and acct.acct_id = sa.acct_id ");
			query.append(" order by case acct.CUST_CL_CD ");
			query.append(" when 'I' then 1 ");
			query.append(" when 'C' then 2 ");
			query.append(" when 'A' then 3 ");
			query.append(" when 'R' then 4 ");
			query.append(" when 'L' then 5 ");
			query.append(" else 6 end , sa.START_DT");
											
		    PreparedStatement resultQuery = createPreparedStatement(query.toString(),"Hourly consumed ");
		    resultQuery.bindString("premiseId", premiseId, "");
		    resultQuery.bindString("spId", spId, "");
		    resultQuery.bindDateTime("periodStartDttm", periodStartDttm);
		    resultQuery.bindDateTime("periodEndDttm", periodEndDttm);
		    return resultQuery.list();
		    
		}
		
		private void createToDo(String measuringComponent, String msrmtDttm) {
		 	try{
		 		log.info("inside to do");
		 		log.info("measuringComponent "+measuringComponent);
		 		log.info("batch application process "+getParameters().getBatchControlId().getEntity().getApplicationService());
 		 		MessageCategory_Id msgCatId = new MessageCategory_Id(BigInteger.valueOf(mdgCId));
	            Message_Id msgId =  new Message_Id(msgCatId,BigInteger.valueOf(msgID));
	           // ToDoEntry_Impl todo = new ToDoEntry_Impl();
	            if (!isTodoEntryExist(measuringComponent,msgId, toDoType)) {
	            	log.info("no existing todo ");
		        	ToDoCreator toDoCreator = ToDoCreator.Factory.newInstance();
		        	log.info("after 1 "+toDoCreator);
			        ToDoEntry_DTO toDoEntryDto = (ToDoEntry_DTO)createDTO(ToDoEntry.class);
			    	log.info("after 2"+toDoEntryDto);
		            toDoEntryDto.setToDoEntryStatus(ToDoEntryStatusLookup.constants.OPEN);
		            log.info(" after entry sa "+toDoEntryDto);
		            toDoEntryDto.setToDoRoleId(toDoRole.getId());
		            log.info("after toDoEntryDto.setToDoRoleId(toDoRole.getId()) "+toDoEntryDto);
                    log.info("toDoType.getId() "+toDoType.getId());
		            toDoEntryDto.setToDoTypeId(toDoType.getId());
		            log.info(" setToDoTypeId "+toDoEntryDto);
		            log.info("msrmtDttm "+msrmtDttm);
		            toDoEntryDto.setComments(msrmtDttm);
		            log.info("msgId "+msgId);
		            log.info(" setComments "+toDoEntryDto);
		            toDoEntryDto.setMessageId(msgId);
		            log.info("toDoEntryDto "+toDoEntryDto);
		            log.info(" setMessageId "+toDoEntryDto);
		            log.info("measuringComponent "+measuringComponent);
		            log.info(" setToDoDTO "+toDoEntryDto);
		            toDoCreator.addMessageParameter(measuringComponent);
		            log.info("befor drill key ");

		            //ToDoDrillKeyType_Id dkey = new ToDoDrillKeyType_Id(toDoType, BigInteger.ZERO);
		           
		            //Add the drill key
		            log.info("applicaton process"+toDoType.fetchCreationProcess().getApplicationService());
		            try{
		            log.info(" drill key size "+toDoType.getDrillKeyTypes().size());
		            if (toDoType.getDrillKeyTypes().size()>0) {   
						Iterator<ToDoDrillKeyType> toDoDrillKeyTypeIterator = toDoType.getDrillKeyTypes().iterator();
						log.info("toDoDrillKeyTypeIterator "+toDoDrillKeyTypeIterator);
						while (toDoDrillKeyTypeIterator.hasNext()) {
							log.info("inside while ");
							 ToDoDrillKeyType drillKeyType = toDoDrillKeyTypeIterator.next();
							 log.info("drillKeyType  "+drillKeyType.getField().getId().getIdValue());
							 if(drillKeyType.getField().getId().getIdValue().trim().equalsIgnoreCase("MEASR_COMP_ID")){
								// ToDoDrillKeyValue_Id id =  new ToDoDrillKeyValue_Id(toDo, sequence)
				                log.info("drillKeyType  "+drillKeyType);
				                log.info("drillKeyType  "+drillKeyType.getField().getId().getIdValue());
				               
				                toDoCreator.addDrillKeyValue(drillKeyType, measuringComponent);
				                log.info("toDoCreator inside drillKey  "+toDoCreator.getToDoDTO());
				                break;
							 }   
						}
				     }
		            }catch(Exception e){
		            	e.printStackTrace();
		            }
		
		            /*
			         * Set the To Do Sort Key Fields
			         */
		            try{
			            log.info("before sort key ");
				        if (toDoType.getSortKeyTypes().size()>0) {
				        	log.info("inside sort key " +toDoType.getSortKeyTypes().isEmpty());
				        	toDoCreator.addSortKeyValue(toDoType.getSortKeyTypes().iterator().next(), measuringComponent);  
				        }	
		            }
			        catch(Exception e){
			        	e.printStackTrace();
			        }
			        log.info("after sort key ");
		            //addToDoDrillKey(toDoCreator,toDoType,measuringComponent);
		            //Add a sort key
		            // addToDoSortKey(toDoCreator,toDoType,measuringComponent);
			        try{
			        toDoCreator.setToDoDTO(toDoEntryDto);
		            log.info("toDoCreator  "+toDoCreator);
		            ToDoEntry todo =  toDoCreator.create(); 
		            log.info("todo id "+todo.getId());
		            createServerMessage(msgCatId,msgId,todo,measuringComponent);
		            log.info("after server mesg creation  ");
			        }catch(Exception e){
			        	e.printStackTrace();
			        }
		           
	            }  
	       } 
		 	catch(Exception e){
		 		e.printStackTrace();
		 	}
	  }
	private void addToDoDrillKey(ToDoCreator toDoCreator, ToDoType toDoType, String mc) {
	        //Add a drill Key 
		try{
			
	        ToDoTypeDrillKeyTypes drillKeyTypes = toDoType.getDrillKeyTypes();
	        log.info("drillKeyTypes "+drillKeyTypes);
	        if (drillKeyTypes != null) {
	        	log.info("inside not null drillKeyTypes "+drillKeyTypes);
	        	Iterator<ToDoDrillKeyType> iterator = drillKeyTypes.iterator();
	            while(iterator.hasNext()) {
	                ToDoDrillKeyType drillKeyType = iterator.next();
	                log.info("drillKeyType  "+drillKeyType);
	                toDoCreator.addDrillKeyValue(drillKeyType, mc);
	                log.info("toDoCreator inside sortkeys  "+toDoCreator);
	                break;
	            }
	        }
		}catch(Exception e){
			e.printStackTrace();
		}
	  }
	 /**
     * This method is used to add a Sort Key to the ToDo
     * @param transBO
     * @param toDoCreator
     * @param toDoType 
     * @param toDoType
     */
    private void addToDoSortKey(ToDoCreator toDoCreator, ToDoType toDoType,String mc) {
        //Add a Sort Key
        ToDoTypeSortKeyTypes sortKeyTypes = toDoType.getSortKeyTypes();
        log.info("sortKeyTypes "+sortKeyTypes);
        if (sortKeyTypes != null) {
        	log.info("inside not null sortKeyTypes "+sortKeyTypes);
        	Iterator<ToDoSortKeyType> iterator = sortKeyTypes.iterator();
        	 
            while (iterator.hasNext()) {
                ToDoSortKeyType sortKeyType = iterator.next();
                log.info("sortKeyType  "+sortKeyType);
                toDoCreator.addSortKeyValue(sortKeyType, mc);
                log.info("toDoCreator inside sortkeys  "+toDoCreator);
                break;
            }
        }
    }
	 private boolean isTodoEntryExist(String mcId, Message_Id msgId, ToDoType toDoType) {
		 try{
	        StringBuffer queryString = new StringBuffer();
	        queryString.append("SELECT DRLKEY.TD_ENTRY_ID ");
	        queryString.append("FROM CI_TD_ENTRY TODO, CI_TD_DRLKEY DRLKEY ");
	        queryString.append("WHERE TODO.TD_ENTRY_ID = DRLKEY.TD_ENTRY_ID ");
	        queryString.append("AND TODO.ENTRY_STATUS_FLG != 'C'  ");
	        queryString.append("AND DRLKEY.KEY_VALUE =:mcId ");
	        queryString.append("AND TODO.MESSAGE_CAT_NBR =:msgCatNbr ");
	        queryString.append("AND TODO.MESSAGE_NBR =:msgNum ");
	        queryString.append("AND TODO.TD_TYPE_CD =:toDoType ");

	        PreparedStatement statement = createPreparedStatement(queryString.toString(),"calNetConsumptionLogic");
	        statement.bindStringProperty("mcId", ToDoDrillKeyValue.properties.keyValue, mcId);
	        statement.bindBigInteger("msgCatNbr", msgId.getMessageCategoryId().getIdValue());
	        statement.bindBigInteger("msgNum", msgId.getMessageNumber());
	        statement.bindId("toDoType", toDoType.getId());
	        log.info(" statement.list().size() "+statement.list().size()); 
	        for (SQLResultRow row : statement.list()) {
	            String tdId = row.getString("TD_ENTRY_ID");
	            log.info("tdId "+tdId);
	            if (!isBlankOrNull(tdId)) {
	            	log.info("checking if todo already exits. Yes");
	                return Boolean.TRUE;
	            }
	        }
           
	        statement.close();
		 }catch(Exception e){
			 e.printStackTrace();
		 }
	        return Boolean.FALSE;
	    }
		  
	 /**
	     * Creates ServerMessage by taking Message Category Id and Message Id
	     * @param msgCatId
	     * @param msgId
	     * @param toDoEntry 
	     */
	    private void createServerMessage(MessageCategory_Id msgCatId, Message_Id msgId, ToDoEntry toDoEntry,String mc) {
	    	try{
	        if (notNull(toDoEntry)) {
	        	MeasuringComponent_Id mcId = new MeasuringComponent_Id(mc);
	            ServerMessageFactory serverMessageFactory = ServerMessageFactory.Factory.newInstance();
	            MessageParameters messageParameters = new MessageParameters();
	            messageParameters.addEntityIdValues(mcId);
	            ServerMessage serverMessage = serverMessageFactory.createMessage(msgCatId, msgId.getMessageNumber().intValue(), messageParameters);
	            log.info("serverMessage "+serverMessage);
	            getCharacteristicType(serverMessage, toDoEntry,mcId.getEntity());
	        }
	    	}catch(Exception e){
	    		e.printStackTrace();
	    	}
	    }
	    /**
	     * Creates CharacteristicType 
	     * @param message
	     * @param toDoEntry 
	     */
	    private void getCharacteristicType(ServerMessage message, ToDoEntry toDoEntry,MeasuringComponent mc) {
	    	try{
		        CharacteristicType_Id charId = new CharacteristicType_Id(TODO_CHARACTERISTIC_TYPE);
		        CharacteristicType charType = charId.getEntity();
		        if (notNull(charType)) {
		            createLogEntry(message,charType,toDoEntry, mc);
		            log.info("charType "+charType);
		        }
	    	}catch(Exception e){
	    		e.printStackTrace();
	    	}
	    }
	    /**
	     * Creates a ToDo Log Entry by taking Server message and Characteristic Type
	     * @param message
	     * @param charType
	     * @param toDoEntry 
	     */
	    private void createLogEntry(ServerMessage message, CharacteristicType charType,ToDoEntry toDoEntry, MeasuringComponent measuringComponent  ) {
	    	try{
		        BusinessObject_Id boId = measuringComponent.getBusinessObject().getId();
		        MaintenanceObject mo = measuringComponent.getBusinessObject().getMaintenanceObject();
		        //Add the MO Log
		        MaintenanceObjectLogHelper moHelper = new MaintenanceObjectLogHelper<MeasuringComponent, MeasuringComponentLog>(
		                mo, measuringComponent, boId);
		        moHelper.addLogEntry(LogEntryTypeLookup.constants.TO_DO, message, null,charType, toDoEntry);
		       
		        log.info("moHelper.toString() "+moHelper.toString());
	    	}catch(Exception e){
	    		e.printStackTrace();
	    	}
	        
	    }

	}
		
	

}