package com.splwg.cm.domain.admin.usageRule.applyUsageRule;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dom4j.Element;
import org.dom4j.tree.DefaultElement;

import com.ibm.icu.math.BigDecimal;
import com.ibm.icu.math.MathContext;
import com.splwg.base.api.Query;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.businessObject.SchemaInstance;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.LookupHelper;
import com.splwg.base.api.datatypes.TimeInterval;
import com.splwg.base.api.installation.InstallationHelper;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.StandardMessages;
import com.splwg.base.domain.common.businessObject.BusinessObject;
import com.splwg.base.domain.common.businessObject.BusinessObject_Id;
import com.splwg.base.domain.common.timeZone.TimeZone;
import com.splwg.base.domain.common.timeZone.TimeZone_Id;
import com.splwg.cm.api.lookup.BillTypeLookup;
import com.splwg.d1.api.lookup.BillConditionD1Lookup;
import com.splwg.d1.api.lookup.DataQualityAssessmentLookup;
import com.splwg.d1.api.lookup.DeviceIdentifierTypeLookup;
import com.splwg.d1.api.lookup.IntervalScalarLookup;
import com.splwg.d1.api.lookup.MeasuringComponentUsageLookup;
import com.splwg.d1.api.lookup.UsageD1Lookup;
import com.splwg.d1.api.lookup.UsageServiceQuantityTypeLookup;
import com.splwg.d1.api.lookup.UseMeasurementLookup;
import com.splwg.d1.domain.admin.measuringComponentType.entities.MeasuringComponentType;
import com.splwg.d1.domain.admin.measuringComponentType.entities.MeasuringComponentTypeValueIdentifier;
import com.splwg.d1.domain.admin.serviceQuantityIdentifier.entities.ServiceQuantityIdentifierD1;
import com.splwg.d1.domain.admin.serviceQuantityIdentifier.entities.ServiceQuantityIdentifierD1_Id;
import com.splwg.d1.domain.admin.timeOfUse.entities.TimeOfUseD1;
import com.splwg.d1.domain.admin.timeOfUse.entities.TimeOfUseD1_Id;
import com.splwg.d1.domain.admin.timeOfUseMapTemplate.entities.TimeOfUseMapTemplateD1_Id;
import com.splwg.d1.domain.admin.unitOfMeasure.entities.UnitOfMeasureD1;
import com.splwg.d1.domain.admin.unitOfMeasure.entities.UnitOfMeasureD1_Id;
import com.splwg.d1.domain.admin.usageGroup.entities.UsageGroup;
import com.splwg.d1.domain.admin.usageRule.applyUsageRule.ApplyUsageRuleAlgorithmInputData;
import com.splwg.d1.domain.admin.usageRule.applyUsageRule.ApplyUsageRuleAlgorithmInputOutputData;
import com.splwg.d1.domain.admin.usageRule.applyUsageRule.ApplyUsageRuleAlgorithmSpot;
import com.splwg.d1.domain.admin.usageRule.entities.UsageRule;
import com.splwg.d1.domain.common.routines.RoundingHelper;
import com.splwg.d1.domain.deviceManagement.device.entities.Device;
import com.splwg.d1.domain.deviceManagement.device.entities.DeviceIdentifier;
import com.splwg.d1.domain.deviceManagement.device.entities.DeviceIdentifiers;
import com.splwg.d1.domain.deviceManagement.deviceConfiguration.entities.DeviceConfiguration;
import com.splwg.d1.domain.deviceManagement.deviceConfiguration.entities.DeviceConfiguration_Id;
import com.splwg.d1.domain.deviceManagement.measuringComponent.entities.MeasuringComponent;
import com.splwg.d1.domain.deviceManagement.measuringComponent.entities.MeasuringComponent_Id;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointD1;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointD1_Id;
import com.splwg.d1.domain.measurement.measurement.entities.Measurement;
import com.splwg.d1.domain.measurement.measurement.entities.Measurement_Id;
import com.splwg.d1.domain.measurement.measurementServices.common.ConsumptionRetrieverInputData;
import com.splwg.d1.domain.measurement.measurementServices.intervalConsumptionAndInstallationHistoryRetriever.IntervalConsumptionAndInstallationHistoryRetriever;
import com.splwg.d1.domain.measurement.measurementServices.intervalConsumptionAndInstallationHistoryRetriever.IntervalConsumptionAndInstallationHistoryRetrieverInputData;
import com.splwg.d1.domain.measurement.measurementServices.intervalConsumptionAndInstallationHistoryRetriever.IntervalConsumptionAndInstallationHistoryRetrieverOutputData;
import com.splwg.d1.domain.measurement.measurementServices.intervalConsumptionAndInstallationHistoryRetriever.IntervalConsumptionData;
import com.splwg.d1.domain.usage.timeOfUseMap.entities.TimeOfUseMapD1;
import com.splwg.d1.domain.usage.timeOfUseMap.entities.TimeOfUseMapD1_Id;
import com.splwg.d1.domain.usage.usageSubscription.data.DetermineSubscriptionDvcCfgPeriodInputData;
import com.splwg.d1.domain.usage.usageSubscription.data.DetermineSubscriptionDvcCfgPeriodOutputData;
import com.splwg.d1.domain.usage.usageSubscription.entities.UsageSubscription;
import com.splwg.d1.domain.usage.usageTransaction.entities.UsageTransaction_Id;
import com.splwg.d2.api.lookup.AdjustUsagePeriodEndDateTimeLookup;
import com.splwg.d2.api.lookup.AllowEstimateLookup;
import com.splwg.d2.api.lookup.BillConditionD2Lookup;
import com.splwg.d2.api.lookup.IsEstimateLookup;
import com.splwg.d2.api.lookup.UsageTypeD2Lookup;
import com.splwg.d2.api.lookup.UseLatestEndReadLookup;
import com.splwg.d2.domain.admin.MessageRepository;
import com.splwg.d2.domain.admin.usageRule.data.PopulateMeasurementTempInputData;
import com.splwg.d2.domain.admin.usageRule.data.PopulateMsrmtTempInputMsrmtData;
import com.splwg.d2.domain.admin.usageRule.routines.PopulateMeasurementTemp;
import com.splwg.d2.domain.admin.usageRule.routines.UsageRulesHelper;
import com.splwg.shared.common.ServerMessage;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 *
 *@AlgorithmComponent (softParameters = { @AlgorithmSoftParameter (name = regularBottomRangeCondition, type = integer)
 *            , @AlgorithmSoftParameter (name = regularTopRangeCondition, type = integer)
 *            , @AlgorithmSoftParameter (name = estimateBottomRangeCondition,  type = integer)
 *            , @AlgorithmSoftParameter (name = estimateTopRangeConditionRange, type = integer)
 *            , @AlgorithmSoftParameter (name = activeConsumptionUom, type = string)
 *            , @AlgorithmSoftParameter (name = inductiveReactiveConsumptionUom, type = string)
 *            , @AlgorithmSoftParameter (name = additionalConsumptionUOM, type = string)
 *            , @AlgorithmSoftParameter (name = touMapID, type = string)
 *            , @AlgorithmSoftParameter (name = capacitiveReactiveConsumptionUom, type = string)})
 */
public class CmGetScalarUsageAlgComp_Impl
        extends CmGetScalarUsageAlgComp_Gen
        implements ApplyUsageRuleAlgorithmSpot {

    private static final Logger logger = LoggerFactory.getLogger(CmGetScalarUsageAlgComp_Impl.class);

    private static final String START_DTTM = "startDateTime";
    private static final String END_DTTM = "endDateTime";
    private static final String SCALAR_MC = "scalarMC";
    private static final String IS_ESTIMATE = "isEstimate";
    private static final String END_DTTM_TO = "endDateTimeTo";
    private static final String END_DTTM_FROM = "endDateTimeFrom";
    private static final String USAGE_PERIODS = "usagePeriods";
    private static final String USAGE_TYPE = "usageType";
    private static final String SEQUENCE = "sequence";
    private static final String USAGE_GROUP = "usageGroup";
    private static final String USAGE_RULE = "usageRule";
    private static final String USAGE_ID = "usageId";
    private static final String SCALAR_DETAILS = "scalarDetails";
    private static final String SCALAR_DETAILS_LIST = "scalarDetailsList";
    private static final String VALUE_IDENTIFIERS = "valueIdentifiers";
    private static final String SP_ID = "spId";
    private static final String SQI = "sqi";
    private static final String TOU = "tou";
    private static final String UOM = "uom";
    private static final String OFFSET_HOURS = "offsetHours";
    private static final String PREVIOUS_USAGE_ID = "previousUsageId";
    private static final String MEASURING_COMPONENT_ID = "measuringComponentId";
    private static final String MSRMT_DTTM1 = "MSRMT_DTTM1";
    private static final String MSRMT_DTTM = "MSRMT_DTTM";
    private static final String MSRMT_COND_FLG = "MSRMT_COND_FLG";
    private static final String FIELD_NAME = "MSRMT_VAL";
    private static final String MEASURES_PEAK_QUANTITY = "measuresPeakQuantity";
    private static final String MC_HOW_TO_USE = "mcHowToUse";
    private static final String USE_PERCENT = "usePercent";
    private static final String SP_HOW_TO_USE = "spHowToUse";
    private static final String QUANTITY = "quantity";
    private static final String END_MEASUREMENT = "endMeasurement";
    private static final String START_MEASUREMENT = "startMeasurement";
    private static final String FINAL_SQI = "finalSqi";
    private static final String FINAL_TOU = "finalTou";
    private static final String FINAL_UOM = "finalUom";
    private static final String APPLIED_MULTIPLIER = "appliedMultiplier";
    private static final String FINAL_QUANTITY = "finalQuantity";
    private static final String SQ_TYPE = "sqType";
    private static final String SQS_LIST = "SQsList";
    private static final String SQS_GROUP = "SQs";
    private static final String SQ_SEQUENCE = "sqSequence";
    private static final String USAGE_DETAILS = "usageDetails";
    private static final String END_MEASUREMENT_COND = "endMeasurementCondition";
    private static final String START_MEASUREMENT_COND = "startMeasurementCondition";
    private static final String USE_LATEST_END_READ_FLG = "alwaysUseLatestEndRead";
    private static final String ADJ_USAGE_PERIOD_END_DTTM_FLG = "adjustUsagePeriodEndDateTime";
    private static final String BILL_CONDITION = "billCondition";
    private static final String ALLOW_ESTIMATE = "allowEstimate";
    private static final String ALIGNMENT_DTTM = "alignmentDateTime";
    private static final String INVALID_DATA_EXP = "invalidDataException";
    private static final String MSRMT_RETRIEVAL_EXP = "measurementRetrievalException";
    private static final String CONFIGURATION_EXP = "configurationException";
    private static final String DATA_QLTY_ASSMT = "dataQualityAssessment";
    private static final String BILL_TYPE = "billType";
    private static final String UOM_KWH = "KWH";
    private static final String SQI_KWH = "GEN";
    private static final int FIVE = 5;

    private DateTime usagePeriodStartDateTime;
    private DateTime usagePeriodEndDateTime;
    private UsageRule usageRule;
    private UsageGroup usageGroup;
    private BusinessObjectInstance usageRuleBoInstance;
    private SchemaInstance usageTransactionBOInstance;
    
    //Rule Parameters
    private UnitOfMeasureD1 uom;
    private TimeOfUseD1 tou;
    private ServiceQuantityIdentifierD1 sqi;
    private TimeInterval offsetHours;
    private BillTypeLookup billType;
    
    private UsageSubscription usageSubscription;
    private DateTime scalarMcStartDateTime;
    private DateTime scalarMcEndDateTimeTo;
    private DateTime scalarMcEndDateTimeFrom;
    private DateTime startMsrmtDttm;
    private DateTime endMsrmtDttm;
    private Measurement startMeasurement;
    private Measurement endMeasurement;
    private BigDecimal addConsumptionEndMsrmt;

    private Element previousUTScalarDetailsGroup;
    private TimeZone usageTimeZone;
    private RoundingHelper roundingHelper = RoundingHelper.Factory.newInstance();
    private MathContext mathContext = roundingHelper.getContext(FIELD_NAME);
    private String previousUsageIdStr;
    private boolean isEstimate;
    private boolean isMeterExchange = false;
    private boolean isTOUCustomer = false;
    private AdjustUsagePeriodEndDateTimeLookup adjustUsagePeriodEndDateTimeLookup;
    private DateTime latestMeasurementEndDateTime;
    private BillConditionD1Lookup billCondition = null;

    private ApplyUsageRuleAlgorithmInputData applyUsageRuleAlgorithmInputData;
    private ApplyUsageRuleAlgorithmInputOutputData applyUsageRuleAlgorithmInputOutputData;
    private UsageRulesHelper usageRuleHelper = UsageRulesHelper.Factory.newInstance();
    private TimeZone installationTimeZone;
    private String usageSubscriptionMoCode;
    private String usageSubscriptionId;
    private DateTime alignmentDateTime;
    private Bool exitFromAlg = Bool.FALSE;
    private UseLatestEndReadLookup endReadOption;
    private AllowEstimateLookup allowEstimates;
    private int readingDetailsSequence = 1;
    private PreparedStatement statement = null;
    private BigDecimal netGenerationTouSum = BigDecimal.ZERO;
    private BigDecimal netConsumptionTouSum = BigDecimal.ZERO;
    
    private List<Element> readingDetailsList;
    private List<Element> valueIdentifierList;
    private Map<String, List<String>> deviceConfigurationMap = new HashMap<String, List<String>>();
    private Map<String, String> intervalMcMap = new HashMap<String, String>();
    private Map<String, String> netGenerationTouMap = new HashMap<String, String>();
    private Map<String, String> netConsumptionTouMap = new HashMap<String, String>();
    
    public ApplyUsageRuleAlgorithmInputOutputData getApplyUsageRuleAlgorithmInputOutputData() {
        return applyUsageRuleAlgorithmInputOutputData;
    }

    public void setApplyUsageRuleAlgorithmInputData(ApplyUsageRuleAlgorithmInputData applyusagerulealgorithminputdata) {
        applyUsageRuleAlgorithmInputData = applyusagerulealgorithminputdata;
    }

    public void setApplyUsageRuleAlgorithmInputOutputData(
            ApplyUsageRuleAlgorithmInputOutputData applyusagerulealgorithminputoutputdata) {
        applyUsageRuleAlgorithmInputOutputData = applyusagerulealgorithminputoutputdata;
    }

    /**
     * This method validates the input parameters and process the usage period list from the usage Bo
     */
    public void invoke() {
    	
        loadUsageSubscription();
        
        loadAndValidateScalarMcPeriods();
        if(exitFromAlg.isTrue()) {
        	return;        
        }
        
        retrieveUsageRuleBoInstance();
        loadUsageRuleDetails();
        if(exitFromAlg.isTrue()) {
        	return;        
        }
        
        validateSoftParams();
        if(exitFromAlg.isTrue()) {
        	return;
        }

        alignmentDateTime = usageTransactionBOInstance.getDateTime(ALIGNMENT_DTTM);
        if(notNull(alignmentDateTime)){
        	scalarMcEndDateTimeFrom = alignmentDateTime;
        	scalarMcEndDateTimeTo = alignmentDateTime;
        }
        
        retrievePreviousUTScalarDetails();
        
        validateUsagePeriodList(usageTransactionBOInstance); 
        
        retrieveBillCondition();
        
        processUsagePeriods();
        if(exitFromAlg.isTrue()) {
        	return;
        }
        
        //Update Usage Transaction Is Estimate flag value if it is not populated
        Element scalarMcGrp = usageTransactionBOInstance.getElement().element(SCALAR_MC);
        Element isEstimateElement = scalarMcGrp.element(IS_ESTIMATE);
    	if(isNull(isEstimateElement)){
    		isEstimateElement = scalarMcGrp.addElement(IS_ESTIMATE);
    	}
    	if(notNull(isEstimateElement)){
	        if (isEstimate) {
	        	isEstimateElement.setText(IsEstimateLookup.constants.YES.value());
	        } else {
	        	isEstimateElement.setText(IsEstimateLookup.constants.NO.value());
	        }
    	}
        
        applyUsageRuleAlgorithmInputOutputData.setUsageTransactionBOData(usageTransactionBOInstance);
    }

    /**
     * This method loads Usage Subscription. 
     *
     */
    private void loadUsageSubscription() {
        if (notNull(applyUsageRuleAlgorithmInputData)) {
            usageSubscription = applyUsageRuleAlgorithmInputData.getUsageSubscription();

            //Retrieve Usage Subscription MOCode and Entity Id
            usageSubscriptionMoCode = usageSubscription.getBusinessObject().getMaintenanceObject().getId()
                    .getTrimmedValue();
            usageSubscriptionId = usageSubscription.getId().getTrimmedValue();
            
            TimeZone_Id timeZoneId = new TimeZone_Id(usageSubscription.getTimeZone());
            if (isNull(timeZoneId)) {
                usageTimeZone = null;
            } else {
                usageTimeZone = timeZoneId.getEntity();
            }
        }
    }

    /**
     *This method loads and validates the Usage BO Scalar MC startDateTime and endDateTimeTo is populated 
     *checks whether if endDateTime is less than startDateTime, if so raise an error
     *
     */
    private void loadAndValidateScalarMcPeriods() {
        if (notNull(applyUsageRuleAlgorithmInputOutputData)) {
            retrieveUsageTransactionBoInstance();
            
            Element scalarMC = usageTransactionBOInstance.getElement().element(SCALAR_MC);
            scalarMcStartDateTime = DateTime.fromIso(scalarMC.elementText(START_DTTM));
            scalarMcEndDateTimeFrom = DateTime.fromIso(scalarMC.elementText(END_DTTM_FROM));
            scalarMcEndDateTimeTo = DateTime.fromIso(scalarMC.elementText(END_DTTM_TO));
            
            if(!usageRuleHelper.validateScalarMcPeriods(scalarMcStartDateTime, scalarMcEndDateTimeTo,
            		applyUsageRuleAlgorithmInputOutputData,INVALID_DATA_EXP, usageRuleBoInstance, 
            		applyUsageRuleAlgorithmInputData.getUsageRuleCode())){
            	exitFromAlg = Bool.TRUE;
            	return;
            }
            
            //UT Is Estimate flag value
            IsEstimateLookup isEstimateLookup = LookupHelper.getLookupInstance(IsEstimateLookup.class, 
            			scalarMC.elementText(IS_ESTIMATE));
            if(notNull(isEstimateLookup) && isEstimateLookup.isYes()){
            	isEstimate = true;
            }
            
            //UT Allow Estimate flag value
            allowEstimates = LookupHelper.getLookupInstance(AllowEstimateLookup.class, 
        			scalarMC.elementText(ALLOW_ESTIMATE));
            
            installationTimeZone = InstallationHelper.fetchTimeZone();
        }
    }

    /**
     * This method will be used for retrieving the usageTransacation BoInstance from plug in spot hard parameters
     *
     */
    private void retrieveUsageTransactionBoInstance() {
        if (isNull(applyUsageRuleAlgorithmInputOutputData.getUsageTransactionBOData())) {
            addError(StandardMessages.fieldMissing("D1_USAGE_ID"));
        }
        usageTransactionBOInstance = applyUsageRuleAlgorithmInputOutputData.getUsageTransactionBOData();
    }

    /**
     * Retrieve the  Usage Rule  Business Object Instance
     */
    private void retrieveUsageRuleBoInstance() {
        if (isNull(applyUsageRuleAlgorithmInputData)) return;
        BusinessObject usageRuleBo = null;
        usageRuleBo = applyUsageRuleAlgorithmInputData.getUsageRuleBo();
        if (notNull(usageRuleBo)) {
            usageRule = applyUsageRuleAlgorithmInputData.getUsageRuleCode();
            usageGroup = applyUsageRuleAlgorithmInputData.getUsageGroupCode();
            readUsageRuleBoInstance(usageRuleBo);

        }
    }

    /**
     *  This is method is used for reading the usage rule bo instance
     * @param usageRuleBo
     */
    private void readUsageRuleBoInstance(BusinessObject usageRuleBo) {
        if (notNull(usageGroup) && notNull(usageGroup.getId())) {
            if (notNull(usageRule) && notNull(usageRule.getId()) && notNull(usageRule.getId().getUsageRule())) {
                long startTime = System.currentTimeMillis();
                usageRuleBoInstance = BusinessObjectInstance.create(usageRuleBo);
                usageRuleBoInstance.set(USAGE_GROUP, usageGroup.getId().getTrimmedValue());
                usageRuleBoInstance.set(USAGE_RULE, usageRule.getId().getUsageRule().trim());
                usageRuleBoInstance = BusinessObjectDispatcher.read(usageRuleBoInstance);
                long endTime = System.currentTimeMillis();

                logger.info("Reading BO:"
                                + usageRuleBoInstance.getSchemaName()
                                + ",in CmScalarAlgComp_Impl, Reason: for Retrieving scalar Data. Time taken:"
                                + (endTime - startTime) + " ms");
            }
        }
    }

    /**
     * this method  validates soft parameters i.e  input Regular Top Range Condition is greater than or equal to the soft parameter input Regular Bottom Range Condition
     *
     */
    private void validateSoftParams() {
        if(!usageRuleHelper.validateEstimateTopBottomRangeConditions(getEstimateBottomRangeCondition(), getEstimateTopRangeConditionRange(), 
        		applyUsageRuleAlgorithmInputOutputData, CONFIGURATION_EXP, usageRuleBoInstance, applyUsageRuleAlgorithmInputData.getUsageRuleCode())){
        	exitFromAlg = Bool.TRUE;
        	return;
        } 
    } 
    
    /**
     * This method retrieves the previous UT scalar details
     */
    private void retrievePreviousUTScalarDetails() {
        if (notNull(usageTransactionBOInstance)) {
            // Previous Usage Transaction ID
            previousUsageIdStr = usageTransactionBOInstance.getString(PREVIOUS_USAGE_ID);
            if (notBlank(previousUsageIdStr)) {
                UsageTransaction_Id previousUsageId = new UsageTransaction_Id(previousUsageIdStr);

                BusinessObjectInstance previousUTBOInstance = BusinessObjectInstance.create(new BusinessObject_Id(
                        "D2-UsageTranScalarDetailsLite").getEntity());
                previousUTBOInstance.set(USAGE_ID, previousUsageId.getIdValue());
                previousUTBOInstance = BusinessObjectDispatcher.read(previousUTBOInstance);

                previousUTScalarDetailsGroup = previousUTBOInstance.getElement().element(SCALAR_DETAILS);
            }           
        }
    }
    
    @SuppressWarnings("unchecked")
	private void loadUsageRuleDetails() {
        Element scalarDetailsGroup = usageRuleBoInstance.getElement().element(SCALAR_DETAILS);
        if (notNull(scalarDetailsGroup)) {
        	//Bill Type
        	String billTypeStr = scalarDetailsGroup.elementText(BILL_TYPE);
            if (notBlank(billTypeStr)) {
            	billType = LookupHelper.getLookupInstance(BillTypeLookup.class, billTypeStr);
            }
        	//Retrieve Adjust Usage Period End Date Time
            String adjustUTEndDateTimeStr = scalarDetailsGroup.elementText(ADJ_USAGE_PERIOD_END_DTTM_FLG);
            if (notBlank(adjustUTEndDateTimeStr)) {
                adjustUsagePeriodEndDateTimeLookup = LookupHelper.getLookupInstance(
                        AdjustUsagePeriodEndDateTimeLookup.class, adjustUTEndDateTimeStr);
            }
            //Retrieve Use Latest End Read
            String alwaysUseLatestEndReadStr = scalarDetailsGroup.elementText(USE_LATEST_END_READ_FLG);
            if (notBlank(alwaysUseLatestEndReadStr)) {
                endReadOption = LookupHelper.getLookupInstance(UseLatestEndReadLookup.class, alwaysUseLatestEndReadStr);
            }
            //Retrieve Offset Hours
            offsetHours = usageRuleBoInstance.getGroup(SCALAR_DETAILS).getTimeInterval(OFFSET_HOURS);
            if (isNull(offsetHours)) {
            	offsetHours = TimeInterval.ZERO;
            }
            //Retrieve value identifiers list
            Element valueIdentifiersGrp = scalarDetailsGroup.element(VALUE_IDENTIFIERS);
            if(notNull(valueIdentifiersGrp)){
            	valueIdentifierList = valueIdentifiersGrp.elements();
            }
        	if(isNull(valueIdentifierList) || valueIdentifierList.size() == 0) {
        		exitFromAlg = Bool.TRUE;
        		return;
        	}
        }
    }
    
    @SuppressWarnings("unchecked")
    private void validateUsagePeriodList(SchemaInstance usageTransactionBOInstance){
    	Element usagePeriodGrp = usageTransactionBOInstance.getElement().element("usagePeriods");
    	if(isNull(usagePeriodGrp)){
    		usagePeriodGrp = usageTransactionBOInstance.getElement().addElement("usagePeriods");
    	}
    	
    	boolean insertRow = false;
    	int sequence = 1;
    	List<Element> usagePeriodsList = usagePeriodGrp.elements();
    	if(isNull(usagePeriodsList) || usagePeriodsList.size() == 0){
    		sequence = 1; 
    		insertRow = true;
    	}else{
    		for(Element usagePeriod : usagePeriodsList){
    			UsageTypeD2Lookup usageType = LookupHelper.getLookupInstance(UsageTypeD2Lookup.class, 
    						usagePeriod.elementText("usageType"));
    			if(isNull(usageType)){
    				usagePeriod.addElement("usageType").setText(UsageTypeD2Lookup.constants.SCALAR.toString());
                    return;
    			}
    			if(usageType.isScalar()){
    				return;
    			}
    			
    			if(usageType.isInterval()){
    				insertRow = true;
    			}
    		}
    		
			sequence = Integer.parseInt((usagePeriodsList.get(usagePeriodsList.size() - 1)).elementTextTrim("sequence"));
			sequence += sequence;
    	}
    	
    	if(insertRow == true){
    		//Insert a new row in the usage period list
    		Element scalarMC = usageTransactionBOInstance.getElement().element(SCALAR_MC);
    		
    		Element usagePeriodListNew = new DefaultElement("usagePeriodsList");
    		usagePeriodListNew.addElement("sequence").setText(String.valueOf(sequence));
    		usagePeriodListNew.addElement("startDateTime").setText(scalarMC.elementText("startDateTime"));
    		usagePeriodListNew.addElement("endDateTime").setText(scalarMC.elementText("endDateTimeTo"));
    		usagePeriodListNew.addElement("usageType").setText(UsageTypeD2Lookup.constants.SCALAR.toString());
    		usagePeriodsList.add(usagePeriodListNew);
    	}
    }

    private void retrieveBillCondition(){
    	BillConditionD2Lookup billConditionD2 = (BillConditionD2Lookup) usageTransactionBOInstance.getLookup(BILL_CONDITION);
        if (notNull(billConditionD2)) {
            if (billConditionD2.isInitial()) {
                billCondition = BillConditionD1Lookup.constants.INITIAL;
            } else if (billConditionD2.isInterim()) {
                billCondition = BillConditionD1Lookup.constants.INTERIM;
            } else if (billConditionD2.isClosing()) {
                billCondition = BillConditionD1Lookup.constants.CLOSING;
            } else if (billConditionD2.isInitialAndClosing()) {
                billCondition = BillConditionD1Lookup.constants.INITIAL_AND_CLOSING;
            }
        } else {
            billCondition = BillConditionD1Lookup.constants.INTERIM;
        }
    }
    
    /**
     * This method process the usage periods list and retireve device configurations for that usage period.
     *
     */
    @SuppressWarnings("unchecked")
	private void processUsagePeriods() {
        Element usagePeriodsGroup = usageTransactionBOInstance.getElement().element(USAGE_PERIODS);
        if (isNull(usagePeriodsGroup)) {
            return;
        }
        List<Element> usagePeriodList = usagePeriodsGroup.elements();
        if (isNull(usagePeriodList) || usagePeriodList.isEmpty()) {
            return;
        }
        
        for (Element usagePeriod : usagePeriodList) {
        	usagePeriodStartDateTime = DateTime.fromIso(usagePeriod.elementText(START_DTTM));
            usagePeriodEndDateTime = DateTime.fromIso(usagePeriod.elementText(END_DTTM));
        	
            if(notNull(alignmentDateTime) 
            		&& usagePeriodStartDateTime.compareTo(alignmentDateTime) > 0){
            	continue;
            }
            
            /* skip the usage period if usage type is not scalar */
            String usageTypeStr = usagePeriod.elementText(USAGE_TYPE);
            if(notBlank(usageTypeStr)){
            	UsageTypeD2Lookup usageType = LookupHelper.getLookupInstance(UsageTypeD2Lookup.class, usageTypeStr);
	            if (notNull(usageType) && usageType.isInterval()) {
	                continue;
	            }
            }
            
            readingDetailsList = new ArrayList<Element>();
            retrieveReadDetailsSequence();
            
            processSPDeviceConfiguration();
            if(exitFromAlg.isTrue()){
            	return;
            }
            
            buildUsagePeriods(usagePeriod);
        }
    }

    private void processSPDeviceConfiguration() {
        isMeterExchange = false;
        latestMeasurementEndDateTime = null;
        Bool ignoreNextMeter = Bool.FALSE;
        

        DetermineSubscriptionDvcCfgPeriodInputData determineSubscriptionDvcCfgPeriodInputData = DetermineSubscriptionDvcCfgPeriodInputData.Factory
                .newInstance();
        determineSubscriptionDvcCfgPeriodInputData.setUsageStartDateTime(usagePeriodStartDateTime);
        determineSubscriptionDvcCfgPeriodInputData.setUsageEndDateTime(usagePeriodEndDateTime);
        determineSubscriptionDvcCfgPeriodInputData.setUsageId(usageTransactionBOInstance.getString(USAGE_ID));

        List<DetermineSubscriptionDvcCfgPeriodOutputData> determineSubscriptionDvcCfgPeriodOutputDataList = usageSubscription
                .retrieveDeviceConfigurationPeriod(determineSubscriptionDvcCfgPeriodInputData);
        int deviceConfigListSize = determineSubscriptionDvcCfgPeriodOutputDataList.size();
        
        logger.info("**********processSPDeviceConfiguration***"+deviceConfigListSize);

        int deviceConfigCount = 0;
        for (DetermineSubscriptionDvcCfgPeriodOutputData determineSubscriptionDvcCfgPeriodOutputData : 
        		determineSubscriptionDvcCfgPeriodOutputDataList) {
 	
            ServicePointD1_Id currentSp = null;
            DeviceConfiguration_Id currentSpDC = null;
            ServicePointD1_Id nextOrPreviousSp = null;
            DeviceConfiguration_Id nextOrPreviousSpDC = null;
            ServicePointD1_Id currentSpForNowPreviousSpInNextIteration = null;
            isMeterExchange = false;

            DateTime nextDCStartDateTime = null;

            DeviceConfiguration_Id currentSpDCForNowPreviousSpDCInNextIteration = null;

            currentSp = determineSubscriptionDvcCfgPeriodOutputDataList.get(deviceConfigCount).getServicePoint()
                    .getId();
            currentSpDC = determineSubscriptionDvcCfgPeriodOutputDataList.get(deviceConfigCount)
                    .getDeviceConfiguration().getId();
            //Below Logic for meter exchange scenario
            if (deviceConfigListSize > 1) {
                if (deviceConfigCount < (deviceConfigListSize - 1)) {
                    nextOrPreviousSp = determineSubscriptionDvcCfgPeriodOutputDataList.get(deviceConfigCount + 1)
                            .getServicePoint().getId();
                    nextOrPreviousSpDC = determineSubscriptionDvcCfgPeriodOutputDataList.get(deviceConfigCount + 1)
                            .getDeviceConfiguration().getId();
                    nextDCStartDateTime = determineSubscriptionDvcCfgPeriodOutputDataList.get(deviceConfigCount + 1)
                            .getStartDateTime().asDateOnly();
                } else {
                    nextOrPreviousSp = determineSubscriptionDvcCfgPeriodOutputDataList.get(deviceConfigCount - 1)
                            .getServicePoint().getId();
                    nextOrPreviousSpDC = determineSubscriptionDvcCfgPeriodOutputDataList.get(deviceConfigCount - 1)
                            .getDeviceConfiguration().getId();
                }
                if ((notNull(currentSp) && currentSp.equals(currentSpForNowPreviousSpInNextIteration) && !(notNull(currentSpDC) && currentSpDC
                        .equals(currentSpDCForNowPreviousSpDCInNextIteration)))
                        || (currentSp.equals(nextOrPreviousSp) && (currentSpDC.equals(nextOrPreviousSpDC)))
                        || (currentSp.equals(nextOrPreviousSp) && !(currentSpDC.equals(nextOrPreviousSpDC)))) {

                    isMeterExchange = true;
                }
                currentSpForNowPreviousSpInNextIteration = currentSp;
                currentSpDCForNowPreviousSpDCInNextIteration = currentSpDC;
                // this is for meter exchange at  UT start date time
                //Case1 : If  bill condition is initial or initial and closing then the next meter should be considered for calculation
                if (isMeterExchange && notNull(nextDCStartDateTime)) {
                    if (determineSubscriptionDvcCfgPeriodOutputData.getEndDateTime().asDateOnly().equals(
                            nextDCStartDateTime)
                            && determineSubscriptionDvcCfgPeriodOutputData.getEndDateTime().asDateOnly().equals(
                                    usagePeriodStartDateTime.asDateOnly())
                            && (billCondition.isInitial() || billCondition.isInitialAndClosing())) {
                        deviceConfigCount++;
                        continue;
                    }

                }
                //Case 2 : If  bill condition is closing then the previous meter should be considered for calculation and the next meter should be ignored
                if (isMeterExchange && notNull(nextDCStartDateTime)) {
                    if (determineSubscriptionDvcCfgPeriodOutputData.getEndDateTime().asDateOnly().equals(
                            nextDCStartDateTime)
                            && determineSubscriptionDvcCfgPeriodOutputData.getEndDateTime().asDateOnly().equals(
                                    usagePeriodStartDateTime.asDateOnly()) && (billCondition.isClosing())) {
                        ignoreNextMeter = Bool.TRUE;
                    }
                }
                if (isMeterExchange && ignoreNextMeter.isTrue() && deviceConfigCount > 0) {
                    deviceConfigCount++;
                    continue;
                }
            }
            deviceConfigCount++;

            List<MeasuringComponent> measuringComponentList = determineSubscriptionDvcCfgPeriodOutputData
                    .getDeviceConfiguration().retrieveMeasuringComponents(IntervalScalarLookup.constants.SCALAR);

            if(isNull(measuringComponentList)){
            	continue;
            }
            
            List<MeasuringComponent> intervalMeasuringComponentList = null;
            if(notNull(billType) && billType.isGenerationCustomer()){
            	intervalMeasuringComponentList = determineSubscriptionDvcCfgPeriodOutputData
            				.getDeviceConfiguration().retrieveMeasuringComponents(IntervalScalarLookup.constants.INTERVAL);  
            	//Retrieve Whetehr TOU Customer or Non-TOU Customer
            	for(Element valueIdentifier : valueIdentifierList){
                    tou = null;
                    if(notBlank(valueIdentifier.elementText(TOU))){
                		tou = new TimeOfUseD1_Id(valueIdentifier.elementText(TOU)).getEntity();
                	}
            		if(notNull(tou)){
            			isTOUCustomer = true;
            			break;
            		}
            	}
            	
            }	
            
            for (MeasuringComponent measuringComponent : measuringComponentList) {
                MeasuringComponentType mcType = measuringComponent.getMeasuringComponentType();
                logger.info("mcId "+measuringComponent.getId().getIdValue());
                MeasuringComponentTypeValueIdentifier mctvi = null;
                uom = null;
                tou = null;
                sqi = null;
                
                //check if the measuring component is applicable for the given UOM/TOU/SQI
                for(Element valueIdentifier : valueIdentifierList){
                	uom = null;
                    tou = null;
                    sqi = null;
                    
                	if(notBlank(valueIdentifier.elementText(UOM))){
                		uom = new UnitOfMeasureD1_Id(valueIdentifier.elementText(UOM)).getEntity();
                	}
                	if(notBlank(valueIdentifier.elementText(TOU))){
                		tou = new TimeOfUseD1_Id(valueIdentifier.elementText(TOU)).getEntity();
                	}
                	if(notBlank(valueIdentifier.elementText(SQI))){
                		sqi = new ServiceQuantityIdentifierD1_Id(valueIdentifier.elementText(SQI)).getEntity();
                	}
                	
	                mctvi = mcType.determineValueIdentifier(uom, tou, sqi);
	                logger.info(" uom "+uom+" tou "+tou+"sqi "+sqi);
	                if (notNull(mctvi)) {
	                    break;
	                }
                }

                if (isNull(mctvi)) {
                    continue;
                }
                
                if(notNull(intervalMeasuringComponentList)){
                	               	
                	if(uom.getId().getIdValue().contentEquals(UOM_KWH)){
                		
                		if(netGenerationTouMap.isEmpty() || netConsumptionTouMap.isEmpty()){
                		
                		for (MeasuringComponent intervalMeasuringComponent : intervalMeasuringComponentList) {
                			MeasuringComponentType intervalMcType = intervalMeasuringComponent.getMeasuringComponentType();
                			ServiceQuantityIdentifierD1 genSQI = new ServiceQuantityIdentifierD1_Id(SQI_KWH).getEntity();
                			MeasuringComponentTypeValueIdentifier intervalMctvi = intervalMcType.determineValueIdentifier(uom, null,genSQI);
	                    	if(isNull(intervalMctvi)){
	                    		continue;
	                    	}
	                    	if((isTOUCustomer) && (intervalMctvi.fetchIdValueIdentifierType().isMeasurement())){
	                    		List<SQLResultRow> lstApplyTouMap = applyTouMapping(intervalMeasuringComponent);
		                    	if(notNull(lstApplyTouMap)){
		                    		Iterator<SQLResultRow> lstApplyTouMapItr = lstApplyTouMap.iterator();
		                    		while(lstApplyTouMapItr.hasNext()){
		                    			SQLResultRow applyTOUMapEntry = lstApplyTouMapItr.next();
		                    			netGenerationTouMap.put(applyTOUMapEntry.getString("D1_TOU_CD"),applyTOUMapEntry.getBigDecimal("MSRMTVAL").toString());
		                    			netGenerationTouSum = netGenerationTouSum.add(applyTOUMapEntry.getBigDecimal("MSRMTVAL"));
		                    		}
		                    	
		                    	//Global Hash-Map 
		                    	if(!(netGenerationTouMap.isEmpty()))
		                    		netGenerationTouMap.put("touSum",netGenerationTouSum.toString());	
		                    	}
	                    		
	                    	}
	                    	else if((isTOUCustomer) && (!(intervalMctvi.fetchIdValueIdentifierType().isMeasurement()))){
	                    		List<SQLResultRow> lstApplyTouMap = applyTouMapping(intervalMeasuringComponent);
		                    	if(notNull(lstApplyTouMap)){
		                    		Iterator<SQLResultRow> lstApplyTouMapItr = lstApplyTouMap.iterator();
		                    		while(lstApplyTouMapItr.hasNext()){
		                    			SQLResultRow applyTOUMapEntry = lstApplyTouMapItr.next();
		                    			netConsumptionTouMap.put(applyTOUMapEntry.getString("D1_TOU_CD"),applyTOUMapEntry.getBigDecimal("MSRMTVAL").toString());
		                    			netConsumptionTouSum = netConsumptionTouSum.add(applyTOUMapEntry.getBigDecimal("MSRMTVAL"));
		                    		}
		                    	
		                    	//Global Hash-Map 
		                    	if(!(netConsumptionTouMap.isEmpty()))
		                    		netConsumptionTouMap.put("touSum",netConsumptionTouSum.toString());
		                    	}
	                    		
	                    	}
	                    	else{
	                    		intervalMcMap.put(measuringComponent.getId().getIdValue(), intervalMeasuringComponent.getId().getIdValue());
	                    		break;
	                		}
	                    	
                		}               			
                		
                	}
                }
                	
             }
                
                processMeasuringComponent(measuringComponent, mctvi, determineSubscriptionDvcCfgPeriodOutputData);
                if(exitFromAlg.isTrue()) {
                	return;
                }
            }
        }
    }

    private List<SQLResultRow> applyTouMapping(
			MeasuringComponent intervalMeasuringComponent) {
    	
    	//Retrieve Interval List    	
    	List<QueryResultRow> intervalList = retrieveIntervalMeasurementList(intervalMeasuringComponent, usagePeriodStartDateTime, usagePeriodEndDateTime);
    	   	
    	//Populate Temporary Measurement Table 
    	if(notNull(intervalList))
    	populateMeasurementTempTable(intervalList);
    	
    	//Retrieve TOU Map 
    	TimeOfUseMapD1 timeOfUseMap = new TimeOfUseMapD1_Id(getTouMapID()).getEntity();
    	
    	//Apply TOU Mapping on Temporary Measurements
    	List<SQLResultRow> lstApplyTouMap = this.applyTouMappingonTempMsrmts(timeOfUseMap, true, intervalMeasuringComponent);

    	return lstApplyTouMap;
 		
	}
    
    private List<SQLResultRow> applyTouMappingonTempMsrmts(
			TimeOfUseMapD1 touMapId, Boolean useMsrmtTempTable,
			MeasuringComponent measrComp) {
		StringBuilder touMapQuery = new StringBuilder();
		touMapQuery.append(" SELECT   A.D1_TOU_CD,   B.MSRMT_COND_FLG ");
		touMapQuery
				.append(", SUM (B.MSRMT_VAL) AS MSRMTVAL,  COUNT(*) AS CNT    ");
		if (useMsrmtTempTable) {
			touMapQuery.append(" FROM  D1_TOU_MAP_DATA  A,   D1_MSRMT_TMP B  ");
		} else {
			touMapQuery.append(" FROM  D1_TOU_MAP_DATA  A,   D1_MSRMT B  ");
		}

		touMapQuery
				.append("   WHERE   B.MSRMT_DTTM > :startDateTime AND B.MSRMT_DTTM <= :endDateTime    ");
		if (!useMsrmtTempTable) {
			touMapQuery.append("   AND B.MEASR_COMP_ID = :mc    ");
		}

		touMapQuery
				.append("   AND A.TOU_MAP_DATA_DTTM > :startDateTime AND A.TOU_MAP_DATA_DTTM <= :endDateTime ");
		touMapQuery.append("   AND A.TOU_MAP_DATA_DTTM = B.MSRMT_DTTM  ");
		touMapQuery.append("   AND A.D1_TOU_MAP_ID  = :inputTouMapId ");
		touMapQuery.append("   GROUP BY (A.D1_TOU_CD, B.MSRMT_COND_FLG)  ");
		this.statement = this.createPreparedStatement(touMapQuery.toString(),
				this.getClass().getName());
		if (!useMsrmtTempTable) {
			this.statement.bindEntity("mc", measrComp);
		}

		this.statement.bindEntity("inputTouMapId", touMapId);
		this.statement.bindDateTime("startDateTime",
				usagePeriodStartDateTime);
		this.statement.bindDateTime("endDateTime",
				usagePeriodEndDateTime.addSeconds(1));
		List<SQLResultRow> query = this.statement.list();
		
		return query.size() > 0 ? query : null;
	}

	private void populateMeasurementTempTable(
			List<QueryResultRow> intervalList) {
		
    	PopulateMeasurementTempInputData msrmtTempInputData = com.splwg.d2.domain.admin.usageRule.data.PopulateMeasurementTempInputData.Factory
				.newInstance();
		List<PopulateMsrmtTempInputMsrmtData> msrmtDataList = new ArrayList();
		
		if (this.notNull(intervalList)) {
			Iterator<QueryResultRow> var4 = intervalList.iterator();
			while (var4.hasNext()) {
				QueryResultRow intervalData = var4.next();
				
				PopulateMsrmtTempInputMsrmtData measurementInputData = com.splwg.d2.domain.admin.usageRule.data.PopulateMsrmtTempInputMsrmtData.Factory
						.newInstance();
				
				measurementInputData.setMeasurementCondition(intervalData.getString(MSRMT_COND_FLG));
				measurementInputData.setMeasurementDateTime(intervalData.getDateTime(MSRMT_DTTM));
				measurementInputData.setMeasurementValue(intervalData.getBigDecimal("MSRMT_VAL2"));
				
				msrmtDataList.add(measurementInputData);
			}
		}

		msrmtTempInputData
				.setPopulateMsrmtTempInputMsrmtDataList(msrmtDataList);
		PopulateMeasurementTemp populateMeasurementTemp = com.splwg.d2.domain.admin.usageRule.routines.PopulateMeasurementTemp.Factory
				.newInstance();
		populateMeasurementTemp
				.processMeasurementTemporaryTableData(msrmtTempInputData);
		
	}

	/**
     * using this method process measuring components in 2 cases
     *  case1   determines the start msrmt date time of MC
     *  case 2 determines end msrmt date time 
     * @param measuringComponent
     * @param mctvi
     * @param determineSubscriptionDvcCfgPeriodOutputData
     */
    private void processMeasuringComponent(MeasuringComponent measuringComponent, MeasuringComponentTypeValueIdentifier mctvi,
            DetermineSubscriptionDvcCfgPeriodOutputData determineSubscriptionDvcCfgPeriodOutputData) {
    	logger.info("getAdditionalConsumptionUOM() "+getAdditionalConsumptionUOM());
    	if(!isBlankOrNull(getAdditionalConsumptionUOM()) && 
    			getAdditionalConsumptionUOM().trim().equalsIgnoreCase(mctvi.fetchUnitOfMeasure().getId().getIdValue().trim()) ){
    		determineEndMsrmtDateTimeAndInsertScalarDetailsForAddCons(measuringComponent, mctvi,
                    determineSubscriptionDvcCfgPeriodOutputData);
    		logger.info("getAdditionalConsumptionUOM() "+getAdditionalConsumptionUOM());
    	}
    	else{
	        determineStartMeasurementDateTimeAndMsrmt(measuringComponent, mctvi,
	                determineSubscriptionDvcCfgPeriodOutputData.getServicePoint().getId(),
	                determineSubscriptionDvcCfgPeriodOutputData.getStartDateTime());
	        if(exitFromAlg.isTrue()){
	        	return;
	        }
	        
	        determineEndMsrmtDateTimeAndInsertScalarDetails(measuringComponent, mctvi,
	                determineSubscriptionDvcCfgPeriodOutputData);  
    	}     
    }
    	

    /**
     * This method derives the Start Measurement Date Time  and its Measurment Record
     * @param mc
     * @param mctvi
     * @param determineSubscriptionDvcCfgPeriodOutputData
     * @return
     */
    @SuppressWarnings("unchecked")
	private void determineStartMeasurementDateTimeAndMsrmt(MeasuringComponent mc,
            MeasuringComponentTypeValueIdentifier mctvi, ServicePointD1_Id spId, DateTime startDateTime) {
        startMeasurement = null;
        DateTime previousUTEndMsrmtDttm = null;

        if (notNull(previousUsageIdStr)) {
            if (notNull(previousUTScalarDetailsGroup)) {
            	List<Element> previousUTScalarDetailsList = previousUTScalarDetailsGroup.elements();
            	if(notNull(previousUTScalarDetailsList) && previousUTScalarDetailsList.size() > 0){
            		for (Element previousUTScalarDetails : previousUTScalarDetailsList){
            			String previousUTScalarDetailsSpId = previousUTScalarDetails.elementText(SP_ID);
            			String previousUTScalarDetailsMcId = previousUTScalarDetails.elementText(MEASURING_COMPONENT_ID);
            			if(checkIfEquals(spId.getIdValue(), previousUTScalarDetailsSpId) 
            					&& checkIfEquals(mc.getId().getIdValue(), previousUTScalarDetailsMcId)){
            				DateTime previousUTScalarDetailsEndDttm = DateTime.fromIso(previousUTScalarDetails.elementText(END_DTTM));
            				if (isNull(previousUTEndMsrmtDttm) || (previousUTEndMsrmtDttm.compareTo(previousUTScalarDetailsEndDttm) < 0)) {
                                previousUTEndMsrmtDttm = previousUTScalarDetailsEndDttm;
                            }
            			}
            		}
            	}
            }
            
            if (notNull(previousUTEndMsrmtDttm)) {
                if (isMeterExchange && previousUTEndMsrmtDttm.compareTo(startDateTime) <= 0) {
                    previousUTEndMsrmtDttm = null;
                } else {
                    startMsrmtDttm = previousUTEndMsrmtDttm;
                    if (notNull(mctvi.fetchUnitOfMeasure())
                            && mctvi.fetchUnitOfMeasure().getMeasuresPeakQuantity().isDoesNotMeasurePeakQuantity()) {
                        startMeasurement = findStartMeasurementRecord(mc, mctvi, previousUTEndMsrmtDttm);
                        startMsrmtDttm = startMeasurement.getMeasurementLocalDateTime();
                    }
                }
            }
        }
        
        if (isNull(previousUsageIdStr) || isNull(previousUTEndMsrmtDttm)) {

            startMsrmtDttm = startDateTime;

            //If current MC is non-consumptive usage (MC Type’s value Identifier’s UOM measures peak is NO)
            if (notNull(mctvi.fetchUnitOfMeasure())
                    && mctvi.fetchUnitOfMeasure().getMeasuresPeakQuantity().isDoesNotMeasurePeakQuantity()) {
                String uom =  mctvi.fetchUnitOfMeasure().getId().getIdValue().trim();
                
                startMeasurement = findStartMeasurementRecord(mc, mctvi, startDateTime);

                if (isNull(startMeasurement)) {

                    List<QueryResultRow> measurementList = retrieveMeasurements(mc, startDateTime
                            .subtract(offsetHours), startDateTime
                            .add(offsetHours), Bool.TRUE);

                    if (isNull(measurementList) || measurementList.isEmpty()) {
	                    	String formattedStartMsrmtDttm = usageRuleHelper
	                                .retrieveFormattedLegalDateTime(startMsrmtDttm, installationTimeZone, true,
	                                        usageTimeZone, usageSubscriptionMoCode, usageSubscriptionId);
	                        addException(MSRMT_RETRIEVAL_EXP,MessageRepository.startMeasurementIsNotFoundForMeasuringComponentAndDateTime(mc,
	                                formattedStartMsrmtDttm));
	                        return;
                    }

                    if (notNull(measurementList) && measurementList.size() > 1) {
                        startMeasurement = findHighestPriorityMeasurement(measurementList, mc);
                        startMsrmtDttm = startMeasurement.getMeasurementLocalDateTime();

                    } else {
                        //only one measurement retrieved
                        QueryResultRow mRow = measurementList.iterator().next();
                        DateTime currentMeasurementDateTime = mRow.getDateTime(MSRMT_DTTM1);
                        startMeasurement = new Measurement_Id(mc, currentMeasurementDateTime).getEntity();
                        startMsrmtDttm = startMeasurement.getMeasurementLocalDateTime();
                    }
                }

            }
        }
    }

    /**
     * This method determines end measurement date time and inserts the scalar details
     * @param mc
     * @param mctvi
     * @param determineSubscriptionDvcCfgPeriodOutputData
     */
    private void determineEndMsrmtDateTimeAndInsertScalarDetails(MeasuringComponent mc,
            MeasuringComponentTypeValueIdentifier mctvi,
            DetermineSubscriptionDvcCfgPeriodOutputData determineSubscriptionDvcCfgPeriodOutputData) {
        DateTime endMsrmtSearchPeriodStDateTime = null;
        DateTime endMsrmtSearchPeriodEndDateTime = null;
        endMeasurement = null;
        endMsrmtDttm = null;

        endMsrmtDttm = determineSubscriptionDvcCfgPeriodOutputData.getEndDateTime();
        //If current MC is non-consumptive usage (MC Type’s value Identifier’s UOM measures peak is NO)
        if (notNull(mctvi.fetchUnitOfMeasure())
                && mctvi.fetchUnitOfMeasure().getMeasuresPeakQuantity().isDoesNotMeasurePeakQuantity()) {
        	String uom =  mctvi.fetchUnitOfMeasure().getId().getIdValue().trim();
            endMsrmtSearchPeriodStDateTime = deriveEndMsrmtSearchPeriodStDateTime(determineSubscriptionDvcCfgPeriodOutputData);
            endMsrmtSearchPeriodEndDateTime = determineSubscriptionDvcCfgPeriodOutputData.getEndDateTime().add(
            		offsetHours);

            List<QueryResultRow> measurementList = retrieveMeasurements(mc, endMsrmtSearchPeriodStDateTime,
                    endMsrmtSearchPeriodEndDateTime, Bool.TRUE);
            if (isNull(measurementList) || measurementList.isEmpty()){
            	   String formattedUsagePeriodEndDateTime = usageRuleHelper.retrieveFormattedLegalDateTime(
                           usagePeriodEndDateTime, installationTimeZone, true, usageTimeZone, usageSubscriptionMoCode,
                           usageSubscriptionId);
                   addException(MSRMT_RETRIEVAL_EXP,MessageRepository.measuringComponentEndReadingNotFoundOnTheEndDate(mc,
                               formattedUsagePeriodEndDateTime));
                   return;
            }
            if (measurementList.size() > 1) {
                //multiple measurements retrieved
                endMeasurement = findHighestPriorityMeasurement(measurementList, mc);
            } else {
                //only one measurement retrieved
                QueryResultRow mRow = measurementList.iterator().next();

                DateTime currentMeasurementDateTime = mRow.getDateTime(MSRMT_DTTM1);
                endMeasurement = new Measurement_Id(mc, currentMeasurementDateTime).getEntity();
            }

            endMsrmtDttm = endMeasurement.getMeasurementLocalDateTime();
            setLatestMeasurementEndDateTime(endMsrmtDttm);

        } else {
            //consumptive case   --MC Type’s value Identifier’s UOM measures peak is YES

            List<QueryResultRow> measurementList = retrieveMaxMeasurementRecord(mc, startMsrmtDttm, endMsrmtDttm,
                    Bool.FALSE, mctvi);
            if (isNull(measurementList) || measurementList.isEmpty()) {

                String formattedStartMsrmtDttm = usageRuleHelper.retrieveFormattedLegalDateTime(startMsrmtDttm,
                        installationTimeZone, true, usageTimeZone, usageSubscriptionMoCode, usageSubscriptionId);
                String formattedEndMsrmtDttm = usageRuleHelper.retrieveFormattedLegalDateTime(endMsrmtDttm,
                        installationTimeZone, true, usageTimeZone, usageSubscriptionMoCode, usageSubscriptionId);
                addException(MSRMT_RETRIEVAL_EXP,MessageRepository.measuringComponentConsumptionNotFoundOnThePeriod(mc,
                            formattedStartMsrmtDttm, formattedEndMsrmtDttm));
                return;
            }
            if (measurementList.size() > 1) {
                //multiple measurements retrieved
                endMeasurement = findHighestPriorityMeasurement(measurementList, mc);
            } else {
                QueryResultRow mRow = measurementList.iterator().next();

                DateTime currentMeasurementDateTime = mRow.getDateTime(MSRMT_DTTM1);
                endMeasurement = new Measurement_Id(mc, currentMeasurementDateTime).getEntity();
            }
        }
        
        creatScalarDetailsEntry(mc, mctvi, determineSubscriptionDvcCfgPeriodOutputData);

    }
    
    private void determineEndMsrmtDateTimeAndInsertScalarDetailsForAddCons(MeasuringComponent mc,
            MeasuringComponentTypeValueIdentifier mctvi,
            DetermineSubscriptionDvcCfgPeriodOutputData determineSubscriptionDvcCfgPeriodOutputData) {
    	addConsumptionEndMsrmt = null;
        endMsrmtDttm = null;
        startMsrmtDttm = null;
        
        endMsrmtDttm = determineSubscriptionDvcCfgPeriodOutputData.getEndDateTime();
        startMsrmtDttm = determineSubscriptionDvcCfgPeriodOutputData.getStartDateTime();
        addConsumptionEndMsrmt = retrieveTotalMsrmtValue(mc, startMsrmtDttm, endMsrmtDttm,mctvi, null );
        logger.info("addConsumptionEndMsrmt "+addConsumptionEndMsrmt);
        if(addConsumptionEndMsrmt.compareTo(BigDecimal.ZERO)>0){
        	creatScalarDetailsEntry(mc, mctvi, determineSubscriptionDvcCfgPeriodOutputData);
        }	
        
    }

    /**
     * Using this method will find the start measurement record based input params
     * @param isUsageStartPeriod   
     * @param mc
     * @param mctvi
     * @param dateTime
     * @return
     */
    private Measurement findStartMeasurementRecord(MeasuringComponent mc, MeasuringComponentTypeValueIdentifier mctvi,
            DateTime dateTime) {

        Measurement stMeasurement = null;

        List<QueryResultRow> measurementList = retrieveMeasurements(mc, dateTime, dateTime, Bool.TRUE);

        if (isNull(measurementList) || (measurementList.size() < 1)) {

            return null;
        }
        QueryResultRow mRow = measurementList.iterator().next();
        DateTime currentMeasurementDateTime = mRow.getDateTime(MSRMT_DTTM1);
        stMeasurement = new Measurement_Id(mc, currentMeasurementDateTime).getEntity();

        return stMeasurement;
    }

    /**
     *  This method retrieve measurements based on given input params
     * @param mc
     * @param stDateTime
     * @param edDateTime
     * @param isStartDateTimeMeasurementIncluded
     * @return
     */
    private List<QueryResultRow> retrieveMeasurements(MeasuringComponent mc, DateTime stDateTime, DateTime edDateTime,
            Bool isStartDateTimeMeasurementIncluded) {

        StringBuilder sqlString = new StringBuilder();
        sqlString.append("from Measurement M1 ");
        sqlString.append("where ");
        sqlString.append("M1.id.measuringComponent= :mc ");
        sqlString.append("AND M1.useMeasurement <> :doNotUse ");

        if (isStartDateTimeMeasurementIncluded.isTrue()) {
            sqlString.append("AND M1.measurementLocalDateTime >= :startDateTime ");
        } else {
            sqlString.append("AND M1.measurementLocalDateTime > :startDateTime ");
        }
        sqlString.append("AND M1.measurementLocalDateTime <= :endDateTime ");
        sqlString.append("AND M1.id.measurementDateTime >= :startDateTimeMinusOneHour ");
        sqlString.append("AND M1.id.measurementDateTime <= :endDateTimePlusOneHour ");

        Query<QueryResultRow> query = createQuery(sqlString.toString(), "");

        query.bindEntity("mc", mc);
        query.bindLookup("doNotUse", UseMeasurementLookup.constants.DO_NOT_USE);
        query.bindDateTime("startDateTime", stDateTime);
        query.bindDateTime("endDateTime", edDateTime);
        query.bindDateTime("startDateTimeMinusOneHour", stDateTime.addHours(-1));
        query.bindDateTime("endDateTimePlusOneHour", edDateTime.addHours(1));

        query.addResult("MSRMT_VAL1", "M1.measurementValue");
        query.addResult("MSRMT_DTTM1", "M1.id.measurementDateTime");
        query.addResult("MSRMT_LOCAL_DTTM", "M1.measurementLocalDateTime");
        query.addResult("MSRMT_COND_FLG", "M1.measurementCondition");
        query.orderBy("MSRMT_DTTM1", Query.ASCENDING);

        return query.list();
    }

    /**
     * This method retrieve max measurement value based on given input params
     * @param mc
     * @param stDateTime
     * @param edDateTime
     * @param isStartDateTimeMeasurementIncluded
     * @param mctvi
     * @param isIncludeColName
     * @return
     */
    private BigDecimal retrieveMaxMsrmtValue(MeasuringComponent mc, DateTime stDateTime, DateTime edDateTime,
            Bool isStartDateTimeMeasurementIncluded, MeasuringComponentTypeValueIdentifier mctvi, Bool isIncludeColName) {

        StringBuilder sqlString = new StringBuilder();
        sqlString.append("from Measurement M1 ");
        sqlString.append("where ");
        sqlString.append("M1.id.measuringComponent= :mc ");
        sqlString.append("AND M1.useMeasurement <> :doNotUse ");

        if (isStartDateTimeMeasurementIncluded.isTrue()) {
            sqlString.append("AND M1.measurementLocalDateTime >= :startDateTime ");
        } else {
            sqlString.append("AND M1.measurementLocalDateTime > :startDateTime ");
        }
        sqlString.append("AND M1.measurementLocalDateTime <= :endDateTime ");
        sqlString.append("AND M1.id.measurementDateTime >= :startDateTimeMinusOneHour ");
        sqlString.append("AND M1.id.measurementDateTime <= :endDateTimePlusOneHour ");

        Query<QueryResultRow> query = createQuery(sqlString.toString(), "");

        query.bindEntity("mc", mc);
        query.bindLookup("doNotUse", UseMeasurementLookup.constants.DO_NOT_USE);
        query.bindDateTime("startDateTime", stDateTime);
        query.bindDateTime("endDateTime", edDateTime);
        query.bindDateTime("startDateTimeMinusOneHour", stDateTime.addHours(-1));
        query.bindDateTime("endDateTimePlusOneHour", edDateTime.addHours(1));

        String functionName = "MAX";
        String colName = mctvi.getId().getValueIdentifierType().getLookupValue().getValueName().substring(FIVE).trim();
        try {
            Integer.parseInt(colName);
        } catch (NumberFormatException ex) {
            colName = "";
        }
        if (isIncludeColName.isTrue()) {
            query.addResult("maxMsrmt", functionName + "(M1.measurementValue" + colName + ")");
        } else {
            query.addResult("maxMsrmt", functionName + "(M1.measurementValue" + ")");
        }

        return (BigDecimal) query.firstRow();
    }

    /**
     * This method retrieves total measurement value between start date time and end date time
     * @param mc
     * @param stDateTime
     * @param edDateTime
     * @param isStartDateTimeMeasurementIncluded
     * @param mctvi
     * @param isIncludeColNmae
     * @return
     */
    private BigDecimal retrieveTotalMsrmtValue(MeasuringComponent mc, DateTime stDateTime, DateTime edDateTime,
    			MeasuringComponentTypeValueIdentifier mctvi, String colName) {

        StringBuilder sqlString = new StringBuilder();
        sqlString.append("from Measurement M1 ");
        sqlString.append("where ");
        sqlString.append("M1.id.measuringComponent= :mc ");
        sqlString.append("AND M1.useMeasurement <> :doNotUse ");
        sqlString.append("AND M1.measurementLocalDateTime > :startDateTime ");
        sqlString.append("AND M1.measurementLocalDateTime <= :endDateTime ");
        sqlString.append("AND M1.id.measurementDateTime >= :startDateTimeMinusOneHour ");
        sqlString.append("AND M1.id.measurementDateTime <= :endDateTimePlusOneHour ");

        Query<QueryResultRow> query = createQuery(sqlString.toString(), "");

        query.bindEntity("mc", mc);
        query.bindLookup("doNotUse", UseMeasurementLookup.constants.DO_NOT_USE);
        query.bindDateTime("startDateTime", stDateTime);
        query.bindDateTime("endDateTime", edDateTime);
        query.bindDateTime("startDateTimeMinusOneHour", stDateTime.addHours(-1));
        query.bindDateTime("endDateTimePlusOneHour", edDateTime.addHours(1));

        String functionName = "SUM";
        
        if (notNull(mctvi)) {
        	colName = mctvi.getId().getValueIdentifierType().getLookupValue().getValueName().substring(FIVE).trim();
            try {
                Integer.parseInt(colName);
            } catch (NumberFormatException ex) {
                colName = "";
            }
        } 
        
        if(notBlank(colName)) {
        	query.addResult("sumMsrmt", functionName + "(M1.measurementValue" + colName + ")");
        } else {
            query.addResult("sumMsrmt", functionName + "(M1.measurementValue" + ")");
        }

        return (BigDecimal) query.firstRow();
    }
    
    private List<QueryResultRow> retrieveIntervalMeasurementList(MeasuringComponent mc, DateTime stDateTime, DateTime edDateTime) {

    StringBuilder sqlString = new StringBuilder();
    sqlString.append("from Measurement M1 ");
    sqlString.append("where ");
    sqlString.append("M1.id.measuringComponent= :mc ");
    sqlString.append("AND M1.useMeasurement <> :doNotUse ");
    sqlString.append("AND M1.measurementLocalDateTime > :startDateTime ");
    sqlString.append("AND M1.measurementLocalDateTime <= :endDateTime ");
    sqlString.append("AND M1.id.measurementDateTime >= :startDateTimeMinusOneHour ");
    sqlString.append("AND M1.id.measurementDateTime <= :endDateTimePlusOneHour ");

    Query<QueryResultRow> query = createQuery(sqlString.toString(), "");

    query.bindEntity("mc", mc);
    query.bindLookup("doNotUse", UseMeasurementLookup.constants.DO_NOT_USE);
    query.bindDateTime("startDateTime", stDateTime);
    query.bindDateTime("endDateTime", edDateTime);
    query.bindDateTime("startDateTimeMinusOneHour", stDateTime.addHours(-1));
    query.bindDateTime("endDateTimePlusOneHour", edDateTime.addHours(1));

    query.addResult("MSRMT_VAL2", "M1.measurementValue2");
    query.addResult("MSRMT_DTTM", "M1.id.measurementDateTime");
    query.addResult("MSRMT_COND_FLG", "M1.measurementCondition");
    
    return query.list();
}

    /**
     * This method retrieve max  measurement record based on given input params
     * @param mc
     * @param stDateTime
     * @param edDateTime
     * @param isStartDateTimeMeasurementIncluded
     * @param mctvi
     * @return
     */
    private List<QueryResultRow> retrieveMaxMeasurementRecord(MeasuringComponent mc, DateTime stDateTime,
            DateTime edDateTime, Bool isStartDateTimeMeasurementIncluded, MeasuringComponentTypeValueIdentifier mctvi) {

        BigDecimal maxMsrmtValue = retrieveMaxMsrmtValue(mc, stDateTime, edDateTime,
                isStartDateTimeMeasurementIncluded, mctvi, Bool.TRUE);

        if (isNull(maxMsrmtValue) || maxMsrmtValue.compareTo(BigDecimal.ZERO) == 0) {
            return null;
        }

        StringBuilder sqlString = new StringBuilder();
        sqlString.append("from Measurement M1 ");
        sqlString.append("where ");
        sqlString.append("M1.id.measuringComponent= :mc ");
        sqlString.append("AND M1.useMeasurement <> :doNotUse ");

        if (isStartDateTimeMeasurementIncluded.isTrue()) {
            sqlString.append("AND M1.measurementLocalDateTime >= :startDateTime ");
        } else {
            sqlString.append("AND M1.measurementLocalDateTime > :startDateTime ");
        }
        sqlString.append("AND M1.measurementLocalDateTime <= :endDateTime ");
        sqlString.append("AND M1.id.measurementDateTime >= :startDateTimeMinusOneHour ");
        sqlString.append("AND M1.id.measurementDateTime <= :endDateTimePlusOneHour ");

        String colName = mctvi.getId().getValueIdentifierType().getLookupValue().getValueName().substring(FIVE).trim();
        try {
            Integer.parseInt(colName);
        } catch (NumberFormatException ex) {
            colName = "";
        }
        sqlString.append("AND M1.measurementValue" + colName + "= :msrmtValue ");

        Query<QueryResultRow> query = createQuery(sqlString.toString(), "");

        query.bindEntity("mc", mc);
        query.bindLookup("doNotUse", UseMeasurementLookup.constants.DO_NOT_USE);
        query.bindDateTime("startDateTime", stDateTime);
        query.bindDateTime("endDateTime", edDateTime);
        query.bindDateTime("startDateTimeMinusOneHour", stDateTime.addHours(-1));
        query.bindDateTime("endDateTimePlusOneHour", edDateTime.addHours(1));
        query.bindBigDecimal("msrmtValue", maxMsrmtValue);

        query.addResult("MSRMT_VAL1", "M1.measurementValue");
        query.addResult("MSRMT_DTTM1", "M1.id.measurementDateTime");
        query.addResult("MSRMT_LOCAL_DTTM", "M1.measurementLocalDateTime");
        query.addResult("MSRMT_COND_FLG", "M1.measurementCondition");

        return query.list();
    }

    /**
     * This method finds Highest Priority Measurement based on given input params
     * @param measurementList
     * @param mc
     * @return
     */
    private Measurement findHighestPriorityMeasurement(List<QueryResultRow> measurementList, MeasuringComponent mc) {
        Measurement highPriorityMeasurement = null;
        BigInteger currentCondition = BigInteger.ZERO;
        BigInteger mCondition = BigInteger.ZERO;
        DateTime msrmtDTTM = null;

        if (notNull(measurementList) && measurementList.size() > 0) {
            Collections.reverse(measurementList);

            /*
              *   
              *   If End Read Option is 'Highest Quality Read'
              *          End Read = Highest Quality Measurement
              *   Else If End Read Option is 'Always Use Latest End Read'
              *          End Read = Latest Measurement
              *   Else If End Read Option is 'Latest End Read, Check Allow Estimate'
              *          If UT's Allow Estimates is 'Allow Estimate'
              *              End Read = Latest Measurement 
              *          Else If UT's Allow Estimates is 'Do Not Allow Estimate'
              *              End Read = Highest Quality Measurement    
              */
            
            
            if (isNull(endReadOption) || endReadOption.isBlankLookupValue() || endReadOption.isNo()) {
                for (Iterator<QueryResultRow> i = measurementList.iterator(); i.hasNext();) {
                    QueryResultRow mRow = i.next();
                    String mrmtCondition = mRow.getString(MSRMT_COND_FLG);
                    if (notBlank(mrmtCondition)) {
                        mCondition = new BigInteger(mrmtCondition);
                    }
                    if (notNull(allowEstimates) && allowEstimates.isDoNotAllowEstimate()
                            && notNull(getEstimateBottomRangeCondition())
                            && notNull(getEstimateTopRangeConditionRange())) {
                        // Estimate measurements are those whose condition code value is between the algorithm soft parm Start Estimate Range and End Estimate range inclusive
                        if (mCondition.compareTo(getEstimateBottomRangeCondition()) >= 0
                                && mCondition.compareTo(getEstimateTopRangeConditionRange()) <= 0) {
                            // Ignore current estimate measurement and move to the next measurement
                            continue;
                        }
                    }
                    if (mCondition.compareTo(currentCondition) > 0) {
                        currentCondition = mCondition;
                        msrmtDTTM = mRow.getDateTime(MSRMT_DTTM1);
                    }
                }
            } else if (endReadOption.isYes()) {
                msrmtDTTM = measurementList.get(0).getDateTime(MSRMT_DTTM1);
            } else if (endReadOption.isLatestEndReadCheckAllowEstimate()) {
                if (isNull(allowEstimates) || allowEstimates.isAllowEstimate()) {
                    msrmtDTTM = measurementList.get(0).getDateTime(MSRMT_DTTM1);
                } else {
                    for (Iterator<QueryResultRow> i = measurementList.iterator(); i.hasNext();) {
                        QueryResultRow mRow = i.next();
                        String mrmtCondition = mRow.getString(MSRMT_COND_FLG);
                        if (notBlank(mrmtCondition)) {
                            mCondition = new BigInteger(mrmtCondition);
                        }
                        if (notNull(getEstimateBottomRangeCondition()) && notNull(getEstimateTopRangeConditionRange())) {
                            // Estimate measurements are those whose condition code value is between the algorithm soft parm Start Estimate Range and End Estimate range inclusive
                            if (mCondition.compareTo(getEstimateBottomRangeCondition()) >= 0
                                    && mCondition.compareTo(getEstimateTopRangeConditionRange()) <= 0) {
                                // Ignore current estimate measurement and move to the next measurement
                                continue;
                            }
                        }
                        msrmtDTTM = mRow.getDateTime(MSRMT_DTTM1);
                        break;
                    }
                }
            }
            if (notNull(msrmtDTTM)) {
                highPriorityMeasurement = new Measurement_Id(mc, msrmtDTTM).getEntity();
            }

        }

        return highPriorityMeasurement;
    }

    /**
     *  This method insert scalar details into DB
     * @param mc
     * @param mctvi
     * @param determineSubscriptionDvcCfgPeriodOutputData
     */
    private void creatScalarDetailsEntry(MeasuringComponent mc, MeasuringComponentTypeValueIdentifier mctvi,
            DetermineSubscriptionDvcCfgPeriodOutputData determineSubscriptionDvcCfgPeriodOutputData) {

        BigDecimal usePercentMultiplier = roundingHelper.scaleValue(FIELD_NAME, new BigDecimal(
                determineSubscriptionDvcCfgPeriodOutputData.getUsspUsePercent().toString()).multiply(new BigDecimal(
                0.01), mathContext));

        BigDecimal appliedMultiplier;
        BigDecimal mcChannelMultiplier = mc.getMeasuringComponentMultiplier();
        BigDecimal installEventConstant = determineSubscriptionDvcCfgPeriodOutputData.getInstallEvent()
                .getInstallationConstant();
        if (mcChannelMultiplier.doubleValue() == 0) {
            mcChannelMultiplier = BigDecimal.ONE;
        }
        if (installEventConstant.doubleValue() == 0) {
            installEventConstant = BigDecimal.ONE;
        }
        appliedMultiplier = mcChannelMultiplier.multiply(installEventConstant);
        appliedMultiplier = roundingHelper.scaleValue(FIELD_NAME, appliedMultiplier);
        
        Element readingDetailsNew = new DefaultElement(SCALAR_DETAILS_LIST);
    	readingDetailsNew.addElement(SEQUENCE).setText(String.valueOf(readingDetailsSequence));
    	readingDetailsNew.addElement(SQ_TYPE).setText(UsageServiceQuantityTypeLookup.constants.MEASURING_COMPONENT.value());
    	if(notNull(uom)){
    		readingDetailsNew.addElement(UOM).setText(uom.getId().getIdValue());
    		readingDetailsNew.addElement(MEASURES_PEAK_QUANTITY).setText(uom.getMeasuresPeakQuantity().getLookupValue().fetchIdFieldValue());
    		readingDetailsNew.addElement(FINAL_UOM).setText(uom.getId().getIdValue());
    	}
    	if(notNull(tou)){
    		readingDetailsNew.addElement(TOU).setText(tou.getId().getIdValue());
    		readingDetailsNew.addElement(FINAL_TOU).setText(tou.getId().getIdValue());
    	}
    	if(notNull(sqi)){
    		readingDetailsNew.addElement(SQI).setText(sqi.getId().getIdValue());
    		readingDetailsNew.addElement(FINAL_SQI).setText(sqi.getId().getIdValue());
    	}
    	readingDetailsNew.addElement(SP_ID).setText(determineSubscriptionDvcCfgPeriodOutputData.getServicePoint().getId().getIdValue());
    	readingDetailsNew.addElement(MEASURING_COMPONENT_ID).setText(mc.getId().getIdValue());
    	readingDetailsNew.addElement(SP_HOW_TO_USE).setText(determineSubscriptionDvcCfgPeriodOutputData.getUsspUsageFlag().
    			getLookupValue().fetchIdFieldValue());
    	readingDetailsNew.addElement(USE_PERCENT).setText(String.valueOf(determineSubscriptionDvcCfgPeriodOutputData.getUsspUsePercent()));
    	readingDetailsNew.addElement(MC_HOW_TO_USE).setText(mc.getHowToUse().getLookupValue().fetchIdFieldValue());
    	readingDetailsNew.addElement(APPLIED_MULTIPLIER).setText(appliedMultiplier.toString());
    	if(notNull(startMsrmtDttm)){
    		readingDetailsNew.addElement(START_DTTM).setText(startMsrmtDttm.toString());
    	}
    	if(notNull(startMeasurement)){
	    	readingDetailsNew.addElement(START_MEASUREMENT).setText(String.valueOf(startMeasurement.getReadingValue()));
	    	readingDetailsNew.addElement(START_MEASUREMENT_COND).setText(startMeasurement.getMeasurementCondition());
    	}
    	readingDetailsNew.addElement(END_DTTM).setText(endMsrmtDttm.toString());
        
    	if(!isBlankOrNull(getAdditionalConsumptionUOM()) && getAdditionalConsumptionUOM().trim().equalsIgnoreCase(uom.getId().getIdValue().trim()) )
    	{
    		readingDetailsNew.addElement(QUANTITY).setText(String.valueOf(addConsumptionEndMsrmt));
        	readingDetailsNew.addElement(FINAL_QUANTITY).setText(String.valueOf(addConsumptionEndMsrmt));
        	readingDetailsNew.addElement(END_MEASUREMENT).setText(String.valueOf(addConsumptionEndMsrmt.divide(appliedMultiplier)));
	     	readingDetailsNew.addElement(END_MEASUREMENT_COND).setText(String.valueOf(505001));
    	}
    	else{
	    	readingDetailsNew.addElement(QUANTITY).setText(String.valueOf(retrieveQuantityBasedOnUOM(mc, mctvi, usePercentMultiplier)));
	    	readingDetailsNew.addElement(FINAL_QUANTITY).setText(String.valueOf(retrieveFinalQuantityBasedOnUOM(mc, mctvi, usePercentMultiplier))); 
	     	readingDetailsNew.addElement(END_MEASUREMENT).setText(String.valueOf(endMeasurement.getReadingValue()));
	     	readingDetailsNew.addElement(END_MEASUREMENT_COND).setText(endMeasurement.getMeasurementCondition());
    	}
    	readingDetailsNew.addElement(USAGE_GROUP).setText(usageGroup.getId().getTrimmedValue());
    	readingDetailsNew.addElement(USAGE_RULE).setText(usageRule.getId().getUsageRule().trim());
    	
    	readingDetailsList.add(readingDetailsNew);
        readingDetailsSequence++;
    }
    
    /**
     *  This method will retrieves the next read details sequence
     */
    @SuppressWarnings("unchecked")
	private void retrieveReadDetailsSequence(){
    	Element scalarDetailsGroup = usageTransactionBOInstance.getElement().element(SCALAR_DETAILS);
    	if(notNull(scalarDetailsGroup)){
    		List<Element> scalarDetailsList = scalarDetailsGroup.elements();
    		if(notNull(scalarDetailsList) && scalarDetailsList.size() > 0){
    			for (Element scalarDetails : scalarDetailsList){
    				int scalarDetailsSeq = Integer.parseInt(scalarDetails.elementText(SEQUENCE));
    				if(readingDetailsSequence < scalarDetailsSeq){
    					readingDetailsSequence = scalarDetailsSeq;
    				}
    			}
    			readingDetailsSequence++;
    		}
    	}else{
    		scalarDetailsGroup = usageTransactionBOInstance.getDocument().addElement(SCALAR_DETAILS);
    		readingDetailsSequence = 1;
    	}
    }

    /**
     *  This method process the Scalar Details Records and insert into Service Quantities if it is new one else it updates existing SQ record
     * @param usagePeriodsListNode
     */
    @SuppressWarnings("unchecked")
	private void buildUsagePeriods(Element usagePeriod) {
    	
    	if(isNull(readingDetailsList) || readingDetailsList.size() == 0){
    		return;
    	}
    	
    	// Adjust usage period end date/time
    	if(notNull(latestMeasurementEndDateTime) 
    			&& usagePeriodEndDateTime.compareTo(latestMeasurementEndDateTime) > 0){
    		usagePeriod.element(END_DTTM).setText(latestMeasurementEndDateTime.toString());
    	}
    	
    	Element sqsGroup = usagePeriod.element(SQS_GROUP);
    	if(isNull(sqsGroup)){
    		sqsGroup = usagePeriod.addElement(SQS_GROUP);
    	}
    	
    	if(notNull(sqsGroup)){
    		List<Element> sqsList = sqsGroup.elements();
    		
    		DateTime lastYearStartDateTime = new DateTime(usagePeriodEndDateTime.getYear()-1, 1, 1, 0, 0, 0);
    		DateTime lastYearEndDateTime = new DateTime(usagePeriodEndDateTime.getYear()-1, 12, 31, 0, 0, 0);
    		DateTime currentYearStartDateTime = new DateTime(usagePeriodEndDateTime.getYear(), 1, 1, 0, 0, 0);
    		DateTime currentYearEndDateTime = usagePeriodEndDateTime;
    		long daysDifference = usagePeriodEndDateTime.difference(usagePeriodStartDateTime).getTotalDays();
    		BigDecimal activeConsumption = null;
    		
    		int usagePeriodSqSequence = (notNull(sqsList) && sqsList.size() > 0) ? (sqsList.size() + 1) : 1;
    		
    		for(Element readingDetails : readingDetailsList){
    			//Skip the MC if how to use is Check
    			MeasuringComponentUsageLookup mcHowToUse = LookupHelper.getLookupInstance(MeasuringComponentUsageLookup.class, 
    					readingDetails.elementText(MC_HOW_TO_USE));
    			if (notNull(mcHowToUse) && mcHowToUse.isCheck()) {
                    continue;
                }
    			
	    		String scalarReadUom = readingDetails.elementText(UOM);
	    		String scalarReadTou = readingDetails.elementText(TOU);
	    		String scalarReadSqi = readingDetails.elementText(SQI);
	    		String scalarReadSp = readingDetails.elementText(SP_ID);
	    		String scalarReadMc = readingDetails.elementText(MEASURING_COMPONENT_ID);
	    		DateTime endDateTime = DateTime.fromIso(readingDetails.elementText(END_DTTM));
	    		
	    		if (!isEstimate) {
                    checkforEstimateBottomAndTopRangeConditions(readingDetails.elementText(END_MEASUREMENT_COND));
                }
	    		
	    		if(notNull(latestMeasurementEndDateTime) 
	    				&& endDateTime.compareTo(latestMeasurementEndDateTime) > 0){
	    			endDateTime = latestMeasurementEndDateTime;
	    		}
	    		
	    		// Check the MC and US/SP How To Use flag values 
	    		boolean isSubtractive = isMcSpHowToUseAreSubtractive(readingDetails.elementText(MC_HOW_TO_USE) , 
	    				readingDetails.elementText(SP_HOW_TO_USE));  
	    		
    			BigDecimal finalQuantity = new BigDecimal(readingDetails.elementText(FINAL_QUANTITY));
    			if(isSubtractive){
    				finalQuantity = finalQuantity.negate();
    			}
    			finalQuantity = usageRuleHelper.scaleServiceQuantityValue(finalQuantity, usageRuleBoInstance);
    			
    			ServicePointD1 servicePoint = new ServicePointD1_Id(readingDetails.elementText(SP_ID)).getEntity();
		        MeasuringComponent measuringComponent = new MeasuringComponent_Id(readingDetails.elementText(MEASURING_COMPONENT_ID)).getEntity();
		        
		        String routeId = servicePoint.getMeasurementCycleRoute();
		        String deviceId = null;
		        String deviceSerialNumber = null;
		        String deviceManufacturer = null;
		        String meterType = null;
		        String currentTransformerRatio = "0";
		        String voltageTransformerRatio = "0";
    			List<String> data = retrieveDeviceConfigurationDetails(measuringComponent.getDeviceConfigurationId());
    			if(notNull(data) && data.size() == 6){
    				deviceId = data.get(0);
    				deviceSerialNumber = data.get(1);
    				deviceManufacturer = data.get(2);
    				meterType = data.get(3);
    				voltageTransformerRatio = data.get(4);
    				currentTransformerRatio = data.get(5);
    			}
    			BigDecimal lastYearConsumption = retrieveTotalMsrmtValue(measuringComponent, lastYearStartDateTime, 
    					lastYearEndDateTime, null, null);
	        	lastYearConsumption = usageRuleHelper.scaleServiceQuantityValue(lastYearConsumption, usageRuleBoInstance);
	        	BigDecimal currentYearConsumption = retrieveTotalMsrmtValue(measuringComponent, currentYearStartDateTime, 
	        				currentYearEndDateTime, null, null);
	        	currentYearConsumption = usageRuleHelper.scaleServiceQuantityValue(currentYearConsumption, usageRuleBoInstance);
	        	String firstIndex = readingDetails.elementText(START_MEASUREMENT);
	        	String lastIndex = readingDetails.elementText(END_MEASUREMENT);
	        	BigDecimal dailyAverageUsage = finalQuantity.divide(new BigDecimal(daysDifference));
	        	dailyAverageUsage = usageRuleHelper.scaleServiceQuantityValue(dailyAverageUsage, usageRuleBoInstance);
	        	String firstReadingDate = readingDetails.elementText(START_DTTM);	
	        	String lastReadingDate = readingDetails.elementText(END_DTTM);
	        	
	        	if(isNull(activeConsumption) && notBlank(getActiveConsumptionUom()) && notBlank(scalarReadUom) 
	        			&& scalarReadUom.equals(getActiveConsumptionUom()) 
	        			&& isBlankOrNull(scalarReadTou) && isBlankOrNull(scalarReadSqi)){
	        		activeConsumption = finalQuantity;
	        	}
	        	
	        	BigDecimal transformerLoss = retrieveTotalMsrmtValue(measuringComponent, 
						usagePeriodStartDateTime, usagePeriodEndDateTime, null, "1");
    			
		        Element newSqEntry = new DefaultElement(SQS_LIST);
		        newSqEntry.addElement(SQ_SEQUENCE).setText(String.valueOf(usagePeriodSqSequence));
		        newSqEntry.addElement(SQ_TYPE).setText(UsageServiceQuantityTypeLookup.constants.MEASURING_COMPONENT.value());
		        if(notBlank(scalarReadSp)){
		        	newSqEntry.addElement(SP_ID).setText(scalarReadSp);
		        }
		        if(notBlank(scalarReadMc)){
		        	newSqEntry.addElement(MEASURING_COMPONENT_ID).setText(readingDetails.elementText(MEASURING_COMPONENT_ID));
		        }
		        newSqEntry.addElement(QUANTITY).setText(String.valueOf(finalQuantity));
		        if(notBlank(scalarReadUom)){
		        	newSqEntry.addElement(UOM).setText(scalarReadUom);
		        }
		        if(notBlank(scalarReadTou)){
		        	newSqEntry.addElement(TOU).setText(scalarReadTou);
		        }
		        if(notBlank(scalarReadSqi)){
		        	newSqEntry.addElement(SQI).setText(scalarReadSqi);
		        } 
		        newSqEntry.addElement(DATA_QLTY_ASSMT).setText(DataQualityAssessmentLookup.constants.NO_ASSESSMENT.value());
		        newSqEntry.addElement(USAGE_GROUP).setText(readingDetails.elementText(USAGE_GROUP));
		        newSqEntry.addElement(USAGE_RULE).setText(readingDetails.elementText(USAGE_RULE));
		        
		        //Populate Usage Details
		        UnitOfMeasureD1 scalarUom = new UnitOfMeasureD1_Id(scalarReadUom).getEntity();
		        
		        Element usageDetailsGroup = newSqEntry.element(USAGE_DETAILS);
		    	if(isNull(usageDetailsGroup)){
		    		usageDetailsGroup = newSqEntry.addElement(USAGE_DETAILS);
		    	}
		    	usageDetailsGroup.addElement("spId").setText(readingDetails.elementText(SP_ID));	
		    	if(notBlank(routeId)){
		    		usageDetailsGroup.addElement("routeId").setText(routeId);
		    	}
		    	if(notBlank(deviceSerialNumber) && !"null".equals(deviceSerialNumber)){
		    		usageDetailsGroup.addElement("serialNumber").setText(deviceSerialNumber);
		    	}
		    	if(notNull(currentYearConsumption)){
		    		usageDetailsGroup.addElement("currentYearConsumption").setText(currentYearConsumption.toString());
		    	}
		    	if(notNull(lastYearConsumption)){
		    		usageDetailsGroup.addElement("lastYearConsumption").setText(lastYearConsumption.toString());
		    	}
		    	usageDetailsGroup.addElement("meterId").setText(deviceId);
		    	if(notBlank(deviceManufacturer) && !"null".equals(deviceManufacturer)){
		    		usageDetailsGroup.addElement("meterBrand").setText(deviceManufacturer);
		    	}
		    	if(notBlank(meterType) && !"null".equals(meterType)){
		    		usageDetailsGroup.addElement("meterType").setText(meterType);
		    	}
		    	usageDetailsGroup.addElement("lastIndex").setText(lastIndex);
		    	if(notBlank(firstIndex)){
		    		usageDetailsGroup.addElement("firstIndex").setText(firstIndex);
		    	}
		    	usageDetailsGroup.addElement("multiplier").setText(readingDetails.elementText(APPLIED_MULTIPLIER));
		    	usageDetailsGroup.addElement("consumption").setText(finalQuantity.toString());
		    	if(notNull(transformerLoss)){
		    		usageDetailsGroup.addElement("transformerLoss").setText(transformerLoss.toString());
		    	}
		    	if(notNull(dailyAverageUsage)){
		    		usageDetailsGroup.addElement("dailyAverageUsage").setText(dailyAverageUsage.toString());
		    	}
		    	usageDetailsGroup.addElement("lastReadingDate").setText(lastReadingDate);
		    	if(notBlank(firstReadingDate)){
		    		usageDetailsGroup.addElement("firstReadingDate").setText(firstReadingDate);
		    	}
		    	
		    	if(notNull(scalarUom) && scalarUom.getMeasuresPeakQuantity().isMeasurePeakQuantity()){
			    	usageDetailsGroup.addElement("demandIndex").setText(lastIndex);
			    	usageDetailsGroup.addElement("demandMultiplier").setText(readingDetails.elementText(APPLIED_MULTIPLIER));
			    	usageDetailsGroup.addElement("demandConsumption").setText(finalQuantity.toString());
		    	}
		    	
		    	usageDetailsGroup.addElement("currentTransformerRatio").setText(String.valueOf(currentTransformerRatio));
		    	usageDetailsGroup.addElement("voltageTransformerRatio").setText(String.valueOf(voltageTransformerRatio));
		    	if(isBlankOrNull(firstIndex)){
		    		usageDetailsGroup.addElement("indexDifference").setText(lastIndex);
		    	}else{
		    		usageDetailsGroup.addElement("indexDifference").setText((new BigDecimal(lastIndex).subtract(new BigDecimal(firstIndex))).toString());
		    	}
		    	
		    	if(notBlank(getInductiveReactiveConsumptionUom()) && getInductiveReactiveConsumptionUom().equals(scalarReadUom)
		    			&& notNull(activeConsumption)){
		    		//inductive ratio = (inductive reactive consumption / active consumption(including min bill amount and transformer loss)*100
		    		BigDecimal divisor = activeConsumption;
		    		if(notNull(transformerLoss)){
		    			divisor = divisor.add(transformerLoss);
		    		}
		    		BigDecimal inductiveRatio = (finalQuantity.divide(divisor)).multiply(new BigDecimal(100));
		    		inductiveRatio = usageRuleHelper.scaleServiceQuantityValue(inductiveRatio, usageRuleBoInstance);
		    		
		    		usageDetailsGroup.addElement("inductiveRatio").setText(inductiveRatio.toString());
		    	}
		    	if(notBlank(getCapacitiveReactiveConsumptionUom()) && getCapacitiveReactiveConsumptionUom().equals(scalarReadUom)
		    			&& notNull(activeConsumption)){
		    		//capacitive ratio= (capacitive reactive consumption/active consumption(including min bill amount and transformer loss)*100
		    		BigDecimal divisor = activeConsumption;
		    		if(notNull(transformerLoss)){
		    			divisor = divisor.add(transformerLoss);
		    		}
		    		BigDecimal capacitiveRatio = (finalQuantity.divide(divisor)).multiply(new BigDecimal(100));
		    		capacitiveRatio = usageRuleHelper.scaleServiceQuantityValue(capacitiveRatio, usageRuleBoInstance);
		    		
		    		usageDetailsGroup.addElement("capacitiveRatio").setText(capacitiveRatio.toString());
		    	}
		    	
		    	if(notNull(billType) && billType.isGenerationCustomer()){
		    		if(isTOUCustomer){
			    		if(notNull(netGenerationTouMap) && netGenerationTouMap.size() > 0){
			    			for (String tou : netGenerationTouMap.keySet()){
			    				if(notNull(scalarReadTou) && tou.contentEquals(scalarReadTou)){
			    					String netGeneration = netGenerationTouMap.get(tou);
			    			        logger.info("Net Generation"+netGeneration);
			    					if(notNull(netGeneration)){
						    			usageDetailsGroup.addElement("netGeneration").setText(netGeneration);
						    		}
			    					break;
			    				}
			    			}
			    			if(isNull(scalarReadTou) && (scalarReadUom.contentEquals("KWH"))){
			    				String netGenerationSum = netGenerationTouMap.get("touSum");
			    				if(notNull(netGenerationSum)){
					    			usageDetailsGroup.addElement("netGeneration").setText(netGenerationSum);
					    		}
			    			}
			    		}
			    		if(notNull(netConsumptionTouMap) && netConsumptionTouMap.size() > 0){
			    			 Element consumptionElement = usageDetailsGroup.element("consumption");
			    			for (String tou : netConsumptionTouMap.keySet()){
			    				if(notNull(scalarReadTou) && tou.contentEquals(scalarReadTou)){
			    					String netConsumption = netConsumptionTouMap.get(tou);
			    			        logger.info("Net Consumption"+netConsumption);
			    					if(notNull(netConsumption)){
						    			//usageDetailsGroup.addElement("consumption").setText(netConsumption);
			    						if(notNull(consumptionElement)){
			    							consumptionElement.setText(netConsumption);
			    					   }
						    		}
			    					break;
			    				}
			    			}
			    			if(isNull(scalarReadTou) && (scalarReadUom.contentEquals("KWH"))){
			    				String netConsumptionSum = netConsumptionTouMap.get("touSum");
			    				if(notNull(netConsumptionSum)){
			    					if(notNull(consumptionElement)){
		    							consumptionElement.setText(netConsumptionSum);
			    				   }
					    		}
			    			}
			    		}
		    		}
		    		else{
		    			if(notNull(intervalMcMap) && intervalMcMap.size() > 0){
			    			String intervalMc = intervalMcMap.get(readingDetails.elementText(MEASURING_COMPONENT_ID).trim());
			    			if(notBlank(intervalMc)){
				    			MeasuringComponent intervalMeasuringComponent = new MeasuringComponent_Id(intervalMc).getEntity();
					    		BigDecimal netGeneration = retrieveTotalMsrmtValue(intervalMeasuringComponent,
												usagePeriodStartDateTime, usagePeriodEndDateTime, null, "2");
					    		if(notNull(netGeneration)){
					    			usageDetailsGroup.addElement("netGeneration").setText(netGeneration.toString());
					    		}
			    			}
			    		}	
		    		}
		    		if(notNull(scalarUom) && scalarUom.getMeasuresPeakQuantity().isMeasurePeakQuantity()){
		    			usageDetailsGroup.addElement("generationDemand").setText(lastIndex);
		    		}
		    	}
		    	
		    	usagePeriodSqSequence++;
		    	
		        sqsGroup.addElement(newSqEntry.getQName()).appendContent(newSqEntry);
	    		
	    		Element scalarDetailsGroup = usageTransactionBOInstance.getElement().element(SCALAR_DETAILS);
	    		scalarDetailsGroup.addElement(readingDetails.getQName()).appendContent(readingDetails);
	    	}
    	}
    }
    
    private List<String> retrieveDeviceConfigurationDetails(DeviceConfiguration_Id deviceConfigurationId) {
    	
    	String deviceCfgId = deviceConfigurationId.getIdValue();
    	if(deviceConfigurationMap.containsKey(deviceCfgId)){
    		return deviceConfigurationMap.get(deviceCfgId);
    	}
    	
    	List<String> data = new ArrayList<String>();
    	
    	//Device Id
    	DeviceConfiguration deviceConfiguration = deviceConfigurationId.getEntity();
    	Device device = deviceConfiguration.getDevice();
    	data.add(device.getId().getIdValue());
    	
    	//Device Serial Number
    	String deviceSerialNumber = "null";
    	DeviceIdentifiers deviceIdentifiers = device.getIdentifiers();
    	if(notNull(deviceIdentifiers)){
    		Iterator<DeviceIdentifier> deviceIdentifierItr = deviceIdentifiers.iterator();
            for (Iterator<DeviceIdentifier> iter = deviceIdentifierItr; iter.hasNext();) {
                DeviceIdentifier deviceIdentifier = deviceIdentifierItr.next();
                if (DeviceIdentifierTypeLookup.constants.SERIAL_NUMBER.equals(deviceIdentifier.fetchIdDeviceIdentifierType())) {
                	deviceSerialNumber = deviceIdentifier.getIdValue();
                	break;
                }
            }
    	}
    	data.add(deviceSerialNumber);
    	
    	//Device Manufacturer
    	String deviceManufacturer = "null";
    	if(notNull(device.getManufacturerId())){
    		deviceManufacturer = device.getManufacturerId().getIdValue();
    	}
    	data.add(deviceManufacturer);
    	
    	//Meter Type
    	String meterType = null;
    	try{
	    	String deviceCfgType = deviceConfiguration.getDeviceConfigurationType().getId().getIdValue();
	        BusinessObjectInstance meterTypeLookupInstance = BusinessObjectInstance.create("CM-MeterTypeLookup");
	        meterTypeLookupInstance.set("bo", "CM-MeterTypeLookup");
	        meterTypeLookupInstance.set("lookupValue", deviceCfgType);
	        meterTypeLookupInstance = BusinessObjectDispatcher.read(meterTypeLookupInstance);
	        meterType = meterTypeLookupInstance.getLookup("meterType").getLookupValue().fetchLanguageDescription();
    	} catch (Exception ex) {
    		//empty
    	}
    	if(isBlankOrNull(meterType)){
        	meterType = "null";
        }
        data.add(meterType);
    	
    	BigDecimal voltageTransformerRatio = BigDecimal.ZERO;
    	BigDecimal currentTransformerRatio = BigDecimal.ZERO;
    	try {
    		
    		String dcBoDataArea = deviceConfiguration.getBusinessObjectDataArea();
    		if(notBlank(dcBoDataArea)){
    			//Voltage Transformer Ratio
    			String voltageRatio1 = "1";
    			String voltageRatio2 = "1";
    			int voltageRatio1StartIndex = dcBoDataArea.indexOf("<voltageRatio1>");
    			if(voltageRatio1StartIndex > 0){
    				voltageRatio1StartIndex += 15;
    			}
    			int voltageRatio1EndIndex = dcBoDataArea.indexOf("</voltageRatio1>");
    			if(voltageRatio1EndIndex > 0){
    				voltageRatio1 = dcBoDataArea.substring(voltageRatio1StartIndex, voltageRatio1EndIndex);
    			}
    			int voltageRatio2StartIndex = dcBoDataArea.indexOf("<voltageRatio2>");
    			if(voltageRatio2StartIndex > 0){
    				voltageRatio2StartIndex += 15;
    			}
    			int voltageRatio2EndIndex = dcBoDataArea.indexOf("</voltageRatio2>");
    			if(voltageRatio2EndIndex > 0){
    				voltageRatio2 = dcBoDataArea.substring(voltageRatio2StartIndex, voltageRatio2EndIndex);
    				
    				double voltageTransformer = (Double.parseDouble(voltageRatio1) / Double.parseDouble(voltageRatio2));
    				voltageTransformerRatio = usageRuleHelper.scaleServiceQuantityValue(new BigDecimal(String.valueOf(voltageTransformer)), usageRuleBoInstance);
    			}
    			
    			//Current Transformer Ratio
    			String currentRatio1 = "1";
    			String currentRatio2 = "1";
    			int currentRatio1StartIndex = dcBoDataArea.indexOf("<currentRatio1>");
    			if(currentRatio1StartIndex > 0){
    				currentRatio1StartIndex += 15;
    			}
    			int currentRatio1EndIndex = dcBoDataArea.indexOf("</currentRatio1>");
    			if(currentRatio1EndIndex > 0){
    				currentRatio1 = dcBoDataArea.substring(currentRatio1StartIndex, currentRatio1EndIndex);
    			}
    			
    			int currentRatio2StartIndex = dcBoDataArea.indexOf("<currentRatio2>");
    			if(currentRatio2StartIndex > 0){
    				currentRatio2StartIndex += 15;
    			}
    			int currentRatio2EndIndex = dcBoDataArea.indexOf("</currentRatio2>");
    			if(currentRatio2EndIndex > 0){
    				currentRatio2 = dcBoDataArea.substring(currentRatio2StartIndex, currentRatio2EndIndex);
    				
    				double currentTransformer = (Double.parseDouble(currentRatio1) / Double.parseDouble(currentRatio2));
    				currentTransformerRatio = usageRuleHelper.scaleServiceQuantityValue(new BigDecimal(String.valueOf(currentTransformer)), usageRuleBoInstance);
    			}
    		}
    	} catch (Exception ex) {
    		logger.debug("Exception while reading device configuration bo data area : " + ex.getMessage());
        }
    	
    	data.add(String.valueOf(voltageTransformerRatio));
    	data.add(String.valueOf(currentTransformerRatio));
    	deviceConfigurationMap.put(deviceCfgId, data);
    	
    	return data;
    }

    /**
     * This method will compare two elements and return true or false if they match or does not match respectively 
     * 
     * @param element1
     * @param element2
     * @return
     */
    private boolean checkIfEquals(String element1, String element2) {
        if (isBlankOrNull(element1) && isBlankOrNull(element2)) {
            return true;
        }
        if (notBlank(element1) && notBlank(element2) 
        		&& element1.equals(element2)) {
            return true;
        }
        return false;
    }
    
    /**
     * This method will check for the MC and US/SP How To Use flag values for subtractive or additive
     * 
     * @param readingDetailsMcHowToUse
     * @param readingDetailsSpHowToUse
     * @return
     */
    private boolean isMcSpHowToUseAreSubtractive(String readingDetailsMcHowToUse, 
    		String readingDetailsSpHowToUse){
    	MeasuringComponentUsageLookup mcHowToUse = LookupHelper.getLookupInstance(MeasuringComponentUsageLookup.class, 
    			readingDetailsMcHowToUse);
		UsageD1Lookup spHowToUse = LookupHelper.getLookupInstance(UsageD1Lookup.class, 
				readingDetailsSpHowToUse);
		
		if(spHowToUse.isAdd() 
				&& (mcHowToUse.isAdditive() || mcHowToUse.isPeak())){
			return false;
		}else if(spHowToUse.isSubtract() && mcHowToUse.isSubtractive()){
			return false;
		}
		
		return true;
    }

    /**
     * This method retrieves the quantity from D1_MSRMT  based on usage type (i.e consumptive or non-consumptive) 
     *  and startMsrmtDateTime and endMsrmtDateTime
     *  
     * @param mc
     * @param mctvi
     * @param usePercentMultiplier
     * @return
     */
    private BigDecimal retrieveQuantityBasedOnUOM(MeasuringComponent mc, MeasuringComponentTypeValueIdentifier mctvi,
            BigDecimal usePercentMultiplier) {
        BigDecimal quantity = BigDecimal.ZERO;
        if (mctvi.fetchUnitOfMeasure().getMeasuresPeakQuantity().isMeasurePeakQuantity()) {
            quantity = roundingHelper.scaleValue(FIELD_NAME, retrieveMaxMsrmtValue(mc, startMsrmtDttm, endMsrmtDttm,
                    Bool.FALSE, mctvi, Bool.FALSE).multiply(usePercentMultiplier, mathContext));
        } else {
            quantity = retrieveTotalMsrmtValue(mc, startMsrmtDttm, endMsrmtDttm, null, null);
        }
        return quantity;
    }

    /**
     * This method retrieves the final quantity from D1_MSRMT  based on usage type (i.e consumptive or non-consumptive) and 
     *   startMsrmtDateTime and endMsrmtDateTime
     *   
     * @param mc
     * @param mctvi
     * @param usePercentMultiplier
     * @return
     */
    private BigDecimal retrieveFinalQuantityBasedOnUOM(MeasuringComponent mc,
            MeasuringComponentTypeValueIdentifier mctvi, BigDecimal usePercentMultiplier) {
        BigDecimal finalQuantity = BigDecimal.ZERO;

        if (mctvi.fetchUnitOfMeasure().getMeasuresPeakQuantity().isMeasurePeakQuantity()) {
            finalQuantity = roundingHelper.scaleValue(FIELD_NAME, retrieveMaxMsrmtValue(mc, startMsrmtDttm,
                    endMsrmtDttm, Bool.FALSE, mctvi, Bool.TRUE).multiply(usePercentMultiplier, mathContext));
        } else {
            finalQuantity = retrieveTotalMsrmtValue(mc, startMsrmtDttm, endMsrmtDttm, mctvi, null);
        }
        return finalQuantity;

    }

    /**
     * This method will derives the EndMsrmtSearchPeriodStDateTime based on given inputs
     * @param determineSubscriptionDvcCfgPeriodOutputData
     * @return
     */
    private DateTime deriveEndMsrmtSearchPeriodStDateTime(
            DetermineSubscriptionDvcCfgPeriodOutputData determineSubscriptionDvcCfgPeriodOutputData) {
        DateTime endMsrmtSearchPeriodStDateTime = null;

        if (determineSubscriptionDvcCfgPeriodOutputData.getIsUsageEndPeriod().isFalse()
                && scalarMcEndDateTimeFrom.compareTo(determineSubscriptionDvcCfgPeriodOutputData.getEndDateTime()) > 0) {

            endMsrmtSearchPeriodStDateTime = determineSubscriptionDvcCfgPeriodOutputData.getEndDateTime().subtract(
            		offsetHours);
        } else {
            endMsrmtSearchPeriodStDateTime = scalarMcEndDateTimeFrom.subtract(offsetHours);
        }
        return endMsrmtSearchPeriodStDateTime;
    }

    /**
     *   Check if the end measurements measurement condition falls in between Regular Bottom and Top Range condition.
     *    If Yes, populate Is Estimate as NO.
     *    If No, then check if any measurements measurement condition falls in between Estimate Bottom and Top Range condition.
     *    If Yes, populate Is Estimate as YES.
     * 
     */
    private void checkforEstimateBottomAndTopRangeConditions(String endMsrmtCondition) {
    	if(isBlankOrNull(endMsrmtCondition)){
    		return;
    	}
    	
        BigInteger msrmtConditionBigInt = new BigInteger(endMsrmtCondition);

        if ((notNull(getRegularBottomRangeCondition()) && msrmtConditionBigInt.compareTo(getRegularBottomRangeCondition()) >= 0)
                && (notNull(getRegularTopRangeCondition()) && msrmtConditionBigInt.compareTo(getRegularTopRangeCondition()) <= 0)) {
            return;
        }
        if ((notNull(getEstimateBottomRangeCondition()) && msrmtConditionBigInt.compareTo(getEstimateBottomRangeCondition()) >= 0)
                && (notNull(getEstimateTopRangeConditionRange()) && msrmtConditionBigInt.compareTo(getEstimateTopRangeConditionRange()) <= 0)) {
            isEstimate = true;
        }
    }

    /**
     * This method will retrieves and sets the Latest Measurement date time from Measurement List
     */
    private void setLatestMeasurementEndDateTime(DateTime endMeasurementDTTM) {
        if (notNull(adjustUsagePeriodEndDateTimeLookup) && adjustUsagePeriodEndDateTimeLookup.isYes()) {
            if (isNull(latestMeasurementEndDateTime)) {
                latestMeasurementEndDateTime = endMeasurementDTTM;
            }
            if (notNull(latestMeasurementEndDateTime) && latestMeasurementEndDateTime.compareTo(endMeasurementDTTM) < 0) {
                latestMeasurementEndDateTime = endMeasurementDTTM;
            }
        }
    }
    
    /**
     * @param exceptionCategory
     * @param message
     */
    private void addException(String exceptionXpath,ServerMessage message){    	
    	usageRuleHelper.addException(applyUsageRuleAlgorithmInputOutputData,exceptionXpath,message,
    			usageRuleBoInstance.getElement(), null, applyUsageRuleAlgorithmInputData.getUsageRuleCode());    	
    	exitFromAlg = Bool.TRUE;		
	}
}
