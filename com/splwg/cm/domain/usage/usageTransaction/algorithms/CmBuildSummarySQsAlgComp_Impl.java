package com.splwg.cm.domain.usage.usageTransaction.algorithms;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.dom4j.Document;
import org.dom4j.Element;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.businessObject.COTSInstanceList;
import com.splwg.base.api.businessObject.COTSInstanceListNode;
import com.splwg.base.api.businessObject.COTSInstanceNode;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.TimeInterval;
import com.splwg.base.api.lookup.BusinessObjectActionLookup;
import com.splwg.base.domain.common.businessObject.BusinessObject;
import com.splwg.base.domain.common.businessObject.PreprocessBusinessObjectRequestAlgorithmSpot;
import com.splwg.base.support.schema.metadata.SchemaListMD;
import com.splwg.base.support.schema.metadata.SchemaRawMD;
import com.splwg.d1.api.lookup.ExtractIntervalDataLookup;
import com.splwg.d1.api.lookup.RoundingMethodD1Lookup;
import com.splwg.d1.api.lookup.UsageSubscriptionRelationshipTypeLookup;
import com.splwg.d1.domain.admin.serviceQuantityIdentifier.entities.ServiceQuantityIdentifierD1;
import com.splwg.d1.domain.admin.serviceQuantityIdentifier.entities.ServiceQuantityIdentifierD1_Id;
import com.splwg.d1.domain.admin.unitOfMeasure.entities.UnitOfMeasureD1;
import com.splwg.d1.domain.admin.unitOfMeasure.entities.UnitOfMeasureD1_Id;
import com.splwg.d1.domain.common.routines.IntervalPeriodHelper;
import com.splwg.d1.domain.usage.usageSubscription.data.DetermineSubscriptionDvcCfgPeriodInputData;
import com.splwg.d1.domain.usage.usageSubscription.data.DetermineSubscriptionDvcCfgPeriodOutputData;
import com.splwg.d1.domain.usage.usageSubscription.entities.UsageSubscription;
import com.splwg.d1.domain.usage.usageSubscription.entities.UsageSubscriptionRelationship;
import com.splwg.d1.domain.usage.usageSubscription.entities.UsageSubscription_Id;
import com.splwg.d1.domain.usage.usageTransaction.entities.UsageTransaction;
import com.splwg.d1.domain.usage.usageTransaction.entities.UsageTransaction_Id;
import com.splwg.d2.api.lookup.UsageTypeD2Lookup;
import com.splwg.d2.domain.usage.MessageRepository;
import com.splwg.d2.domain.usage.usageTransaction.data.ScalarDetailsInputOutputData;
import com.splwg.cm.domain.usage.usageTransaction.data.CmSummaryUsagePeriodsOutputData;
import com.splwg.cm.domain.usage.usageTransaction.data.CmUsagePeriodsInputData;
import com.splwg.cm.domain.usage.usageTransaction.data.CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod;
import com.splwg.cm.domain.usage.usageTransaction.routines.CmBuildSummarySQsHelper;
import com.splwg.shared.common.Dom4JHelper;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Abjayon
 *
 @AlgorithmComponent (softParameters = { @AlgorithmSoftParameter (name = targetSPI, type = integer)
 *            , @AlgorithmSoftParameter (name = usagePeriodGapTolerance, type = decimal)
 *            , @AlgorithmSoftParameter (name = uomDecimalPositions, type = string)})
 */
public class CmBuildSummarySQsAlgComp_Impl
        extends CmBuildSummarySQsAlgComp_Gen
        implements PreprocessBusinessObjectRequestAlgorithmSpot {

    private static final String SEND_DETAILS_GROUP = "sendDetails";
    private static final String USAGE_ID = "usageId";
    private static final String SUMMARY_USAGE_PERIODS_GROUP = "summaryUsagePeriods";
    private static final String SEND_DETAILS_SUMMARY_USAGE_PERIODS_GROUP = "sendDetails/summaryUsagePeriods";
    private static final String SUMMARY_USAGE_PERIODS_LIST = "summaryUsagePeriodsList";
    private static final String USAGE_PERIOD_GROUP = "usagePeriods";
    private static final String USAGE_PERIOD_LIST = "usagePeriodsList";
    private static final String SEQUENCE = "sequence";
    private static final String USAGE_PERIOD_SP_SEQUENCE = "spSQsequence";
    private static final String USAGE_PERIOD_START_DATETIME = "startDateTime";
    private static final String USAGE_PERIOD_END_DATETIME = "endDateTime";
    private static final String USAGE_PERIOD_USAGE_TYPE = "usageType";
    private static final String USAGE_PERIOD_SQ_GRP = "SQs";
    private static final String USAGE_PERIOD_SQ_LIST = "SQsList";
    private static final String USAGE_PERIOD_SP_SQ_GRP = "spSQs";
    private static final String USAGE_PERIOD_SP_SQ_LIST = "spSQsList";
    private static final String USAGE_PERIOD_SQ_SEQUENCE = "sqSequence";
    private static final String USAGE_PERIOD_SQ_UOM = "uom";
    private static final String USAGE_PERIOD_SQ_TOU = "tou";
    private static final String USAGE_PERIOD_SQ_SQI = "sqi";
    private static final String USAGE_PERIOD_SQ_QUANTITY = "quantity";
    private static final String USAGE_PERIOD_SQ_SP_ID = "spId";
    private static final String SCALAR_DETAILS_GROUP = "scalarDetails";
    private static final String SCALAR_DETAILS_LIST = "scalarDetailsList";

    private static final String ITEM_DETAILS_GROUP = "itemDetails";
    private static final String ITEM_DETAILS_LIST = "itemDetailsList";
    private static final String SUMMARY_ITEMS_GROUP = "items";
    private static final String SUMMARY_ITEMS_LIST = "itemsList";
    private static final String SUMMARY_ITEM_SEQ = "itemSeq";
    private static final String ITEM_DETAILS_LIST_START_DATE_TIME = "startDateTime";
    private static final String ITEM_DETAILS_LIST_END_DATE_TIME = "endDateTime";
    private static final String ITEM_ID = "itemId";
    private static final String ITEM_TYPE = "itemType";
    private static final String ITEM_COUNT = "itemCount";
    private static final String QUANTITY = "quantity";
    private static final String DAILY_SERVICE_QUANTITY = "dailyServiceQuantity";
    private static final String ITEM_UOM = "uom";
    private static final String SP_ITEMS_GROUP = "spItems";
    private static final String SP_ITEMS_LIST = "spItemsList";
    private static final String SP_ID = "spId";

    private static final String USAGE_PERIOD_SQ_MC_ID = "measuringComponentId";
    private static final String USAGE_PERIOD_STANDARD_START_DATETIME = "standardStartDateTime";
    private static final String USAGE_PERIOD_STANDARD_END_DATETIME = "standardEndDateTime";
    private static final String SECONDS_PER_INTERVAL = "secondsPerInterval";
    private static final String SHOULD_EXTRACT_INTERVAL_DATA = "shouldExtractIntervalData";
    private static final String SQ_INTERVALS_GROUP = "intervals";
    private static final String SQ_INTERVALS_LIST = "mL";
    private static final String INTERVAL_SEQUENCE = "s";
    private static final String INTERVAL_DATETIME = "dt";
    private static final String INTERVAL_QUANTITY = "q";
    private static final String INTERVAL_CONDITION = "c";

    private static final String HIGHLIGHT_DTTMS_GRP = "highlightDateTimes";
    private static final String HIGHLIGHT_DTTMS_LIST = "highlightDateTimesList";
    private static final String HIGHLIGHT_SEQ = "sequence";
    private static final String HIGHLIGHT_DTTM = "highlightDateTime";
    private static final String HIGHLIGHT_TYPE = "highlightType";
    private static final String HIGHLIGHT_CONDITION = "highlightCondition";
    private static final String HIGHLIGHT_DERIVED_CONDITION = "highlightDerivedCondition";

    private static final String START_TAG = "<";
    private static final String END_TAG = ">";
    private static final String CLOSE_TAG = "</";

    private static final String USAGE_START_DATETIME = "startDateTime";
    private static final String USAGE_END_DATETIME = "endDateTime";
    private DateTime usageStartDateTime = null;
    private DateTime usageEndDateTime = null;
    private DateTime summaryUsagePeriodStartDate1 = null;
    private DateTime summaryUsagePeriodStartDate2 = null;
    private Boolean isMeterExchange = false;
    private BigDecimal usageperiodgaptolerance = BigDecimal.ZERO;
    private BigDecimal milliSecondsConversion = new BigDecimal("3600000");
    private BusinessObjectInstance usageTransUsgPeriodBoInstance;
    private UsageSubscription usageSubscription;

    private Bool performRounding = Bool.FALSE;
    private int overridePrecision = -1;

    private static final Logger logger = LoggerFactory.getLogger(CmBuildSummarySQsAlgComp_Impl.class);

    private COTSInstanceList summarySqListNode = null;
    private COTSInstanceList summarySpSqListNode = null;

    /** Input Business Object Instance */
    private BusinessObjectInstance inputBusinessObjectInstance = null;

    /**
     * Method to get the request BusinessObjectInstance
     * @return BusinessObjectInstance
     */
    public BusinessObjectInstance getRequest() {
        return inputBusinessObjectInstance;
    }

    public void setAction(BusinessObjectActionLookup boAction) {
        // Empty

    }

    public void setBusinessObject(BusinessObject bo) {
        // Empty

    }

    /**
     * Method to set the Business Object Instance that is to be processed
     * @param BusinessObjectInstance
     */
    public void setRequest(BusinessObjectInstance businessObjectInstance) {
        inputBusinessObjectInstance = businessObjectInstance;

    }

    public void invoke() {

        validateParameters();

        COTSInstanceNode sendDetailsGroup = inputBusinessObjectInstance.getGroup(SEND_DETAILS_GROUP);
        String usageId = sendDetailsGroup.getString(USAGE_ID);
        UsageTransaction usageTransaction = new UsageTransaction_Id(usageId).getEntity();
        usageTransUsgPeriodBoInstance = BusinessObjectInstance.create(usageTransaction.getBusinessObject());
        usageTransUsgPeriodBoInstance.set(USAGE_ID, usageId);
        usageTransUsgPeriodBoInstance = BusinessObjectDispatcher.read(usageTransUsgPeriodBoInstance);
        sendDetailsGroup.getElement().setContent(usageTransUsgPeriodBoInstance.getElement().elements());

        // Get Summary Usage Period Group
        COTSInstanceNode summaryUsgPeriodGrp = sendDetailsGroup.getGroup(SUMMARY_USAGE_PERIODS_GROUP);
        COTSInstanceList summaryUsgPeriodListNode = summaryUsgPeriodGrp.getList(SUMMARY_USAGE_PERIODS_LIST);

        // Get Usage Period List
        COTSInstanceNode usagePeriodGroup = usageTransUsgPeriodBoInstance.getGroup(USAGE_PERIOD_GROUP);
        COTSInstanceList usagePeriodListNode = usagePeriodGroup.getList(USAGE_PERIOD_LIST);

        //Get Scalar Details
        COTSInstanceNode scalarDetailsGroup = sendDetailsGroup.getGroup(SCALAR_DETAILS_GROUP);
        COTSInstanceList scalarDetailsListNode = scalarDetailsGroup.getList(SCALAR_DETAILS_LIST);

        usageSubscription = usageTransaction.getUsageSubscriptionId().getEntity();
        CmSummaryUsagePeriodsOutputData summaryUsagePeriodsOutputData = retrieveSummaryUsagePeriods(usagePeriodListNode);
        prepareSummaryUsagePeriods(summaryUsagePeriodsOutputData, summaryUsgPeriodListNode);
        retrieveAndUpdateScalarDetails(scalarDetailsListNode);

        prepareItemDetails(sendDetailsGroup);
    }

    /**
     *   This method will retrieve the Summary Usage Periods for the given Usage Periods.
     * 
     * @param usagePeriodListNode
     * @return usageSubscription
     */
    private CmSummaryUsagePeriodsOutputData retrieveSummaryUsagePeriods(COTSInstanceList usagePeriodListNode) {
        int usgPeriodSequence = 1;
        int serviceQuantitySequence = 1;
        int intervalSequence = 1;
        CmUsagePeriodsInputData usagePeriodsInputData = new CmUsagePeriodsInputData();
        List<CmUsagePeriodsInputData.UsagePeriod> usagePeriodList = new ArrayList<CmUsagePeriodsInputData.UsagePeriod>();

        // Populate Usage Period List
        for (COTSInstanceListNode usagePeriod : usagePeriodListNode) {
            CmUsagePeriodsInputData.UsagePeriod usagePeriodData = new CmUsagePeriodsInputData.UsagePeriod();
            usagePeriodData.setSequence(new BigDecimal(usgPeriodSequence));
            usagePeriodData.setStartDateTime(usagePeriod.getDateTime(USAGE_PERIOD_START_DATETIME));
            usagePeriodData.setEndDateTime(usagePeriod.getDateTime(USAGE_PERIOD_END_DATETIME));
            usagePeriodData.setUsageType((UsageTypeD2Lookup) usagePeriod.getLookup(USAGE_PERIOD_USAGE_TYPE));

            List<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity> serviceQuantityList = new ArrayList<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity>();

            // Populate SQ List
            COTSInstanceNode sqGroup = usagePeriod.getGroup(USAGE_PERIOD_SQ_GRP);
            COTSInstanceList sqList = sqGroup.getList(USAGE_PERIOD_SQ_LIST);
            serviceQuantitySequence = 1;
            ArrayList<COTSInstanceListNode> sqSortedList = sortSqList(sqList);
            for (COTSInstanceListNode sqListNode : sqSortedList) {
                CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity serviceQuantityData = new CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity();
                serviceQuantityData.setSequence(new BigDecimal(serviceQuantitySequence));
                serviceQuantityData.setUom(sqListNode.getString(USAGE_PERIOD_SQ_UOM));
                serviceQuantityData.setTou(sqListNode.getString(USAGE_PERIOD_SQ_TOU));
                serviceQuantityData.setSqi(sqListNode.getString(USAGE_PERIOD_SQ_SQI));
                serviceQuantityData.setQuantity(sqListNode.getNumber(USAGE_PERIOD_SQ_QUANTITY));
                serviceQuantityData.setSpId(sqListNode.getString(USAGE_PERIOD_SQ_SP_ID));
                serviceQuantityData.setMeasuringComponentId(sqListNode.getString(USAGE_PERIOD_SQ_MC_ID));
                serviceQuantityData.setSecondsPerInterval(sqListNode.getTimeInterval(SECONDS_PER_INTERVAL));
                serviceQuantityData.setShouldExtractInterval((ExtractIntervalDataLookup) sqListNode
                        .getLookup(SHOULD_EXTRACT_INTERVAL_DATA));
                
                COTSInstanceNode usageDetailsGroup = sqListNode.getGroup("usageDetails");
                if(notNull(usageDetailsGroup)){
	                //Set new Usage Details
	                if(notBlank(usageDetailsGroup.getString("routeId"))){
	                	serviceQuantityData.setRouteId(usageDetailsGroup.getString("routeId"));
	                }
	                if(notBlank(usageDetailsGroup.getString("serialNumber"))){
	                	serviceQuantityData.setSerialNumber(usageDetailsGroup.getString("serialNumber"));
	                }
	                if(notNull(usageDetailsGroup.getNumber("currentYearConsumption"))){
	                	serviceQuantityData.setCurrentYearConsumption(usageDetailsGroup.getNumber("currentYearConsumption"));
	                }
	                if(notNull(usageDetailsGroup.getNumber("lastYearConsumption"))){
	                	serviceQuantityData.setLastYearConsumption(usageDetailsGroup.getNumber("lastYearConsumption"));
	                }
	                if(notBlank(usageDetailsGroup.getString("meterId"))){
	                	serviceQuantityData.setMeterId(usageDetailsGroup.getString("meterId"));
	                }
	                if(notBlank(usageDetailsGroup.getString("meterBrand"))){
	                	serviceQuantityData.setMeterBrand(usageDetailsGroup.getString("meterBrand"));
	                }
	                if(notNull(usageDetailsGroup.getString("meterType"))){
	                	serviceQuantityData.setMeterType(usageDetailsGroup.getString("meterType"));
	                }
	                if(notNull(usageDetailsGroup.getNumber("lastIndex"))){
	                	serviceQuantityData.setLastIndex(usageDetailsGroup.getNumber("lastIndex"));
	                }
	                if(notNull(usageDetailsGroup.getNumber("firstIndex"))){
	                	serviceQuantityData.setFirstIndex(usageDetailsGroup.getNumber("firstIndex"));
	                }
	                if(notNull(usageDetailsGroup.getNumber("multiplier"))){
	                	serviceQuantityData.setMultiplier(usageDetailsGroup.getNumber("multiplier"));
	                }
	                if(notNull(usageDetailsGroup.getNumber("consumption"))){
	                	serviceQuantityData.setConsumption(usageDetailsGroup.getNumber("consumption"));
	                }
	                if(notNull(usageDetailsGroup.getNumber("transformerLoss"))){
	                	serviceQuantityData.setTransformerLoss(usageDetailsGroup.getNumber("transformerLoss"));
	                }
	                if(notNull(usageDetailsGroup.getNumber("dailyAverageUsage"))){
	                	serviceQuantityData.setDailyAverageUsage(usageDetailsGroup.getNumber("dailyAverageUsage"));
	                }
	                if(notNull(usageDetailsGroup.getDateTime("lastReadingDate"))){
	                	serviceQuantityData.setLastReadingDate(usageDetailsGroup.getDateTime("lastReadingDate"));
	                }
	                if(notNull(usageDetailsGroup.getDateTime("firstReadingDate"))){
	                	serviceQuantityData.setFirstReadingDate(usageDetailsGroup.getDateTime("firstReadingDate"));
	                }
	                if(notNull(usageDetailsGroup.getNumber("demandIndex"))){
	                	serviceQuantityData.setDemandIndex(usageDetailsGroup.getNumber("demandIndex"));
	                }
	                if(notNull(usageDetailsGroup.getNumber("demandMultiplier"))){
	                	serviceQuantityData.setDemandMultiplier(usageDetailsGroup.getNumber("demandMultiplier"));
	                }
	                if(notNull(usageDetailsGroup.getNumber("currentTransformerRatio"))){
	                	serviceQuantityData.setCurrentTransformerRatio(usageDetailsGroup.getNumber("currentTransformerRatio"));
	                }
	                if(notNull(usageDetailsGroup.getNumber("voltageTransformerRatio"))){
	                	serviceQuantityData.setVoltageTransformerRatio(usageDetailsGroup.getNumber("voltageTransformerRatio"));
	                }
	                if(notNull(usageDetailsGroup.getNumber("indexDifference"))){
	                	serviceQuantityData.setIndexDifference(usageDetailsGroup.getNumber("indexDifference"));
	                }
	                if(notNull(usageDetailsGroup.getNumber("demand"))){
	                	serviceQuantityData.setDemandConsumption(usageDetailsGroup.getNumber("demand"));
	                }
	                if(notNull(usageDetailsGroup.getNumber("netGeneration"))){
	                	serviceQuantityData.setNetGeneration(usageDetailsGroup.getNumber("netGeneration"));
	                }
	                if(notNull(usageDetailsGroup.getNumber("generationDemand"))){
	                	serviceQuantityData.setGenerationDemand(usageDetailsGroup.getNumber("generationDemand"));
	                }
	                if(notNull(usageDetailsGroup.getNumber("inductiveRatio"))){
	                	serviceQuantityData.setInductiveRatio(usageDetailsGroup.getNumber("inductiveRatio"));
	                }
	                if(notNull(usageDetailsGroup.getNumber("capacitiveRatio"))){
	                	serviceQuantityData.setCapacitiveRatio(usageDetailsGroup.getNumber("capacitiveRatio"));
	                }
                }

                // Populate Interval List
                List<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Interval> intervalList = new ArrayList<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Interval>();
                COTSInstanceNode sqNodeIntervalDataGroup = sqListNode.getGroupFromPath(SQ_INTERVALS_GROUP);
                COTSInstanceList sqNodeIIntervalDataList = sqNodeIntervalDataGroup.getList(SQ_INTERVALS_LIST);
                intervalSequence = 1;
                for (COTSInstanceListNode sqNodeIIntervalData : sqNodeIIntervalDataList) {
                    CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Interval intervalData = new CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Interval();
                    intervalData.setSequence(new BigDecimal(intervalSequence));
                    intervalData.setDateTime(sqNodeIIntervalData.getDateTime(INTERVAL_DATETIME));
                    intervalData.setQuantity(sqNodeIIntervalData.getNumber(INTERVAL_QUANTITY));
                    intervalData.setCondition(sqNodeIIntervalData.getExtendedLookupId(INTERVAL_CONDITION));

                    intervalList.add(intervalData);
                    intervalSequence++;
                }
                serviceQuantityData.setIntervalList(intervalList);

                List<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight> usgPeriodHighlightList = new ArrayList<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight>();
                COTSInstanceNode highlightSqGroup = sqListNode.getGroupFromPath(HIGHLIGHT_DTTMS_GRP);
                COTSInstanceList highlightSqList = highlightSqGroup.getList(HIGHLIGHT_DTTMS_LIST);

                if (notNull(highlightSqList)) {
                    for (COTSInstanceListNode sqHighlightListData : highlightSqList) {
                        CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight highlightSqData = new CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight();
                        highlightSqData.setSequence(sqHighlightListData.getNumber(SEQUENCE));
                        highlightSqData.setHighlightDateTime(sqHighlightListData.getDateTime(HIGHLIGHT_DTTM));
                        highlightSqData.setHighlightType(sqHighlightListData.getExtendedLookupId(HIGHLIGHT_TYPE));
                        if (notNull(sqHighlightListData.getExtendedLookupId(HIGHLIGHT_CONDITION))) {
                            highlightSqData.setHighlightCondition(sqHighlightListData
                                    .getExtendedLookupId(HIGHLIGHT_CONDITION));
                        }
                        if (notNull(sqHighlightListData.getExtendedLookupId(HIGHLIGHT_DERIVED_CONDITION))) {
                            highlightSqData.setHighlightDerivedCondition(sqHighlightListData
                                    .getExtendedLookupId(HIGHLIGHT_DERIVED_CONDITION));
                        }
                        usgPeriodHighlightList.add(highlightSqData);
                    }
                }
                serviceQuantityData.setHighlightList(usgPeriodHighlightList);
                serviceQuantityList.add(serviceQuantityData);
                serviceQuantitySequence++;
            }

            usagePeriodData.setServiceQuantityList(serviceQuantityList);

            List<CmUsagePeriodsInputData.UsagePeriod.Item> itemList = new ArrayList<CmUsagePeriodsInputData.UsagePeriod.Item>();

            COTSInstanceNode itemsGroup = usagePeriod.getGroup(ITEM_DETAILS_GROUP);
            COTSInstanceList itemsList = itemsGroup.getList(ITEM_DETAILS_LIST);

            int itemSequence = 1;
            ArrayList<COTSInstanceListNode> itemSortedList = sortSqList(itemsList);
            for (COTSInstanceListNode itemListNode : itemSortedList) {
                CmUsagePeriodsInputData.UsagePeriod.Item itemData = new CmUsagePeriodsInputData.UsagePeriod.Item();
                itemData.setItemSequence(new BigDecimal(itemSequence));
                itemData.setItemId(itemListNode.getString(ITEM_ID));
                itemData.setItemType(itemListNode.getString(ITEM_TYPE));
                itemData.setItemCount(itemListNode.getNumber(ITEM_COUNT));
                itemData.setQuantity(itemListNode.getNumber(QUANTITY));
                itemData.setDailyServiceQuantity(itemListNode.getNumber(DAILY_SERVICE_QUANTITY));
                itemData.setStartDateTime(itemListNode.getDateTime(ITEM_DETAILS_LIST_START_DATE_TIME));
                itemData.setEndDateTime(itemListNode.getDateTime(ITEM_DETAILS_LIST_END_DATE_TIME));
                itemData.setUom(itemListNode.getString(ITEM_UOM));
                itemData.setSpId(itemListNode.getString(SP_ID));

                itemSequence = itemSequence + 1;

                itemList.add(itemData);
            }
            usagePeriodData.setItemList(itemList);

            usagePeriodList.add(usagePeriodData);
            usgPeriodSequence++;
        }

        usagePeriodsInputData.setUsageSubscription(usageSubscription);
        usagePeriodsInputData.setTargetSPI((isNull(getTargetSPI()) ? null
                : new TimeInterval(getTargetSPI().longValue())));
        usagePeriodsInputData.setUsagePeriodList(usagePeriodList);

        CmBuildSummarySQsHelper buildSummarySQsHelper = CmBuildSummarySQsHelper.Factory.newInstance();
        CmSummaryUsagePeriodsOutputData summaryUsagePeriodsOutputData = buildSummarySQsHelper
                .buildSummaryUsagePeriods(usagePeriodsInputData);

        return summaryUsagePeriodsOutputData;
    }

    /**
     *  This method will prepare the Summary Usage Periods in the form of required BO Schema.
     * 
     * @param summaryUsagePeriodsOutputData
     * @param summaryUsgPeriodListNode
     */
    private void prepareSummaryUsagePeriods(CmSummaryUsagePeriodsOutputData summaryUsagePeriodsOutputData,
            COTSInstanceList summaryUsgPeriodListNode) {
        List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> summaryUsagePeriodList = summaryUsagePeriodsOutputData
                .getSummaryUsagePeriodList();
        if (summaryUsagePeriodList.size() > 1) {

            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> summaryUsagePeriodSortedList = sortSummaryUsagePeriodList(summaryUsagePeriodList);
            usageperiodgaptolerance = getUsagePeriodGapTolerance();
            summaryUsagePeriodList = checkForMeterExchangeAndAdjustGap(summaryUsagePeriodSortedList);
        }

        for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod summaryUsagePeriodListData : summaryUsagePeriodList) {
            COTSInstanceListNode summaryUsagePeriod = summaryUsgPeriodListNode.newChild();
            summaryUsagePeriod.set(SEQUENCE, summaryUsagePeriodListData.getSequence());
            summaryUsagePeriod.set(USAGE_PERIOD_START_DATETIME, summaryUsagePeriodListData.getStartDateTime());
            summaryUsagePeriod.set(USAGE_PERIOD_END_DATETIME, summaryUsagePeriodListData.getEndDateTime());
            summaryUsagePeriod.set(USAGE_PERIOD_USAGE_TYPE, summaryUsagePeriodListData.getUsageType());

            SchemaListMD summaryUsagePeriodsSchemaListMD = inputBusinessObjectInstance.getGroupFromPath(
                    SEND_DETAILS_SUMMARY_USAGE_PERIODS_GROUP).getList(SUMMARY_USAGE_PERIODS_LIST).getSchemaMD();
            if (notNull(summaryUsagePeriodsSchemaListMD.getFieldNamed(USAGE_PERIOD_STANDARD_START_DATETIME))) {
                /*standardStartDateTime element will be present only for D2-UsageTranOutboundMesg BO and not for D2-XSDUsageTranOutboundMesg BO*/
                summaryUsagePeriod.set(USAGE_PERIOD_STANDARD_START_DATETIME, summaryUsagePeriodListData
                        .getStandardStartDateTime());
            }
            if (notNull(summaryUsagePeriodsSchemaListMD.getFieldNamed(USAGE_PERIOD_STANDARD_END_DATETIME))) {
                /*standardEndDateTime element will be present only for D2-UsageTranOutboundMesg BO and not for D2-XSDUsageTranOutboundMesg BO*/
                summaryUsagePeriod.set(USAGE_PERIOD_STANDARD_END_DATETIME, summaryUsagePeriodListData
                        .getStandardEndDateTime());
            }

            // Get SQList of Summary Usage Period
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity> summarySQList = summaryUsagePeriodListData
                    .getServiceQuantityList();
            COTSInstanceNode summarySqGroup = summaryUsagePeriod.getGroup(USAGE_PERIOD_SQ_GRP);
            summarySqListNode = summarySqGroup.getList(USAGE_PERIOD_SQ_LIST);

            for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity sQListData : summarySQList) {
                BigDecimal sQListDataQuantity = sQListData.getQuantity();
                int precision = determineUomDecimalPrecision(sQListData.getSqi(), sQListData.getUom());
                if (precision > -1) {
                    sQListDataQuantity = retrieveRoundedQuantity(sQListDataQuantity, precision);
                }
                COTSInstanceListNode summarySqNode = summarySqListNode.newChild();
                summarySqNode.set(USAGE_PERIOD_SQ_SEQUENCE, sQListData.getSequence());
                summarySqNode.set(USAGE_PERIOD_SQ_UOM, sQListData.getUom());
                summarySqNode.set(USAGE_PERIOD_SQ_TOU, sQListData.getTou());
                summarySqNode.set(USAGE_PERIOD_SQ_SQI, sQListData.getSqi());
                summarySqNode.set(USAGE_PERIOD_SQ_QUANTITY, sQListDataQuantity);
                summarySqNode.set(SECONDS_PER_INTERVAL, sQListData.getSecondsPerInterval());
                //Summary Intervals
                SchemaRawMD rawMD = summarySqNode.getSchemaMD().getRawNamed(SQ_INTERVALS_GROUP);
                if (notNull(rawMD)) {
                    String sqIntervalsRawData = retrieveSQIntervalsRawData(sQListData.getIntervalList(), precision);
                    if (notBlank(sqIntervalsRawData)) {

                        try {
                            Document document = Dom4JHelper.parseText(sqIntervalsRawData);
                            Element element = document.getRootElement();
                            summarySqNode.getRawElements(SQ_INTERVALS_GROUP).add(element);
                        } catch (Exception ex) {
                            logger.debug("Exception occured while parsing the text");
                        }
                    }
                } else {
                    COTSInstanceNode summaryNodeIntervalDataGroup = summarySqNode.getGroupFromPath(SQ_INTERVALS_GROUP);
                    COTSInstanceList summaryNodeIIntervalDataList = summaryNodeIntervalDataGroup
                            .getList(SQ_INTERVALS_LIST);
                    List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Interval> summarySQIntervalList = sQListData
                            .getIntervalList();
                    if (notNull(summarySQIntervalList)) {
                        for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Interval summarySQIntervalData : summarySQIntervalList) {
                            BigDecimal sQIntervalDataQuantity = summarySQIntervalData.getQuantity();
                            if (precision > -1) {
                                sQIntervalDataQuantity = retrieveRoundedQuantity(sQIntervalDataQuantity, precision);
                            }
                            COTSInstanceListNode newNode = summaryNodeIIntervalDataList.newChild();
                            newNode.set(INTERVAL_SEQUENCE, summarySQIntervalData.getSequence());
                            newNode.set(INTERVAL_DATETIME, summarySQIntervalData.getDateTime());
                            newNode.set(INTERVAL_QUANTITY, sQIntervalDataQuantity);
                            newNode.set(INTERVAL_CONDITION, summarySQIntervalData.getCondition());
                        }
                    }
                }

                COTSInstanceNode highlightSqGroup = summarySqNode.getGroup(HIGHLIGHT_DTTMS_GRP);
                COTSInstanceList highlightSqListNode = highlightSqGroup.getList(HIGHLIGHT_DTTMS_LIST);

                List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight> sqHighightList = sQListData
                        .getHighlightList();
                if (notNull(sqHighightList) && sqHighightList.size() > 0) {
                    for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight sqHighlightListData : sqHighightList) {
                        COTSInstanceListNode highlightSqList = highlightSqListNode.newChild();
                        highlightSqList.set(HIGHLIGHT_SEQ, sqHighlightListData.getSequence());
                        highlightSqList.set(HIGHLIGHT_DTTM, sqHighlightListData.getHighlightDateTime());
                        highlightSqList.set(HIGHLIGHT_TYPE, sqHighlightListData.getHighlightType());
                        highlightSqList.set(HIGHLIGHT_CONDITION, sqHighlightListData.getHighlightCondition());
                        highlightSqList.set(HIGHLIGHT_DERIVED_CONDITION, sqHighlightListData
                                .getHighlightDerivedCondition());
                    }
                }
                
                //Set Usage Details
                COTSInstanceNode usageDetailsGroup = summarySqNode.getGroup("usageDetails");
                if(notNull(usageDetailsGroup)){
                	if(notBlank(sQListData.getSpId())){
                		usageDetailsGroup.set("spId", sQListData.getSpId());
                	}
                	if(notBlank(sQListData.getRouteId())){
                		usageDetailsGroup.set("routeId", sQListData.getRouteId());
                	}
                	if(notBlank(sQListData.getSerialNumber())){
                		usageDetailsGroup.set("serialNumber", sQListData.getSerialNumber());
                	}
                	if(notNull(sQListData.getCurrentYearConsumption())){
                		usageDetailsGroup.set("currentYearConsumption", sQListData.getCurrentYearConsumption());
                	}
                	if(notNull(sQListData.getLastYearConsumption())){
                		usageDetailsGroup.set("lastYearConsumption", sQListData.getLastYearConsumption());
                	}
                	if(notBlank(sQListData.getMeterId())){
                		usageDetailsGroup.set("meterId", sQListData.getMeterId());
                	}
                	if(notBlank(sQListData.getMeterBrand())){
                		usageDetailsGroup.set("meterBrand", sQListData.getMeterBrand());
                	}
                	if(notBlank(sQListData.getMeterType())){
                		usageDetailsGroup.set("meterType", sQListData.getMeterType());
                	}
                	if(notNull(sQListData.getLastIndex())){
                		usageDetailsGroup.set("lastIndex", sQListData.getLastIndex());
                	}
                	if(notNull(sQListData.getFirstIndex())){
                		usageDetailsGroup.set("firstIndex", sQListData.getFirstIndex());
                	}
                	if(notNull(sQListData.getMultiplier())){
                		usageDetailsGroup.set("multiplier", sQListData.getMultiplier());
                	}
                	if(notNull(sQListData.getConsumption())){
                		usageDetailsGroup.set("consumption", sQListData.getConsumption());
                	}
                	if(notNull(sQListData.getTransformerLoss())){
                		usageDetailsGroup.set("transformerLoss", sQListData.getTransformerLoss());
                	}
                	if(notNull(sQListData.getDailyAverageUsage())){
                		usageDetailsGroup.set("dailyAverageUsage", sQListData.getDailyAverageUsage());
                	}
                	if(notNull(sQListData.getLastReadingDate())){
                		usageDetailsGroup.set("lastReadingDate", sQListData.getLastReadingDate());
                	}
                	if(notNull(sQListData.getFirstReadingDate())){
                		usageDetailsGroup.set("firstReadingDate", sQListData.getFirstReadingDate());
                	}
                	if(notNull(sQListData.getDemandIndex())){
                		usageDetailsGroup.set("demandIndex", sQListData.getDemandIndex());
                	}
                	if(notNull(sQListData.getDemandMultiplier())){
                		usageDetailsGroup.set("demandMultiplier", sQListData.getDemandMultiplier());
                	}
                	if(notNull(sQListData.getDemandConsumption())){
                		usageDetailsGroup.set("demand", sQListData.getDemandConsumption());
                	}
                	if(notNull(sQListData.getCurrentTransformerRatio())){
                		usageDetailsGroup.set("currentTransformerRatio", sQListData.getCurrentTransformerRatio());
                	}
                	if(notNull(sQListData.getVoltageTransformerRatio())){
                		usageDetailsGroup.set("voltageTransformerRatio", sQListData.getVoltageTransformerRatio());
                	}
                	if(notNull(sQListData.getIndexDifference())){
                		usageDetailsGroup.set("indexDifference", sQListData.getIndexDifference());
                	}
                	if(notNull(sQListData.getNetGeneration())){
                		usageDetailsGroup.set("netGeneration", sQListData.getNetGeneration());
                	}
                	if(notNull(sQListData.getGenerationDemand())){
                		usageDetailsGroup.set("generationDemand", sQListData.getGenerationDemand());
                	}
                	if(notNull(sQListData.getInductiveRatio())){
                		usageDetailsGroup.set("inductiveRatio", sQListData.getInductiveRatio());
                	}
                	if(notNull(sQListData.getCapacitiveRatio())){
                		usageDetailsGroup.set("capacitiveRatio", sQListData.getCapacitiveRatio());
                	}
                }
            }

            COTSInstanceNode summaryItemGroup = summaryUsagePeriod.getGroup(SUMMARY_ITEMS_GROUP);
            COTSInstanceList summaryitemListNode = summaryItemGroup.getList(SUMMARY_ITEMS_LIST);
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.Item> summaryItemList = summaryUsagePeriodListData
                    .getItemList();
            for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.Item itemData : summaryItemList) {
                BigDecimal itemDataQuantity = itemData.getDailyServiceQuantity();
                int precision = determineUomDecimalPrecision(null, itemData.getUom());
                if (precision > -1) {
                    itemDataQuantity = retrieveRoundedQuantity(itemDataQuantity, precision);
                }

                COTSInstanceListNode itemListNode = summaryitemListNode.newChild();
                itemListNode.set(SUMMARY_ITEM_SEQ, itemData.getItemSequence());
                itemListNode.set(ITEM_TYPE, itemData.getItemType());
                itemListNode.set(ITEM_COUNT, itemData.getItemCount());
                itemListNode.set(DAILY_SERVICE_QUANTITY, itemDataQuantity);
                itemListNode.set(ITEM_UOM, itemData.getUom());
                itemListNode.set(ITEM_DETAILS_LIST_START_DATE_TIME, itemData.getStartDateTime());
                itemListNode.set(ITEM_DETAILS_LIST_END_DATE_TIME, itemData.getEndDateTime());
            }
            // Get SP SQList of Summary Usage Period
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity> spSQList = summaryUsagePeriodListData
                    .getSPServiceQuantityList();
            COTSInstanceNode summarySpSqGroup = summaryUsagePeriod.getGroup(USAGE_PERIOD_SP_SQ_GRP);
            summarySpSqListNode = summarySpSqGroup.getList(USAGE_PERIOD_SP_SQ_LIST);

            for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity spSQListData : spSQList) {
                BigDecimal spSQListDataQuantity = spSQListData.getQuantity();
                int precision = determineUomDecimalPrecision(spSQListData.getSqi(), spSQListData.getUom());
                if (precision > -1) {
                    spSQListDataQuantity = retrieveRoundedQuantity(spSQListDataQuantity, precision);
                }

                COTSInstanceListNode summarySpSqList = summarySpSqListNode.newChild();
                summarySpSqList.set(USAGE_PERIOD_SP_SEQUENCE, spSQListData.getSequence());
                summarySpSqList.set(USAGE_PERIOD_SQ_SP_ID, spSQListData.getSpId());
                summarySpSqList.set(USAGE_PERIOD_SQ_UOM, spSQListData.getUom());
                summarySpSqList.set(USAGE_PERIOD_SQ_TOU, spSQListData.getTou());
                summarySpSqList.set(USAGE_PERIOD_SQ_SQI, spSQListData.getSqi());
                summarySpSqList.set(USAGE_PERIOD_SQ_QUANTITY, spSQListDataQuantity);

                COTSInstanceNode highlightSpSqGroup = summarySpSqList.getGroupFromPath(HIGHLIGHT_DTTMS_GRP);
                COTSInstanceList highlightSpSqListNode = highlightSpSqGroup.getList(HIGHLIGHT_DTTMS_LIST);

                //Set the Highlight Date/times list
                List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight> spSQHighightList = spSQListData
                        .getHighlightList();
                if (notNull(spSQHighightList) && spSQHighightList.size() > 0) {
                    for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight spSQHighlightListData : spSQHighightList) {
                        COTSInstanceListNode highlightSpSqList = highlightSpSqListNode.newChild();
                        highlightSpSqList.set(HIGHLIGHT_SEQ, spSQHighlightListData.getSequence());
                        highlightSpSqList.set(HIGHLIGHT_DTTM, spSQHighlightListData.getHighlightDateTime());
                        highlightSpSqList.set(HIGHLIGHT_TYPE, spSQHighlightListData.getHighlightType());
                        highlightSpSqList.set(HIGHLIGHT_CONDITION, spSQHighlightListData.getHighlightCondition());
                        highlightSpSqList.set(HIGHLIGHT_DERIVED_CONDITION, spSQHighlightListData
                                .getHighlightDerivedCondition());
                    }
                }
            }
            
            COTSInstanceNode summarySPItemGroup = summaryUsagePeriod.getGroup(SP_ITEMS_GROUP);
            COTSInstanceList summarySPitemListNode = summarySPItemGroup.getList(SP_ITEMS_LIST);
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.SPItem> summarySPItemList = summaryUsagePeriodListData
                    .getSpItemList();
            for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.SPItem spItemData : summarySPItemList) {
                BigDecimal spItemDataQuantity = spItemData.getQuantity();
                int precision = determineUomDecimalPrecision(null, spItemData.getUom());
                if (precision > -1) {
                    spItemDataQuantity = retrieveRoundedQuantity(spItemDataQuantity, precision);
                }

                COTSInstanceListNode itemSPListNode = summarySPitemListNode.newChild();
                itemSPListNode.set(SUMMARY_ITEM_SEQ, spItemData.getItemSequence());
                itemSPListNode.set(ITEM_TYPE, spItemData.getItemType());
                itemSPListNode.set(ITEM_COUNT, spItemData.getItemCount());
                itemSPListNode.set(QUANTITY, spItemDataQuantity);
                itemSPListNode.set(ITEM_UOM, spItemData.getUom());
                itemSPListNode.set(ITEM_DETAILS_LIST_START_DATE_TIME, spItemData.getStartDateTime());
                itemSPListNode.set(ITEM_DETAILS_LIST_END_DATE_TIME, spItemData.getEndDateTime());
                itemSPListNode.set(SP_ID, spItemData.getSpId());
            }

        }
    }

    /**
     *  This method will update the MDM SP Identifier with the CCB External SP Identifier for Scalar Usage Periods.
     * 
     * @param scalarDetailsListNode
     */
    private void retrieveAndUpdateScalarDetails(COTSInstanceList scalarDetailsListNode) {
        if (notNull(scalarDetailsListNode) && scalarDetailsListNode.getSize() > 0) {
            int scalarDetailsSeqNo = 1;
            ScalarDetailsInputOutputData scalarDetailsInputData = new ScalarDetailsInputOutputData();
            List<ScalarDetailsInputOutputData.ScalarDetail> scalarDetailsInputList = new ArrayList<ScalarDetailsInputOutputData.ScalarDetail>();
            CmBuildSummarySQsHelper buildSummarySQsHelper = CmBuildSummarySQsHelper.Factory.newInstance();
            for (COTSInstanceListNode scalarDetails : scalarDetailsListNode) {
                ScalarDetailsInputOutputData.ScalarDetail scalarDetailsData = new ScalarDetailsInputOutputData.ScalarDetail();
                scalarDetailsData.setSequence(new BigDecimal(scalarDetailsSeqNo));
                scalarDetailsData.setSpId(scalarDetails.getString(USAGE_PERIOD_SQ_SP_ID));
                scalarDetailsInputList.add(scalarDetailsData);
                scalarDetailsSeqNo++;
            }
            scalarDetailsInputData.setScalarDetailList(scalarDetailsInputList);
            scalarDetailsInputData.setUsageSubscription(usageSubscription);
            ScalarDetailsInputOutputData scalarDetailsOutputData = buildSummarySQsHelper
                    .buildScalarDetails(scalarDetailsInputData);

            List<ScalarDetailsInputOutputData.ScalarDetail> scalarDetailOutputList = scalarDetailsOutputData
                    .getScalarDetailList();
            for (ScalarDetailsInputOutputData.ScalarDetail scalarDetailOutputListData : scalarDetailOutputList) {
                String outputSpId = scalarDetailOutputListData.getSpId();
                String outputExternalSpId = scalarDetailOutputListData.getExternalSpId();
                for (COTSInstanceListNode scalarDetails : scalarDetailsListNode) {
                    String mdmSp = scalarDetails.getString(USAGE_PERIOD_SQ_SP_ID);
                    if (mdmSp.equals(outputSpId)) {
                        scalarDetails.set(USAGE_PERIOD_SQ_SP_ID, outputExternalSpId);
                        break;
                    }
                }
            }
        }
    }

    private ArrayList<COTSInstanceListNode> sortSqList(COTSInstanceList sqList) {
        ArrayList<COTSInstanceListNode> newSqList = new ArrayList<COTSInstanceListNode>();
        if (notNull(sqList) && sqList.getSize() > 0) {
            for (COTSInstanceListNode sqListNode : sqList) {
                newSqList.add(sqListNode);
            }
            Collections.sort(newSqList, new SortBySpIdComparator());
        }
        return newSqList;
    }

    class SortBySpIdComparator
            implements Comparator<COTSInstanceListNode> {
        /**
         * @param sqListNode Objects to be Compared
         * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second.
         */
        public int compare(COTSInstanceListNode sqListNode1, COTSInstanceListNode sqListNode2) {

            String spId1 = notNull(sqListNode1) ? sqListNode1.getString(USAGE_PERIOD_SQ_SP_ID) : "";
            String spId2 = notNull(sqListNode2) ? sqListNode2.getString(USAGE_PERIOD_SQ_SP_ID) : "";

            if (isBlankOrNull(spId1) && isBlankOrNull(spId2)) {
                return 0;
            } else if (isBlankOrNull(spId1)) {
                return -1;
            } else if (isBlankOrNull(spId2)) {
                return 1;
            }
            return spId1.trim().compareTo(spId2.trim());
        }
    }

    /**
     * This method will sort the summary usage period list based on the start date
     */

    private List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> sortSummaryUsagePeriodList(
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> unsortedSummaryUsagePeriodList) {

        if (notNull(unsortedSummaryUsagePeriodList) && unsortedSummaryUsagePeriodList.size() > 0) {
            Collections.sort(unsortedSummaryUsagePeriodList, new SortByStartDateComparator());
        }
        return unsortedSummaryUsagePeriodList;
    }

    private class SortByStartDateComparator
            implements Comparator<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> {

        public int compare(SummaryUsagePeriod summaryUsgPeriodListNode1, SummaryUsagePeriod summaryUsgPeriodListNode2) {

            summaryUsagePeriodStartDate1 = summaryUsgPeriodListNode1.getStartDateTime();
            summaryUsagePeriodStartDate2 = summaryUsgPeriodListNode2.getStartDateTime();

            return summaryUsagePeriodStartDate1.compareTo(summaryUsagePeriodStartDate2);
        }
    }

    private List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> checkForMeterExchangeAndAdjustGap(
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> adjustSummaryUsagePeriodList) {

        List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> summaryUsagePeriodAdjustedList = new ArrayList<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod>();

        //Find out MeterExchange scenario or not
        checkForMeterExchange();

        //If there is meter exchange then adjust if there is gap and gap is with in the tolerance
        if (isMeterExchange && notNull(usageperiodgaptolerance)) {
            summaryUsagePeriodAdjustedList.addAll(adjustUsagePeriods(adjustSummaryUsagePeriodList));
        }

        return adjustSummaryUsagePeriodList;
    }

    private List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> adjustUsagePeriods(
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> summaryUsagePeriodsListNode) {
        int currentIndex = 0;
        int previousIndex = 0;
        CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod summaryUsagePeriodPreviousNode;
        CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod summaryUsagePeriodCurrentNode;
        DateTime usagePeriodEndDateTimePrev = null;
        DateTime usagePeriodStartDateTimeCurr = null;
        DateTime usagePeriodStandardStartDateTimeCurr = null;
        long gapBetweenUsagePeriods = 0;
        BigDecimal gap = BigDecimal.ZERO;

        validateUsagePeriodGapTolerance();

        for (int summaryUsgPeriodIteration = 0; summaryUsgPeriodIteration < summaryUsagePeriodsListNode.size(); summaryUsgPeriodIteration++) {

            if (currentIndex > 0) {
                summaryUsagePeriodPreviousNode = summaryUsagePeriodsListNode.get(previousIndex);
                summaryUsagePeriodCurrentNode = summaryUsagePeriodsListNode.get(currentIndex);

                usagePeriodEndDateTimePrev = summaryUsagePeriodPreviousNode.getEndDateTime();
                usagePeriodStartDateTimeCurr = summaryUsagePeriodCurrentNode.getStartDateTime();
                usagePeriodStandardStartDateTimeCurr = summaryUsagePeriodCurrentNode.getStandardStartDateTime();

                gapBetweenUsagePeriods = usagePeriodStartDateTimeCurr.difference(usagePeriodEndDateTimePrev)
                        .getTotalMilliseconds();
                gap = new BigDecimal(gapBetweenUsagePeriods);
                gap = gap.divide(milliSecondsConversion);
                if ((gap.compareTo(usageperiodgaptolerance) < 1 || gap.compareTo(usageperiodgaptolerance) == 0)
                        && gapBetweenUsagePeriods != 0) {
                    summaryUsagePeriodPreviousNode.setEndDateTime(usagePeriodStartDateTimeCurr);
                    summaryUsagePeriodPreviousNode.setStandardEndDateTime(usagePeriodStandardStartDateTimeCurr);
                }
                previousIndex++;
            }
            currentIndex++;
        }

        return summaryUsagePeriodsListNode;
    }

    /**
     * This method will validate the Usage Period Gap Tolerance
     */

    private void validateUsagePeriodGapTolerance() {
        if (usageperiodgaptolerance.compareTo(BigDecimal.ZERO) < 1
                || usageperiodgaptolerance.compareTo(BigDecimal.ZERO) == 0) {
            addError(MessageRepository.usagePeriodGapToleranceShouldBeGreaterThanZero());
        }
    }

    /**
     * This method checks if there is a meter exchange on any of the Sp's linked to the US
     *
     */

    private void checkForMeterExchange() {
        UsageSubscription usageSubscriptionId;
        String previousDCId = null;
        String currentDCId = null;
        String previousSPId = null;
        String currentSPId = null;
        usageStartDateTime = usageTransUsgPeriodBoInstance.getDateTime(USAGE_START_DATETIME);
        usageEndDateTime = usageTransUsgPeriodBoInstance.getDateTime(USAGE_END_DATETIME);
        // Check if this is a master US or Sub US
        UsageSubscriptionRelationship usageSubscriptionRelationship = null;
        if(notNull(usageSubscription)){
	        usageSubscriptionRelationship = (UsageSubscriptionRelationship) usageSubscription
	                .getUsageSubscriptionRelationships().createFilter(
	                        " WHERE US_REL_TYPE_FLG='" + UsageSubscriptionRelationshipTypeLookup.constants.MASTER_US + "'",
	                        CmBuildSummarySQsAlgComp_Impl.class.getName()).firstRow();
        }
        if (notNull(usageSubscriptionRelationship)) {
            usageSubscriptionId = new UsageSubscription_Id(usageSubscriptionRelationship.getRelatedUsageSubscription())
                    .getEntity();
        } else {
            usageSubscriptionId = usageSubscription;
        }

        if (notNull(usageSubscriptionId)) {
            DetermineSubscriptionDvcCfgPeriodInputData detSubscriptionDvcCfgPeriodInputData = DetermineSubscriptionDvcCfgPeriodInputData.Factory
                    .newInstance();
            detSubscriptionDvcCfgPeriodInputData.setUsageStartDateTime(usageStartDateTime);
            detSubscriptionDvcCfgPeriodInputData.setUsageEndDateTime(usageEndDateTime);

            List<DetermineSubscriptionDvcCfgPeriodOutputData> detSubscriptionDvcCfgPeriodList = usageSubscriptionId
                    .retrieveDeviceConfigurationPeriod(detSubscriptionDvcCfgPeriodInputData);

            for (Iterator<DetermineSubscriptionDvcCfgPeriodOutputData> detSubscriptionDvcCfgPeriodOutputDataItr = detSubscriptionDvcCfgPeriodList
                    .iterator(); detSubscriptionDvcCfgPeriodOutputDataItr.hasNext();) {
                DetermineSubscriptionDvcCfgPeriodOutputData detSubscriptionDvcCfgPeriodOutputDataList = detSubscriptionDvcCfgPeriodOutputDataItr
                        .next();
                currentDCId = detSubscriptionDvcCfgPeriodOutputDataList.getDeviceConfiguration().toString();
                currentSPId = detSubscriptionDvcCfgPeriodOutputDataList.getServicePoint().toString();
                //Check if the previous DC is not equal to the current DC, check if current SP ID is same as previous SP ID and if both are yes set the meter exchange flag to True.
                if (notNull(previousDCId) && notNull(previousSPId) && !currentDCId.equals(previousDCId)
                        && currentSPId.equals(previousSPId)) {
                    isMeterExchange = true;
                    break;
                }
                previousDCId = currentDCId;
                previousSPId = currentSPId;
            }
        }
    }

    /**
     *   The usage period item details start and end dates will be expressed as date/times in the usage transaction â€“ but CC&B uses dates for billing.  
     *   These date/times should be rounded to the prior day.  
     *   For example, if a usage transaction  usage period ran from January 1 to February 1, and there was a new multi-item with a start date on January 15 at 3 PM, we would round the end date of the first period to January 14, and start date of the new period to January 15.
     * 
     */
    private void prepareItemDetails(COTSInstanceNode sendDetailsGroup) {
        DateTime itemStartDateTime = null;
        DateTime roundedItemStartDateTime = null;
        DateTime itemEndDateTime = null;
        DateTime roundedItemEndDateTime = null;
        DateTime usagePeriodEndDateTime = null;

        IntervalPeriodHelper helper = IntervalPeriodHelper.Factory.newInstance();
        TimeInterval MS_IN_DAY = new TimeInterval(new Long(86400000));

        COTSInstanceNode usagePeriodGrp = sendDetailsGroup.getGroup(USAGE_PERIOD_GROUP);
        COTSInstanceList usagePeriodListNode = usagePeriodGrp.getList(USAGE_PERIOD_LIST);

        for (COTSInstanceListNode usagePeriod : usagePeriodListNode) {
            COTSInstanceNode itemsGroup = usagePeriod.getGroup(ITEM_DETAILS_GROUP);
            COTSInstanceList itemsList = itemsGroup.getList(ITEM_DETAILS_LIST);

            if (isNull(itemsList) || itemsList.getSize() == 0) {
                continue;
            }

            usagePeriodEndDateTime = usagePeriod.getDateTime(USAGE_PERIOD_END_DATETIME);
            for (COTSInstanceListNode itemNode : itemsList) {
                itemStartDateTime = itemNode.getDateTime(ITEM_DETAILS_LIST_START_DATE_TIME);
                itemEndDateTime = itemNode.getDateTime(ITEM_DETAILS_LIST_END_DATE_TIME);
                //Update Start Date/Time
                roundedItemStartDateTime = helper.roundDateTime(itemStartDateTime, MS_IN_DAY,
                        RoundingMethodD1Lookup.constants.ROUND_DOWN);
                itemNode.set(ITEM_DETAILS_LIST_START_DATE_TIME, roundedItemStartDateTime);
                //Update End Date/Time
                if (itemEndDateTime.compareTo(usagePeriodEndDateTime) != 0) {
                    roundedItemEndDateTime = helper.roundDateTime(itemEndDateTime, MS_IN_DAY,
                            RoundingMethodD1Lookup.constants.ROUND_DOWN);
                    roundedItemEndDateTime = roundedItemEndDateTime.addDays(-1);
                    itemNode.set(ITEM_DETAILS_LIST_END_DATE_TIME, roundedItemEndDateTime);
                }
            }
        }
    }

    /**
     *   This method will validate the Algorithm Soft Params.
     */
    private void validateParameters() {
        if (notBlank(getUomDecimalPositions())) {
            String uomDecimalPositions = getUomDecimalPositions().trim();
            Pattern pattern = Pattern.compile("[0-9]");
            Matcher matcher = pattern.matcher(uomDecimalPositions);

            if (matcher.find()) {
                //Number
                int uomDecimalPositionsNumber = 0;
                try {
                    uomDecimalPositionsNumber = Integer.parseInt(uomDecimalPositions);
                } catch (Exception e) {
                    addError(MessageRepository.invalidUomDecimalPosition());
                }

                if (uomDecimalPositionsNumber >= 0 && uomDecimalPositionsNumber <= 6) {
                    performRounding = Bool.TRUE;
                    overridePrecision = uomDecimalPositionsNumber;
                } else {
                    addError(MessageRepository.invalidUomDecimalPosition());
                }
            } else if (uomDecimalPositions.equalsIgnoreCase("UOM")) {
                performRounding = Bool.TRUE;
            } else {
                addError(MessageRepository.invalidUomDecimalPosition());
            }
        }
    }

    /**
     *  This method will determine the UOM decimal precision based on SQI/UOM.
     *  
     * @param sqi
     * @param uom
     * @return int
     */
    private int determineUomDecimalPrecision(String sqi, String uom) {
        int precision = -1;
        if (performRounding.isTrue()) {
            //UOM Decimal Positions = UOM
            if (overridePrecision == -1) {
                if (notBlank(sqi)) {
                    ServiceQuantityIdentifierD1 sqiEntity = (new ServiceQuantityIdentifierD1_Id(sqi)).getEntity();
                    precision = sqiEntity.getDecimalPositions().intValue();
                } else if (notBlank(uom)) {
                    UnitOfMeasureD1 uomEntity = (new UnitOfMeasureD1_Id(uom)).getEntity();
                    precision = uomEntity.getDecimalPositions().intValue();
                }
            } else {
                //UOM Decimal Positions = 0 to 6 
                precision = overridePrecision;
            }
        }
        return precision;
    }

    /**
     *  This method will retrieves the rounded quantity to the given precision scale.
     * 
     * @param quantity
     * @param precision
     * @return BigDecimal
     */
    private BigDecimal retrieveRoundedQuantity(BigDecimal quantity, int precision) {
        java.math.BigDecimal roundedSQListDataQuantity = new java.math.BigDecimal(quantity.toString());
        roundedSQListDataQuantity = roundedSQListDataQuantity.setScale(precision, RoundingMode.HALF_UP);
        return (new BigDecimal(roundedSQListDataQuantity));
    }

    /**
     *  This method will retrieves the raw data formatted SQ Intervals.
     * 
     * @param summarySQIntervalList
     * @param precision
     * @return
     */

    private String retrieveSQIntervalsRawData(
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Interval> summarySQIntervalList,
            int precision) {
        StringBuilder intervalListStringBuilder = new StringBuilder();

        if (notNull(summarySQIntervalList) && summarySQIntervalList.size() > 0) {
            intervalListStringBuilder.append("<intervals>");
            for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Interval summarySQIntervalData : summarySQIntervalList) {
                BigDecimal sQIntervalDataQuantity = summarySQIntervalData.getQuantity();
                if (precision > -1) {
                    sQIntervalDataQuantity = retrieveRoundedQuantity(sQIntervalDataQuantity, precision);
                }
                intervalListStringBuilder.append(START_TAG).append(SQ_INTERVALS_LIST).append(END_TAG);
                intervalListStringBuilder.append(START_TAG + INTERVAL_SEQUENCE + END_TAG
                        + summarySQIntervalData.getSequence() + CLOSE_TAG + INTERVAL_SEQUENCE + END_TAG);
                intervalListStringBuilder.append(START_TAG + INTERVAL_DATETIME + END_TAG
                        + summarySQIntervalData.getDateTime() + CLOSE_TAG + INTERVAL_DATETIME + END_TAG);
                intervalListStringBuilder.append(START_TAG + INTERVAL_QUANTITY + END_TAG + sQIntervalDataQuantity
                        + CLOSE_TAG + INTERVAL_QUANTITY + END_TAG);
                intervalListStringBuilder.append(START_TAG + INTERVAL_CONDITION + END_TAG
                        + summarySQIntervalData.getCondition().getValue() + CLOSE_TAG + INTERVAL_CONDITION + END_TAG);
                intervalListStringBuilder.append(CLOSE_TAG + SQ_INTERVALS_LIST + END_TAG);
            }
            intervalListStringBuilder.append("</intervals>");
        }

        return intervalListStringBuilder.toString();
    }
}