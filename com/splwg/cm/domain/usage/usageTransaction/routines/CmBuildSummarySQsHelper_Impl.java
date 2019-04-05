package com.splwg.cm.domain.usage.usageTransaction.routines;

import java.util.ArrayList;

import java.util.Iterator;
import java.util.List;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.GenericBusinessComponent;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.LookupHelper;
import com.splwg.base.api.datatypes.TimeInterval;
import com.splwg.base.api.installation.InstallationHelper;
import com.splwg.base.domain.common.businessObject.BusinessObject;
import com.splwg.base.domain.common.businessObject.BusinessObject_Id;
import com.splwg.base.domain.common.extendedLookupValue.ExtendedLookupValue_Id;
import com.splwg.base.domain.common.timeZone.TimeZone;
import com.splwg.base.domain.common.timeZone.TimeZone_Id;
import com.splwg.d1.api.lookup.ConditionCodeOptionLookup;
import com.splwg.d1.api.lookup.ExtractIntervalDataLookup;
import com.splwg.d1.api.lookup.MeasurementConditionProcessingMethodLookup;
import com.splwg.d1.api.lookup.MeasuresPeakQuantityLookup;
import com.splwg.d1.api.lookup.RoundingMethodD1Lookup;
import com.splwg.d1.api.lookup.ServicePointIdentifierTypeLookup;
import com.splwg.d1.domain.admin.serviceProvider.entities.ServiceProviderD1_Id;
import com.splwg.d1.domain.admin.unitOfMeasure.entities.UnitOfMeasureD1;
import com.splwg.d1.domain.admin.unitOfMeasure.entities.UnitOfMeasureD1_Id;
import com.splwg.d1.domain.common.routines.IntervalPeriodHelper;
import com.splwg.d1.domain.common.routines.ShiftLegalDateTimeToStandard;
import com.splwg.d1.domain.common.routines.ShiftStandardDateTimeToLegal;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointD1_Id;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointIdentifier;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointIdentifier_Id;
import com.splwg.d1.domain.measurement.measurementServices.axisConversion.AxisConversionInputData;
import com.splwg.d1.domain.measurement.measurementServices.axisConversion.AxisConversionOutputData;
import com.splwg.d1.domain.measurement.measurementServices.common.ConsumptionAmountAndConditionData;
import com.splwg.d1.domain.usage.usageSubscription.entities.UsageSubscription;
import com.splwg.d2.api.lookup.ExternalSpIdOptionLookup;
import com.splwg.d2.api.lookup.UsageTypeD2Lookup;
import com.splwg.d2.domain.usage.MessageRepository;
import com.splwg.d2.domain.usage.usageTransaction.data.ScalarDetailsInputOutputData;
import com.splwg.cm.domain.usage.usageTransaction.data.CmSummaryUsagePeriodsOutputData;
import com.splwg.cm.domain.usage.usageTransaction.data.CmUsagePeriodsInputData;

/**
 * @author Abjayon
 *
 * @BusinessComponent (customizationReplaceable = false, customizationCallable = true)
 */

public class CmBuildSummarySQsHelper_Impl
        extends GenericBusinessComponent
        implements CmBuildSummarySQsHelper {

    private static final String DERIVED = "999999";
    private static final String NO_READ_SYSTEM = "100000";
    private static final String USAGE_TRAN_EXPORT_CONFIG_BO = "D2-UsageTranExportConfig";
    private static final String EXTERNAL_SP_ID_OPTION = "externalSpIdOption";

    private static final String MAX = "MAXIMUM";
    private static final String MIN = "MINIMUM";
    
    private static final String CONDITION_CODE_OPTION = "conditionCodeOption";

    private BusinessObject conditionLookupBO = null;
    private Boolean extractCurve = Boolean.FALSE;
    private TimeInterval highestSPI = null;
    private TimeZone usageTimeZone = null;
    private ShiftLegalDateTimeToStandard shiftLegalDateTimeToStandard = null;
    private ShiftStandardDateTimeToLegal shiftStandardDateTimeToLegal = null;

    private BusinessObject highlightTypeLookupBO = new BusinessObject_Id("D2-SQDttmHightlightTypeLookup").getEntity();
    private Bool updateHighlightList = Bool.FALSE;
    private Bool clearHighlightList = Bool.FALSE;
    private Bool modifyHighlightList = Bool.FALSE;
    private Bool checkForMixedHighlightTypes = Bool.FALSE;
    private int highlightListSize = 0;
    private int usgPeriodHighlightListSize = 0;
    private ExtendedLookupValue_Id maxHighlightType = new ExtendedLookupValue_Id(highlightTypeLookupBO, MAX);
    private ExtendedLookupValue_Id minHighlightType = new ExtendedLookupValue_Id(highlightTypeLookupBO, MIN);
    private int usgPeriodMaxHighlightTypeCount = 0;
    private int usgPeriodMinHighlightTypeCount = 0;
    private int summaryUsgPeriodMaxHighlightTypeCount = 0;
    private int summaryUsgPeriodMinHighlightTypeCount = 0;
    private int countList = 0;
    private int summaryUsagePeriodHighliglightLisstSize = 0;
    private int spSQHighliglightLisstSize = 0;
    private Bool highlightTypeOnlyMax = Bool.FALSE;
    private Bool highlightTypeOnlyMin = Bool.FALSE;
    private Bool highlightTypeMixed = Bool.FALSE;

    private BusinessObjectInstance utExportConfigObjectInstance = null;
    private ExternalSpIdOptionLookup externalSpIdOption = null;
    
    private ConditionCodeOptionLookup conditionCodeOption;

    /**
     *  This method will build the Summary Usage Periods.
     *  
     *   @param usagePeriodsInputData
     *   @return SummaryUsagePeriodsOutputData
     */
    public CmSummaryUsagePeriodsOutputData buildSummaryUsagePeriods(CmUsagePeriodsInputData usagePeriodsInputData) {
        shiftLegalDateTimeToStandard = ShiftLegalDateTimeToStandard.Factory.newInstance();
        shiftStandardDateTimeToLegal = ShiftStandardDateTimeToLegal.Factory.newInstance();

        CmSummaryUsagePeriodsOutputData cmSummaryUsagePeriodsOutputData = new CmSummaryUsagePeriodsOutputData();
        List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> summaryUsagePeriodList = new ArrayList<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod>();

        UsageSubscription usageSubscription = usagePeriodsInputData.getUsageSubscription();
        TimeInterval targetSPI = usagePeriodsInputData.getTargetSPI();
        List<CmUsagePeriodsInputData.UsagePeriod> usagePeriodList = usagePeriodsInputData.getUsagePeriodList();
        conditionLookupBO = new BusinessObject_Id("D2-UTSQIntvlConditionLookup").getEntity();

        if (notNull(usageSubscription) && notNull(usageSubscription.getTimeZone())) {
            usageTimeZone = (new TimeZone_Id(usageSubscription.getTimeZone())).getEntity();
        }
        
        if(isNull(usageTimeZone)){
        	usageTimeZone = InstallationHelper.fetchTimeZone();
        }

        ServicePointIdentifierTypeLookup spIdentifierTypeLookup = null;
        if(notNull(usageSubscription)){
	        ServiceProviderD1_Id usServiceProviderId = notNull(usageSubscription.getServiceProviderId()) ? usageSubscription
	                .getServiceProviderId()
	                : usageSubscription.getUsageSubscriptionType().getServiceProviderId();
	
	        String serviceProviderBODataAreaString = null;
	        if(notNull(usServiceProviderId)){
	        	serviceProviderBODataAreaString = usServiceProviderId.getEntity().getBusinessObjectDataArea();
	        }
	        String spIdentifierTypeStartTag = "<servicePointIdentifierType>";
	        String spIdentifierTypeEndTag = "</servicePointIdentifierType>";
	        String spIdentifierTypeString = null;
	
	        if (notNull(serviceProviderBODataAreaString)
	                && serviceProviderBODataAreaString.contains(spIdentifierTypeStartTag)
	                && serviceProviderBODataAreaString.contains(spIdentifierTypeEndTag)) {
	
	            spIdentifierTypeString = serviceProviderBODataAreaString.substring(serviceProviderBODataAreaString
	                    .indexOf(spIdentifierTypeStartTag)
	                    + spIdentifierTypeStartTag.length(), serviceProviderBODataAreaString
	                    .indexOf(spIdentifierTypeEndTag));
	        }
	
	        if (notNull(spIdentifierTypeString)) {
	            spIdentifierTypeLookup = LookupHelper.getLookupInstance(ServicePointIdentifierTypeLookup.class,
	                    spIdentifierTypeString);
	        }
        }

        for (CmUsagePeriodsInputData.UsagePeriod usagePeriodListData : usagePeriodList) {
            CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod summaryUsagePeriod = new CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod();
            summaryUsagePeriod.setSequence(usagePeriodListData.getSequence());
            summaryUsagePeriod.setStartDateTime(usagePeriodListData.getStartDateTime());
            summaryUsagePeriod.setEndDateTime(usagePeriodListData.getEndDateTime());

            summaryUsagePeriod.setStandardStartDateTime(shiftLegalDateTimeToStandard.shiftToStandard(usageTimeZone,
                    usageTimeZone, usagePeriodListData.getStartDateTime(), Bool.TRUE));
            summaryUsagePeriod.setStandardEndDateTime(shiftLegalDateTimeToStandard.shiftToStandard(usageTimeZone,
                    usageTimeZone, usagePeriodListData.getEndDateTime(), Bool.TRUE));

            summaryUsagePeriod.setUsageType(usagePeriodListData.getUsageType());
            summaryUsagePeriod.setServiceQuantityList(retrieveSQList(usagePeriodListData.getServiceQuantityList(),
                    targetSPI, usagePeriodListData.getStartDateTime(), usagePeriodListData.getEndDateTime(),
                    usagePeriodListData.getUsageType()));
            summaryUsagePeriod.setItemList(retrieveItemList(usagePeriodListData.getItemList()));
            summaryUsagePeriod.setSPServiceQuantityList(retrieveSPSQList(usagePeriodListData.getServiceQuantityList(),
                    spIdentifierTypeLookup));
            summaryUsagePeriod.setSpItemList(retrieveSPItemList(usagePeriodListData.getItemList(),
                    spIdentifierTypeLookup));
            summaryUsagePeriodList.add(summaryUsagePeriod);

        }

        summaryUsagePeriodList = retrieveMixedIntervalAndScalarSummaryUsagePeriods(summaryUsagePeriodList);
        cmSummaryUsagePeriodsOutputData.setSummaryUsagePeriodList(summaryUsagePeriodList);
        return cmSummaryUsagePeriodsOutputData;
    }

    /**
     *  This method will build the Scalar Details with the updated CCB External SP Identifiers.
     *  
     *  @param scalarDetailsInputData
     *  @return ScalarDetailsInputOutputData
     */
    public ScalarDetailsInputOutputData buildScalarDetails(ScalarDetailsInputOutputData scalarDetailsInputData) {
        ScalarDetailsInputOutputData scalarDetailsInputOutputData = new ScalarDetailsInputOutputData();
        List<ScalarDetailsInputOutputData.ScalarDetail> scalarDetailsInputList = scalarDetailsInputData
                .getScalarDetailList();

        ServicePointIdentifierTypeLookup spIdentifierTypeLookup = null;
        UsageSubscription usageSubscription = scalarDetailsInputData.getUsageSubscription();
        if(notNull(usageSubscription)){
	        ServiceProviderD1_Id usServiceProviderId = notNull(usageSubscription.getServiceProviderId()) ? usageSubscription
	                .getServiceProviderId()
	                : usageSubscription.getUsageSubscriptionType().getServiceProviderId();
	
	        String serviceProviderBODataAreaString = usServiceProviderId.getEntity().getBusinessObjectDataArea();
	        String spIdentifierTypeStartTag = "<servicePointIdentifierType>";
	        String spIdentifierTypeEndTag = "</servicePointIdentifierType>";
	        String spIdentifierTypeString = null;
	        
	
	        if (notNull(serviceProviderBODataAreaString)
	                && serviceProviderBODataAreaString.contains(spIdentifierTypeStartTag)
	                && serviceProviderBODataAreaString.contains(spIdentifierTypeEndTag)) {
	
	            spIdentifierTypeString = serviceProviderBODataAreaString.substring(serviceProviderBODataAreaString
	                    .indexOf(spIdentifierTypeStartTag)
	                    + spIdentifierTypeStartTag.length(), serviceProviderBODataAreaString
	                    .indexOf(spIdentifierTypeEndTag));
	        }
	
	        if (notNull(spIdentifierTypeString)) {
	            spIdentifierTypeLookup = LookupHelper.getLookupInstance(ServicePointIdentifierTypeLookup.class,
	                    spIdentifierTypeString);
	        }
        }

        for (ScalarDetailsInputOutputData.ScalarDetail scalarDetailsInputListData : scalarDetailsInputList) {
            String mdmSp = scalarDetailsInputListData.getSpId();

            String mdmSPIdentifier = null;
            if (notNull(spIdentifierTypeLookup) && !spIdentifierTypeLookup.isBlankLookupValue()) {
                mdmSPIdentifier = getSPIdentifier(mdmSp, spIdentifierTypeLookup);
                if (notNull(mdmSPIdentifier)) {
                    scalarDetailsInputListData.setExternalSpId(mdmSPIdentifier);
                }
            }

            /*  If no external SP ID Type is configured on Service Provider, then check the "External SP Id Option" element value in UT Export Master Configuration.
             *  If the element value is "Populate with MDM SP Id", populate summary SQs with MDM SP ID
             *  If the element value is blank, populate summary SQs with MDM External Service Point ID (D1EI), thrown an error if MDM External Service Point ID is not found
             *  */
            if (isNull(mdmSPIdentifier)) {
            	retrieveUTExportConfigBOInstance();
                if (notNull(utExportConfigObjectInstance)) {
                       externalSpIdOption = (ExternalSpIdOptionLookup) utExportConfigObjectInstance
                                .getLookup(EXTERNAL_SP_ID_OPTION);
                }

                if (notNull(externalSpIdOption) && externalSpIdOption.isPopulateWithMdmSpId()) {
                    mdmSPIdentifier = mdmSp;
                } else {
                    mdmSPIdentifier = getSPIdentifier(mdmSp, ServicePointIdentifierTypeLookup.constants.EXTERNAL_ID);
                }

                if (notNull(mdmSPIdentifier)) {
                    scalarDetailsInputListData.setExternalSpId(mdmSPIdentifier);
                } else {
                    addError(MessageRepository.moreThenServicePointFound(mdmSp));
                }
            }
        }

        scalarDetailsInputOutputData.setScalarDetailList(scalarDetailsInputList);

        return scalarDetailsInputOutputData;
    }

    /**
     *  This method will retrieve the Summary Usage Period SQ Lists.
     *  
     *   @param usgPeriodServiceQuantityList
     *   @param targetSPI
     *   @param startDateTime
     *   @param endDateTime
     *   @return List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity>
     *  
     */
    private List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity> retrieveSQList(
            List<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity> usgPeriodServiceQuantityList,
            TimeInterval targetSPI, DateTime startDateTime, DateTime endDateTime, UsageTypeD2Lookup usageType) {
        List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity> summarySQList = new ArrayList<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity>();
        insertSummaryUsagePeriods(usgPeriodServiceQuantityList, summarySQList);
        if (extractCurve.booleanValue()) {
            prepareSummaryUsagePeriodSQIntervals(usgPeriodServiceQuantityList, summarySQList, targetSPI, startDateTime,
                    endDateTime, usageType);
        }

        return summarySQList;
    }

    /**
     * This method will retrieves Summary Usage Period Item list
     * @param inputUsgPeriodItemList
     * @return
     */
    private List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.Item> retrieveItemList(
            List<CmUsagePeriodsInputData.UsagePeriod.Item> inputUsgPeriodItemList) {
        List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.Item> outputSummaryItemList = new ArrayList<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.Item>();
        if (isNull(inputUsgPeriodItemList) || inputUsgPeriodItemList.size() == 0) return outputSummaryItemList;

        BigDecimal sequence = BigDecimal.ONE;
        for (CmUsagePeriodsInputData.UsagePeriod.Item inputItemData : inputUsgPeriodItemList) {

            boolean isUpdate = updateSummarisedItemListBasedOnUOMItemTypeAndDateRangeMatch(outputSummaryItemList,
                    inputItemData);

            if (!isUpdate) {
                CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.Item outputItem = new CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.Item();
                outputItem.setItemSequence(sequence);
                sequence = sequence.add(BigDecimal.ONE);
                outputItem.setItemType(inputItemData.getItemType());
                outputItem.setItemCount(inputItemData.getItemCount());
                outputItem.setDailyServiceQuantity(inputItemData.getDailyServiceQuantity());
                outputItem.setStartDateTime(inputItemData.getStartDateTime());
                outputItem.setEndDateTime(inputItemData.getEndDateTime());
                outputItem.setUom(inputItemData.getUom());
                outputSummaryItemList.add(outputItem);
            }

        }
        return outputSummaryItemList;
    }

    /**
     * This method checks if UOM/ItemType/StartDate/EndDateTime is match in Summarized Item List  then it will update summarized item details quantity based on UOM  and returns with  true else returns false
     * @param outputSummaryItemList
     * @param inputItem
     * @return
     */
    private boolean updateSummarisedItemListBasedOnUOMItemTypeAndDateRangeMatch(
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.Item> outputSummaryItemList,
            CmUsagePeriodsInputData.UsagePeriod.Item inputItem) {

        for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.Item outputItem : outputSummaryItemList) {
            if (checkIfCodeExist(inputItem.getUom(), outputItem.getUom())
                    && checkIfCodeExist(inputItem.getItemType(), outputItem.getItemType())
                    && checkIfDateCodeExist(inputItem.getStartDateTime(), outputItem.getStartDateTime())
                    && checkIfDateCodeExist(inputItem.getEndDateTime(), outputItem.getEndDateTime())) {

                outputItem.setItemCount(outputItem.getItemCount().add(inputItem.getItemCount()));
                return true;
            }
        }
        return false;
    }

    /**
     *  This method will retrieves Summary Usage Period SP Item List
     * @param inputUsagePeriodSPItemList
     * @return
     */
    private List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.SPItem> retrieveSPItemList(
            List<CmUsagePeriodsInputData.UsagePeriod.Item> inputUsagePeriodSPItemList,
            ServicePointIdentifierTypeLookup spIdentifierTypeLookup) {
        List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.SPItem> outputSummarySPItemList = new ArrayList<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.SPItem>();
        BigDecimal seq = BigDecimal.ONE;
        if (isNull(inputUsagePeriodSPItemList) || inputUsagePeriodSPItemList.size() == 0)
            return outputSummarySPItemList;

        for (CmUsagePeriodsInputData.UsagePeriod.Item inputSPItemData : inputUsagePeriodSPItemList) {
            CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.SPItem outputSPItemData = new CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.SPItem();

            outputSPItemData.setItemCount(inputSPItemData.getItemCount());
            outputSPItemData.setItemSequence(seq);
            outputSPItemData.setItemType(inputSPItemData.getItemType());
            outputSPItemData.setQuantity(inputSPItemData.getQuantity());
            outputSPItemData.setStartDateTime(inputSPItemData.getStartDateTime());
            outputSPItemData.setEndDateTime(inputSPItemData.getEndDateTime());
            outputSPItemData.setUom(inputSPItemData.getUom());
            if (isBlankOrNull(inputSPItemData.getSpId())) {
                continue;
            }

            String externalSpId = null;
            if (notNull(spIdentifierTypeLookup) && !spIdentifierTypeLookup.isBlankLookupValue()) {
                externalSpId = getSPIdentifier(inputSPItemData.getSpId(), spIdentifierTypeLookup);
            }
            /*  If no external SP ID Type is configured on Service Provider, then check the "External SP Id Option" element value in UT Export Master Configuration.
             *  If the element value is "Populate with MDM SP Id", populate summary SQs with MDM SP ID
             *  If the element value is blank, populate summary SQs with MDM External Service Point ID (D1EI)
             *  */
            if (isNull(externalSpId)) {
            	retrieveUTExportConfigBOInstance();
                if (notNull(utExportConfigObjectInstance)) {
                    externalSpIdOption = (ExternalSpIdOptionLookup) utExportConfigObjectInstance
                                .getLookup(EXTERNAL_SP_ID_OPTION);
                }

                if (notNull(externalSpIdOption) && externalSpIdOption.isPopulateWithMdmSpId()) {
                    externalSpId = inputSPItemData.getSpId();
                } else {
                    externalSpId = getSPIdentifier(inputSPItemData.getSpId(),
                            ServicePointIdentifierTypeLookup.constants.EXTERNAL_ID);
                }
            }

            if (notNull(externalSpId)) {
                outputSPItemData.setSpId(externalSpId);
            }
            outputSummarySPItemList.add(outputSPItemData);
            seq = seq.add(BigDecimal.ONE);
        }
        return outputSummarySPItemList;
    }

    /**
     *  This method will retrieve the Summary Usage Period SPSQLists.
     *  
     *  @param usgPeriodServiceQuantityList
     *  @return  List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity>
     */
    private List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity> retrieveSPSQList(
            List<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity> usgPeriodServiceQuantityList,
            ServicePointIdentifierTypeLookup spIdentifierTypeLookup) {
        List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity> summarySPSQList = new ArrayList<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity>();
        String prevSpId = null;
        int spSQSequence = 0;
        Bool isUpdate = Bool.FALSE;
        int spSQPeriodHighlightListSequence = 0;

        for (CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity usgPeriodServiceQuantityData : usgPeriodServiceQuantityList) {
            String uomCode = usgPeriodServiceQuantityData.getUom();
            String touCode = usgPeriodServiceQuantityData.getTou();
            String sqiCode = usgPeriodServiceQuantityData.getSqi();
            String spId = usgPeriodServiceQuantityData.getSpId();
            BigDecimal quantity = usgPeriodServiceQuantityData.getQuantity();
            TimeInterval spi = usgPeriodServiceQuantityData.getSecondsPerInterval();
            isUpdate = Bool.FALSE;
            List<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight> usagePeriodHighlightList = usgPeriodServiceQuantityData
                    .getHighlightList();
            List<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight> usagePeriodHighlightListTemp = new ArrayList<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight>();

            if (isBlankOrNull(spId)) {
                continue;
            }

            String spIdentifier = null;
            if (notNull(spIdentifierTypeLookup) && !spIdentifierTypeLookup.isBlankLookupValue()) {
                spIdentifier = getSPIdentifier(spId, spIdentifierTypeLookup);
            }

            /*  If no external SP ID Type is configured on Service Provider, then check the "External SP Id Option" element value in UT Export Master Configuration.
             *  If the element value is "Populate with MDM SP Id", populate summary SQs with MDM SP ID
             *  If the element value is blank, populate summary SQs with MDM External Service Point ID (D1EI) 
             *  */
            if (isNull(spIdentifier)) {
            	retrieveUTExportConfigBOInstance();
                if (notNull(utExportConfigObjectInstance)) {
                    externalSpIdOption = (ExternalSpIdOptionLookup) utExportConfigObjectInstance
                                .getLookup(EXTERNAL_SP_ID_OPTION);
                }
                if (notNull(externalSpIdOption) && externalSpIdOption.isPopulateWithMdmSpId()) {
                    spIdentifier = spId;
                } else {
                    spIdentifier = getSPIdentifier(spId, ServicePointIdentifierTypeLookup.constants.EXTERNAL_ID);
                }
            }

            for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity summaryServiceQuantityData : summarySPSQList) {
                String summaryUomCode = summaryServiceQuantityData.getUom();
                String summaryTouCode = summaryServiceQuantityData.getTou();
                String summarySqiCode = summaryServiceQuantityData.getSqi();
                String summarySpId = summaryServiceQuantityData.getSpId();
                BigDecimal summaryQuantity = summaryServiceQuantityData.getQuantity();

                //Fetch the highlight date/time list, process and accordingly set the details onto the summary usage period SP SQ List
                List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight> summarySPSQHighlightList = summaryServiceQuantityData
                        .getHighlightList();

                if (checkIfCodeExist(spIdentifier, summarySpId) && checkIfCodeExist(uomCode, summaryUomCode)
                        && checkIfCodeExist(sqiCode, summarySqiCode) && checkIfCodeExist(touCode, summaryTouCode)) {

                    checkForMixedHighlightTypes = Bool.FALSE;
                    checkHighlightTypes(usagePeriodHighlightList, summarySPSQHighlightList);
                    if (notNull(summarySPSQHighlightList)) {
                        spSQPeriodHighlightListSequence = summarySPSQHighlightList.size();
                    }
                    quantity = determineQuantity(summaryUomCode, summaryQuantity, quantity);
                    summaryServiceQuantityData.setQuantity(quantity);

                    if (updateHighlightList.isTrue()) {
                        List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight> summaryHighlightList;
                        summaryHighlightList = new ArrayList<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight>();
                        for (CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight usgPeriodHighlightList : usagePeriodHighlightList) {
                            CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight highlightData = new CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight();
                            highlightData.setSequence(usgPeriodHighlightList.getSequence());
                            highlightData.setHighlightDateTime(usgPeriodHighlightList.getHighlightDateTime());
                            highlightData.setHighlightType(usgPeriodHighlightList.getHighlightType());
                            highlightData.setHighlightCondition(usgPeriodHighlightList.getHighlightCondition());
                            highlightData.setHighlightDerivedCondition(usgPeriodHighlightList
                                    .getHighlightDerivedCondition());
                            summaryHighlightList.add(highlightData);
                        }
                        summaryServiceQuantityData.setHighlightList(summaryHighlightList);
                    }
                    if (modifyHighlightList.isTrue()) {
                        if (notNull(summarySPSQHighlightList)) {
                            spSQHighliglightLisstSize = summarySPSQHighlightList.size();
                        }
                        for (CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight usgPeriodHighlightList : usagePeriodHighlightList) {
                            countList = 0;
                            for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight summaryPeriodHighlightList : summarySPSQHighlightList) {
                                if (!usgPeriodHighlightList.getHighlightDateTime().equals(
                                        summaryPeriodHighlightList.getHighlightDateTime())) countList++;
                            }
                            if (countList == spSQHighliglightLisstSize) {
                                spSQPeriodHighlightListSequence++;
                                CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight highlightData = new CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight();
                                highlightData.setSequence(new BigDecimal(spSQPeriodHighlightListSequence));
                                highlightData.setHighlightDateTime(usgPeriodHighlightList.getHighlightDateTime());
                                highlightData.setHighlightType(usgPeriodHighlightList.getHighlightType());
                                highlightData.setHighlightCondition(usgPeriodHighlightList.getHighlightCondition());
                                highlightData.setHighlightDerivedCondition(usgPeriodHighlightList
                                        .getHighlightDerivedCondition());
                                usagePeriodHighlightListTemp.add(highlightData);
                            }
                        }
                        for (CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight usgPeriodHighlightList : usagePeriodHighlightListTemp) {
                            CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight highlightData = new CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight();
                            highlightData.setSequence(usgPeriodHighlightList.getSequence());
                            highlightData.setHighlightDateTime(usgPeriodHighlightList.getHighlightDateTime());
                            highlightData.setHighlightType(usgPeriodHighlightList.getHighlightType());
                            highlightData.setHighlightCondition(usgPeriodHighlightList.getHighlightCondition());
                            highlightData.setHighlightDerivedCondition(usgPeriodHighlightList
                                    .getHighlightDerivedCondition());
                            summarySPSQHighlightList.add(highlightData);
                        }
                        summaryServiceQuantityData.setHighlightList(summarySPSQHighlightList);
                    }
                    if (clearHighlightList.isTrue()) {
                        summaryServiceQuantityData.setHighlightList(null);
                    }
                    isUpdate = Bool.TRUE;
                }
            }
            if (isUpdate.isFalse()) {
                // Get External SP Id
                String mdmSPIdentifier = null;
                if (notNull(spIdentifierTypeLookup) && !spIdentifierTypeLookup.isBlankLookupValue()) {
                    mdmSPIdentifier = getSPIdentifier(spId, spIdentifierTypeLookup);
                    if (notNull(mdmSPIdentifier)) {
                        usgPeriodServiceQuantityData.setSpId(mdmSPIdentifier);
                    }
                }

                /*  If no SP ID Type is configured on Service Provider, then check the "External SP Id Option" element value in UT Export Master Configuration.
                 *  If the element value is "Populate with MDM SP Id", populate summary SQs with MDM SP ID
                 *  If the element value is blank, populate summary SQs with MDM External Service Point ID (D1EI),  throw an error if MDM External Service Point ID is not found
                 *  */
                if (isNull(mdmSPIdentifier)) {
                	retrieveUTExportConfigBOInstance();
                    if (notNull(utExportConfigObjectInstance)) {
                        externalSpIdOption = (ExternalSpIdOptionLookup) utExportConfigObjectInstance
                                    .getLookup(EXTERNAL_SP_ID_OPTION);
                    }

                    if (notNull(externalSpIdOption) && externalSpIdOption.isPopulateWithMdmSpId()) {
                        mdmSPIdentifier = spId;
                    } else {
                        mdmSPIdentifier = getSPIdentifier(spId, ServicePointIdentifierTypeLookup.constants.EXTERNAL_ID);
                    }

                    if (notNull(mdmSPIdentifier)) {
                        usgPeriodServiceQuantityData.setSpId(mdmSPIdentifier);
                    } else {
                        addError(MessageRepository.moreThenServicePointFound(spId));
                    }
                }

                if (isNull(prevSpId)) {
                    prevSpId = spId;
                }
                if (prevSpId.equals(spId)) {
                    spSQSequence++;
                } else {
                    spSQSequence = 1;
                }
                prevSpId = spId;

                CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity summaryUsgPeriodServiceQuantityData = new CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity();
                summaryUsgPeriodServiceQuantityData.setSequence(new BigDecimal(spSQSequence));
                summaryUsgPeriodServiceQuantityData.setUom(uomCode);
                summaryUsgPeriodServiceQuantityData.setTou(touCode);
                summaryUsgPeriodServiceQuantityData.setSqi(sqiCode);
                summaryUsgPeriodServiceQuantityData.setQuantity(quantity);
                summaryUsgPeriodServiceQuantityData.setSecondsPerInterval(spi);
                summaryUsgPeriodServiceQuantityData.setSpId(mdmSPIdentifier);

                UnitOfMeasureD1 uom = new UnitOfMeasureD1_Id(uomCode).getEntity();
                MeasuresPeakQuantityLookup measPeakQuantityFlag = uom.getMeasuresPeakQuantity();
                if (notNull(usagePeriodHighlightList) && usagePeriodHighlightList.size() > 0
                        && measPeakQuantityFlag.isMeasurePeakQuantity()) {
                    List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight> summaryHighlightList;
                    summaryHighlightList = new ArrayList<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight>();
                    for (CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight usgPeriodHighlightList : usagePeriodHighlightList) {
                        CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight highlightData = new CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight();
                        highlightData.setSequence(usgPeriodHighlightList.getSequence());
                        highlightData.setHighlightDateTime(usgPeriodHighlightList.getHighlightDateTime());
                        highlightData.setHighlightType(usgPeriodHighlightList.getHighlightType());
                        highlightData.setHighlightCondition(usgPeriodHighlightList.getHighlightCondition());
                        highlightData.setHighlightDerivedCondition(usgPeriodHighlightList
                                .getHighlightDerivedCondition());
                        summaryHighlightList.add(highlightData);
                    }
                    summaryUsgPeriodServiceQuantityData.setHighlightList(summaryHighlightList);
                }
                summarySPSQList.add(summaryUsgPeriodServiceQuantityData);
            }
        }

        return summarySPSQList;
    }

    /**
     *  This method will inserts or update the Summary Usage Period SQLists.
     * 
     * @param usgPeriodServiceQuantityList
     * @param summarySQList
     */
    private void insertSummaryUsagePeriods(
            List<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity> usgPeriodServiceQuantityList,
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity> summarySQList) {
        Bool isUpdate = Bool.FALSE;
        int sQSequence = 1;
        Bool spFound = Bool.FALSE;
        int summaryUsgPeriodHighlightListSequence = 0;

        for (CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity usgPeriodServiceQuantityData : usgPeriodServiceQuantityList) {
            String uomCode = usgPeriodServiceQuantityData.getUom();
            String touCode = usgPeriodServiceQuantityData.getTou();
            String sqiCode = usgPeriodServiceQuantityData.getSqi();
            BigDecimal quantity = usgPeriodServiceQuantityData.getQuantity();
            TimeInterval spi = usgPeriodServiceQuantityData.getSecondsPerInterval();
            ExtractIntervalDataLookup shouldExtractIntervalData = usgPeriodServiceQuantityData
                    .getShouldExtractInterval();
            isUpdate = Bool.FALSE;
            String spId = usgPeriodServiceQuantityData.getSpId();
            List<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight> usagePeriodHighlightList = usgPeriodServiceQuantityData
                    .getHighlightList();
            List<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight> usagePeriodHighlightListTemp = new ArrayList<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight>();

            if (notBlank(spId))
                spFound = Bool.TRUE;
            else
                spFound = Bool.FALSE;

            for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity summaryServiceQuantityData : summarySQList) {
                String summaryUomCode = summaryServiceQuantityData.getUom();
                String summaryTouCode = summaryServiceQuantityData.getTou();
                String summarySqiCode = summaryServiceQuantityData.getSqi();
                BigDecimal summaryQuantity = summaryServiceQuantityData.getQuantity();
                List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight> summaryUsgPeriodHighlightList = summaryServiceQuantityData
                        .getHighlightList();

                if (checkIfCodeExist(uomCode, summaryUomCode) && checkIfCodeExist(sqiCode, summarySqiCode)
                        && checkIfCodeExist(touCode, summaryTouCode)) {
                    checkForMixedHighlightTypes = Bool.TRUE;
                    checkHighlightTypes(usagePeriodHighlightList, summaryUsgPeriodHighlightList);
                    if (notNull(summaryUsgPeriodHighlightList)) {
                        summaryUsgPeriodHighlightListSequence = summaryUsgPeriodHighlightList.size();
                    }
                    quantity = determineQuantity(summaryUomCode, summaryQuantity, quantity);
                    summaryServiceQuantityData.setQuantity(quantity);
                    // if SP ID is found then clear the highlight list. As we should be populating the Highlight list under summary SQs only if all the entries doesnt have a SP.
                    if (notBlank(spId) && spFound.isTrue()) {
                        summaryServiceQuantityData.setHighlightList(null);
                    }
                    // if SP ID is not fund then update the existing list with new one.
                    else {
                        if (spFound.isFalse() && updateHighlightList.isTrue()) {
                            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight> summaryHighlightList = new ArrayList<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight>();
                            for (CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight usgPeriodHighlightList : usagePeriodHighlightList) {
                                CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight highlightData = new CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight();
                                highlightData.setSequence(usgPeriodHighlightList.getSequence());
                                highlightData.setHighlightDateTime(usgPeriodHighlightList.getHighlightDateTime());
                                highlightData.setHighlightType(usgPeriodHighlightList.getHighlightType());
                                highlightData.setHighlightCondition(usgPeriodHighlightList.getHighlightCondition());
                                highlightData.setHighlightDerivedCondition(usgPeriodHighlightList
                                        .getHighlightDerivedCondition());
                                summaryHighlightList.add(highlightData);
                            }
                            summaryServiceQuantityData.setHighlightList(summaryHighlightList);
                        }
                        if (spFound.isFalse() && modifyHighlightList.isTrue()) {
                            if (notNull(summaryUsgPeriodHighlightList)) {
                                summaryUsagePeriodHighliglightLisstSize = summaryUsgPeriodHighlightList.size();
                            }
                            for (CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight usgPeriodHighlightList : usagePeriodHighlightList) {
                                countList = 0;
                                for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight summaryPeriodHighlightList : summaryUsgPeriodHighlightList) {
                                    if (!usgPeriodHighlightList.getHighlightDateTime().equals(
                                            summaryPeriodHighlightList.getHighlightDateTime())) countList++;
                                }
                                if (countList == summaryUsagePeriodHighliglightLisstSize) {
                                    summaryUsgPeriodHighlightListSequence++;
                                    CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight highlightData = new CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight();
                                    highlightData.setSequence(new BigDecimal(summaryUsgPeriodHighlightListSequence));
                                    highlightData.setHighlightDateTime(usgPeriodHighlightList.getHighlightDateTime());
                                    highlightData.setHighlightType(usgPeriodHighlightList.getHighlightType());
                                    highlightData.setHighlightCondition(usgPeriodHighlightList.getHighlightCondition());
                                    highlightData.setHighlightDerivedCondition(usgPeriodHighlightList
                                            .getHighlightDerivedCondition());
                                    usagePeriodHighlightListTemp.add(highlightData);
                                }
                            }
                            for (CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight usgPeriodHighlightList : usagePeriodHighlightListTemp) {
                                CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight highlightData = new CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight();
                                highlightData.setSequence(usgPeriodHighlightList.getSequence());
                                highlightData.setHighlightDateTime(usgPeriodHighlightList.getHighlightDateTime());
                                highlightData.setHighlightType(usgPeriodHighlightList.getHighlightType());
                                highlightData.setHighlightCondition(usgPeriodHighlightList.getHighlightCondition());
                                highlightData.setHighlightDerivedCondition(usgPeriodHighlightList
                                        .getHighlightDerivedCondition());
                                summaryUsgPeriodHighlightList.add(highlightData);
                            }
                            summaryServiceQuantityData.setHighlightList(summaryUsgPeriodHighlightList);
                        }

                        if (clearHighlightList.isTrue()) {
                            summaryServiceQuantityData.setHighlightList(null);
                        }
                    }
                    
                    isUpdate = Bool.TRUE;

                    if (notNull(shouldExtractIntervalData) && shouldExtractIntervalData.isYes()) {
                        if (isNull(highestSPI) || highestSPI.compareTo(spi) < 0) {
                            highestSPI = spi;
                            summaryServiceQuantityData.setSecondsPerInterval(highestSPI);
                        }
                        extractCurve = Boolean.TRUE;
                    }
                }
            }
            if (isUpdate.isFalse()) {

                CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity summaryUsgPeriodServiceQuantityData = new CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity();
                summaryUsgPeriodServiceQuantityData.setSequence(new BigDecimal(sQSequence));
                summaryUsgPeriodServiceQuantityData.setUom(uomCode);
                summaryUsgPeriodServiceQuantityData.setTou(touCode);
                summaryUsgPeriodServiceQuantityData.setSqi(sqiCode);
                summaryUsgPeriodServiceQuantityData.setQuantity(quantity);
                if (notNull(shouldExtractIntervalData) && shouldExtractIntervalData.isYes()) {
                    summaryUsgPeriodServiceQuantityData.setSecondsPerInterval(spi);
                }

                UnitOfMeasureD1 uom = new UnitOfMeasureD1_Id(uomCode).getEntity();
                MeasuresPeakQuantityLookup measPeakQuantityFlag = uom.getMeasuresPeakQuantity();
                if (!notNull(spId) && spFound.isFalse() && notNull(usagePeriodHighlightList)
                        && usagePeriodHighlightList.size() > 0 && measPeakQuantityFlag.isMeasurePeakQuantity()) {
                    List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight> summaryHighlightList;
                    summaryHighlightList = new ArrayList<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight>();
                    for (CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight usgPeriodHighlightList : usagePeriodHighlightList) {
                        CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight highlightData = new CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight();
                        highlightData.setSequence(usgPeriodHighlightList.getSequence());
                        highlightData.setHighlightDateTime(usgPeriodHighlightList.getHighlightDateTime());
                        highlightData.setHighlightType(usgPeriodHighlightList.getHighlightType());
                        highlightData.setHighlightCondition(usgPeriodHighlightList.getHighlightCondition());
                        highlightData.setHighlightDerivedCondition(usgPeriodHighlightList
                                .getHighlightDerivedCondition());
                        summaryHighlightList.add(highlightData);
                    }
                    summaryUsgPeriodServiceQuantityData.setHighlightList(summaryHighlightList);
                }

                summarySQList.add(summaryUsgPeriodServiceQuantityData);
                sQSequence++;

                if (notNull(shouldExtractIntervalData) && shouldExtractIntervalData.isYes()) {
                    summaryUsgPeriodServiceQuantityData.setSecondsPerInterval(spi);
                    if (isNull(highestSPI) || highestSPI.compareTo(spi) < 0) {
                        highestSPI = spi;
                    }
                    extractCurve = Boolean.TRUE;
                }
                
                //Set new Usage Details
                if(notBlank(usgPeriodServiceQuantityData.getSpId())){
                	summaryUsgPeriodServiceQuantityData.setSpId(usgPeriodServiceQuantityData.getSpId());
                }
                if(notBlank(usgPeriodServiceQuantityData.getRouteId())){
                	summaryUsgPeriodServiceQuantityData.setRouteId(usgPeriodServiceQuantityData.getRouteId());
                }
                if(notBlank(usgPeriodServiceQuantityData.getSerialNumber())){
                	summaryUsgPeriodServiceQuantityData.setSerialNumber(usgPeriodServiceQuantityData.getSerialNumber());
                }
                if(notNull(usgPeriodServiceQuantityData.getCurrentYearConsumption())){
                	summaryUsgPeriodServiceQuantityData.setCurrentYearConsumption(usgPeriodServiceQuantityData.getCurrentYearConsumption());
                }
                if(notNull(usgPeriodServiceQuantityData.getLastYearConsumption())){
                	summaryUsgPeriodServiceQuantityData.setLastYearConsumption(usgPeriodServiceQuantityData.getLastYearConsumption());
                }
                if(notBlank(usgPeriodServiceQuantityData.getMeterId())){
                	summaryUsgPeriodServiceQuantityData.setMeterId(usgPeriodServiceQuantityData.getMeterId());
                }
                if(notBlank(usgPeriodServiceQuantityData.getMeterBrand())){
                	summaryUsgPeriodServiceQuantityData.setMeterBrand(usgPeriodServiceQuantityData.getMeterBrand());
                }
                if(notBlank(usgPeriodServiceQuantityData.getMeterType())){
                	summaryUsgPeriodServiceQuantityData.setMeterType(usgPeriodServiceQuantityData.getMeterType());
                }
                if(notNull(usgPeriodServiceQuantityData.getLastIndex())){
                	summaryUsgPeriodServiceQuantityData.setLastIndex(usgPeriodServiceQuantityData.getLastIndex());
                }
                if(notNull(usgPeriodServiceQuantityData.getFirstIndex())){
                	summaryUsgPeriodServiceQuantityData.setFirstIndex(usgPeriodServiceQuantityData.getFirstIndex());
                }
                if(notNull(usgPeriodServiceQuantityData.getMultiplier())){
                	summaryUsgPeriodServiceQuantityData.setMultiplier(usgPeriodServiceQuantityData.getMultiplier());
                }
                if(notNull(usgPeriodServiceQuantityData.getConsumption())){
                	summaryUsgPeriodServiceQuantityData.setConsumption(usgPeriodServiceQuantityData.getConsumption());
                }
                if(notNull(usgPeriodServiceQuantityData.getTransformerLoss())){
                	summaryUsgPeriodServiceQuantityData.setTransformerLoss(usgPeriodServiceQuantityData.getTransformerLoss());
                }
                if(notNull(usgPeriodServiceQuantityData.getDailyAverageUsage())){
                	summaryUsgPeriodServiceQuantityData.setDailyAverageUsage(usgPeriodServiceQuantityData.getDailyAverageUsage());
                }
                if(notNull(usgPeriodServiceQuantityData.getLastReadingDate())){
                	summaryUsgPeriodServiceQuantityData.setLastReadingDate(usgPeriodServiceQuantityData.getLastReadingDate());
                }
                if(notNull(usgPeriodServiceQuantityData.getFirstReadingDate())){
                	summaryUsgPeriodServiceQuantityData.setFirstReadingDate(usgPeriodServiceQuantityData.getFirstReadingDate());
                }
                if(notNull(usgPeriodServiceQuantityData.getDemandIndex())){
                	summaryUsgPeriodServiceQuantityData.setDemandIndex(usgPeriodServiceQuantityData.getDemandIndex());
                }
                if(notNull(usgPeriodServiceQuantityData.getDemandMultiplier())){
                	summaryUsgPeriodServiceQuantityData.setDemandMultiplier(usgPeriodServiceQuantityData.getDemandMultiplier());
                }
                if(notNull(usgPeriodServiceQuantityData.getCurrentTransformerRatio())){
                	summaryUsgPeriodServiceQuantityData.setCurrentTransformerRatio(usgPeriodServiceQuantityData.getCurrentTransformerRatio());
                }
                if(notNull(usgPeriodServiceQuantityData.getVoltageTransformerRatio())){
                	summaryUsgPeriodServiceQuantityData.setVoltageTransformerRatio(usgPeriodServiceQuantityData.getVoltageTransformerRatio());
                }
                if(notNull(usgPeriodServiceQuantityData.getIndexDifference())){
                	summaryUsgPeriodServiceQuantityData.setIndexDifference(usgPeriodServiceQuantityData.getIndexDifference());
                }
                if(notNull(usgPeriodServiceQuantityData.getDemandConsumption())){
                	summaryUsgPeriodServiceQuantityData.setDemandConsumption(usgPeriodServiceQuantityData.getDemandConsumption());
                }
                if(notNull(usgPeriodServiceQuantityData.getNetGeneration())){
                	summaryUsgPeriodServiceQuantityData.setNetGeneration(usgPeriodServiceQuantityData.getNetGeneration());
                }
                if(notNull(usgPeriodServiceQuantityData.getGenerationDemand())){
                	summaryUsgPeriodServiceQuantityData.setGenerationDemand(usgPeriodServiceQuantityData.getGenerationDemand());
                }
                if(notNull(usgPeriodServiceQuantityData.getInductiveRatio())){
                	summaryUsgPeriodServiceQuantityData.setInductiveRatio(usgPeriodServiceQuantityData.getInductiveRatio());
                }
                if(notNull(usgPeriodServiceQuantityData.getCapacitiveRatio())){
                	summaryUsgPeriodServiceQuantityData.setCapacitiveRatio(usgPeriodServiceQuantityData.getCapacitiveRatio());
                }
            }
        }
    }

    /**
     *  This method will insert the Summary Usage Period SQs Interval List.
     * 
     * @param usgPeriodServiceQuantityList
     * @param summarySQList
     * @param targetSPI
     * @param startDateTime
     * @param endDateTime
     */
    private void prepareSummaryUsagePeriodSQIntervals(
            List<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity> usgPeriodServiceQuantityList,
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity> summarySQList,
            TimeInterval targetSPI, DateTime startDateTime, DateTime endDateTime, UsageTypeD2Lookup usageType) {

        DateTime roundedStartDateTime = null;
        DateTime roundedEndDateTime = null;
        TimeInterval combinedSPI = targetSPI;
        if (isNull(combinedSPI)) {
            combinedSPI = highestSPI;
        }

        IntervalPeriodHelper helper = IntervalPeriodHelper.Factory.newInstance();
        //Convert the start date to standard, calculate round date times then convert back to wall
        TimeZone installTimeZone = (new TimeZone_Id(InstallationHelper.getTimeZoneCode()).getEntity());
        DateTime standardStartDateTime = shiftLegalDateTimeToStandard.shiftToStandard(installTimeZone, installTimeZone,
                startDateTime, Bool.TRUE);
        DateTime standardEndDateTime = shiftLegalDateTimeToStandard.shiftToStandard(installTimeZone, installTimeZone,
                endDateTime, Bool.TRUE);
        roundedStartDateTime = helper.roundDateTime(standardStartDateTime, combinedSPI,
                RoundingMethodD1Lookup.constants.ROUND_DOWN);
        roundedEndDateTime = helper.roundDateTime(standardEndDateTime, combinedSPI,
                RoundingMethodD1Lookup.constants.ROUND_DOWN);
        roundedStartDateTime = shiftStandardDateTimeToLegal.shiftToLegal(installTimeZone, installTimeZone,
                roundedStartDateTime);
        roundedEndDateTime = shiftStandardDateTimeToLegal.shiftToLegal(installTimeZone, installTimeZone,
                roundedEndDateTime);
        
        retrieveUTExportConfigBOInstance();
        if (notNull(utExportConfigObjectInstance)) {
        	conditionCodeOption = (ConditionCodeOptionLookup) utExportConfigObjectInstance
                        .getLookup(CONDITION_CODE_OPTION);
        }

        for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity summaryServiceQuantityData : summarySQList) {
            String summaryUomCode = summaryServiceQuantityData.getUom();
            String summaryTouCode = summaryServiceQuantityData.getTou();
            String summarySqiCode = summaryServiceQuantityData.getSqi();
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Interval> summaryIntervalList = summaryServiceQuantityData
                    .getIntervalList();

            if (isNull(summaryIntervalList)) {
                summaryIntervalList = new ArrayList<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Interval>();
            }

            for (CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity usgPeriodServiceQuantityData : usgPeriodServiceQuantityList) {
                String uomCode = usgPeriodServiceQuantityData.getUom();
                String touCode = usgPeriodServiceQuantityData.getTou();
                String sqiCode = usgPeriodServiceQuantityData.getSqi();
                TimeInterval sqSpi = usgPeriodServiceQuantityData.getSecondsPerInterval();
                ExtractIntervalDataLookup shouldExtractIntervalData = usgPeriodServiceQuantityData
                        .getShouldExtractInterval();

                if (notNull(shouldExtractIntervalData) && shouldExtractIntervalData.isYes()
                        && checkIfCodeExist(uomCode, summaryUomCode) && checkIfCodeExist(sqiCode, summarySqiCode)
                        && checkIfCodeExist(touCode, summaryTouCode)) {

                    if (isNull(usgPeriodServiceQuantityData.getIntervalList())
                            || usgPeriodServiceQuantityData.getIntervalList().size() == 0) {
                        continue;
                    }

                    List<ConsumptionAmountAndConditionData> consumptionAmountAndConditionList = new ArrayList<ConsumptionAmountAndConditionData>();
                    prepareConumptionAmountAndConditionList(usgPeriodServiceQuantityData.getIntervalList(),
                            consumptionAmountAndConditionList, sqSpi, roundedStartDateTime, roundedEndDateTime);

                    // Axis Conversion
                    if (notNull(sqSpi) && notNull(combinedSPI) && sqSpi.compareTo(combinedSPI) != 0) {
                        UnitOfMeasureD1 uomCodeEntity = new UnitOfMeasureD1_Id(uomCode.trim()).getEntity();
                        consumptionAmountAndConditionList = doAxisConversion(consumptionAmountAndConditionList,
                                roundedStartDateTime, sqSpi, combinedSPI, uomCodeEntity);
                    }

                    //Combine Usage Period SQ List Intervals with  Summary SQ List Intervals.
                    combineCurves(summaryIntervalList, consumptionAmountAndConditionList, summaryUomCode,
                            roundedStartDateTime, combinedSPI);

                    summaryServiceQuantityData.setIntervalList(summaryIntervalList);
                }
            }

            if (notNull(summaryServiceQuantityData.getSecondsPerInterval())) {
                //Updating the Summary Usage Period SPI
                if (isNull(usageType) || usageType.isInterval() || usageType.isMixedIntervalAndScalar()) {
                    summaryServiceQuantityData.setSecondsPerInterval(combinedSPI);
                } else {
                    //Scalar Summary Usage Periods secondsPerInterval = 0
                    summaryServiceQuantityData.setSecondsPerInterval(TimeInterval.ZERO);
                }
            }
        }
    }

    /**
     * This method will combine Usage Period SQ List Intervals with Summary SQ List Intervals.
     *  
     * @param summaryIntervalList
     * @param consumptionAmountAndConditionList
     * @param uom
     * @param startDateTime
     * @param spi
     */
    private void combineCurves(
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Interval> summaryIntervalList,
            List<ConsumptionAmountAndConditionData> consumptionAmountAndConditionList, String uom,
            DateTime startDateTime, TimeInterval spi) {

        DateTime standardStartDttm = shiftLegalDateTimeToStandard.shiftToStandard(usageTimeZone, usageTimeZone,
                startDateTime, Bool.TRUE);
        // First time Summary SQ List will be empty
        if (summaryIntervalList.size() == 0) {
            if (notNull(consumptionAmountAndConditionList) && consumptionAmountAndConditionList.size() > 0) {
                DateTime currentDateTime = standardStartDttm.add(spi);
                int seqNo = 1;

                for (ConsumptionAmountAndConditionData consumptionAmountAndConditionData : consumptionAmountAndConditionList) {
                    CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Interval summaryIntervalData = new CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Interval();
                    summaryIntervalData.setSequence(BigDecimal.valueOf(seqNo));
                    summaryIntervalData.setDateTime(usageTimeZone.applySeasonalShift(currentDateTime));
                    summaryIntervalData.setQuantity(consumptionAmountAndConditionData.getConsumptionAmount());
                    if(isNull(conditionCodeOption) || conditionCodeOption.isBlankLookupValue() 
                    		|| conditionCodeOption.isLowestQuality()){
                    	summaryIntervalData.setCondition(new ExtendedLookupValue_Id(conditionLookupBO,
                            consumptionAmountAndConditionData.getCondition()));
                    }else{
                    	summaryIntervalData.setCondition(new ExtendedLookupValue_Id(conditionLookupBO, DERIVED));
                    }

                    summaryIntervalList.add(summaryIntervalData);
                    currentDateTime = currentDateTime.add(spi);
                    ++seqNo;
                }
            }
        } else {
            if (isNull(consumptionAmountAndConditionList)
                    || consumptionAmountAndConditionList.size() != summaryIntervalList.size()) {
                addError(MessageRepository.intervalCurvesNotAlignedProperly());
            }
            //Updating the Summary SQ List's Quantity & Condition values.
            int index = 0;
            for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Interval summaryIntervalListData : summaryIntervalList) {
                ConsumptionAmountAndConditionData consumptionAmountAndConditionData = consumptionAmountAndConditionList
                        .get(index);
                BigDecimal aggregatedQuantity = determineQuantity(uom, summaryIntervalListData.getQuantity(),
                        consumptionAmountAndConditionData.getConsumptionAmount());
                summaryIntervalListData.setQuantity(aggregatedQuantity);

                String currentCondition = consumptionAmountAndConditionData.getCondition();
                String aggrCondition = null;
                if(notNull(summaryIntervalListData.getCondition())){
                	aggrCondition = summaryIntervalListData.getCondition().getValue();
                }
                if(isNull(conditionCodeOption) || conditionCodeOption.isBlankLookupValue() 
                		|| conditionCodeOption.isLowestQuality()){
                	if(notBlank(currentCondition) && notBlank(aggrCondition)){
	                	BigDecimal currentCon = new BigDecimal(currentCondition);
	                	BigDecimal aggrCon = new BigDecimal(aggrCondition);
	                	if(aggrCon.compareTo(currentCon) > 0){
	                		summaryIntervalListData.setCondition(new ExtendedLookupValue_Id(conditionLookupBO,
	                                currentCondition));
	                	}
                	}else if(notBlank(currentCondition)){
                		summaryIntervalListData.setCondition(new ExtendedLookupValue_Id(conditionLookupBO,
                                currentCondition));
                	}
                }else{
	                // if current interval has no reading, do not update condition
	                if (!NO_READ_SYSTEM.equals(currentCondition)) {
	                    // if interval has already been aggregated with a reading, set condition to derived, otherwise set current condition
	                    if (!NO_READ_SYSTEM.equals(aggrCondition)) {
	                        summaryIntervalListData.setCondition(new ExtendedLookupValue_Id(conditionLookupBO, DERIVED));
	                    } else {
	                        summaryIntervalListData.setCondition(new ExtendedLookupValue_Id(conditionLookupBO,
	                                currentCondition));
	                    }
	                }
                }

                ++index;
            }
        }
    }

    /**
     *   This method will creates a Consumption Amount and Condition List from SQ Interval List and missing intervals in SQ Interval List.
     *   
     *   @param intervalDataList
     *   @param consumptionAmountAndConditionList
     *   @param spi
     *   @param startDttm
     *   @param endDttm
     */
    private void prepareConumptionAmountAndConditionList(
            List<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Interval> intervalDataList,
            List<ConsumptionAmountAndConditionData> consumptionAmountAndConditionList, TimeInterval spi,
            DateTime startDttm, DateTime endDttm) {
        ConsumptionAmountAndConditionData consumptionAmountAndConditionData = null;
        DateTime currentDttm = null;
        DateTime currentLegalDttm = null;
        DateTime standardStartDttm = shiftLegalDateTimeToStandard.shiftToStandard(usageTimeZone, usageTimeZone,
                startDttm, Bool.TRUE);

        Iterator<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Interval> intervalDataIter = intervalDataList
                .iterator();
        CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Interval intervalData = null;
        if (intervalDataIter.hasNext()) intervalData = intervalDataIter.next();

        for (currentDttm = standardStartDttm.add(spi); currentDttm.compareTo(endDttm) <= 0; currentDttm = currentDttm
                .add(spi)) {
            currentLegalDttm = usageTimeZone.applySeasonalShift(currentDttm);
            if (currentLegalDttm.compareTo(startDttm) <= 0 || currentLegalDttm.compareTo(endDttm) > 0) {
                continue;
            }
            if (isNull(intervalData) || intervalData.getDateTime().compareTo(currentLegalDttm) != 0) {
                consumptionAmountAndConditionData = prepareConsumptionAmountAndConditionData(BigDecimal.ZERO,null);
            } else {
            	ExtendedLookupValue_Id intervalDataCondition = intervalData.getCondition();
            	String condition = null;
            	if(notNull(intervalDataCondition)){
            		condition = intervalDataCondition.getValue();
            	}
                consumptionAmountAndConditionData = prepareConsumptionAmountAndConditionData(
                        intervalData.getQuantity(), condition);
                if (intervalDataIter.hasNext()) intervalData = intervalDataIter.next();
            }
            consumptionAmountAndConditionList.add(consumptionAmountAndConditionData);
        }
    }

    /**
     *  This method will returns an object of ConsumptionAmountAndConditionData by setting the input Quantity & Condition values.
     *  
     *  @param quantity
     *  @param condition
     *  @return ConsumptionAmountAndConditionData
     */
    private ConsumptionAmountAndConditionData prepareConsumptionAmountAndConditionData(BigDecimal quantity,
            String condition) {
        ConsumptionAmountAndConditionData consumptionAmountAndConditionData = ConsumptionAmountAndConditionData.Factory
                .newInstance();
        consumptionAmountAndConditionData.setConsumptionAmount(quantity);
        consumptionAmountAndConditionData.setCondition(condition);
        return consumptionAmountAndConditionData;
    }

    /**
     *   This method will performs Axis conversion.
     *   
     *   @param consumptionAmountAndConditionList
     *   @param startDttm
     *   @param sourceSpi
     *   @param targetSpi
     *   @param sourceUOM
     *   @return  List<ConsumptionAmountAndConditionData>  
     */
    private List<ConsumptionAmountAndConditionData> doAxisConversion(
            List<ConsumptionAmountAndConditionData> consumptionAmountAndConditionList, DateTime startDttm,
            TimeInterval sourceSpi, TimeInterval targetSpi, UnitOfMeasureD1 sourceUOM) {

        if (isNull(consumptionAmountAndConditionList) || consumptionAmountAndConditionList.size() == 0) {
            return consumptionAmountAndConditionList;
        }

        AxisConversionInputData axisConversionInputData = AxisConversionInputData.Factory.newInstance();
        axisConversionInputData.setSourceSpi(sourceSpi);
        axisConversionInputData.setTargetSpi(targetSpi);
        if (notNull(sourceUOM)) {
            axisConversionInputData.setSourceUom(sourceUOM);
        }
        axisConversionInputData.setTargetUom(sourceUOM);
        axisConversionInputData.setStartDateTime(startDttm);
        axisConversionInputData.setConsumptionList(consumptionAmountAndConditionList);
        
        if(isNull(conditionCodeOption) || conditionCodeOption.isBlankLookupValue() 
        		|| conditionCodeOption.isLowestQuality()){
        	axisConversionInputData.setMeasurementConditionProcessingMethod(MeasurementConditionProcessingMethodLookup.constants.LOWEST_QUALITY);
        }

        AxisConversionOutputData axisConversionOutputData = axisConversionInputData.getSourceUom()
                .invokeAxisConversion(axisConversionInputData);
        if (isNull(axisConversionOutputData) || (axisConversionOutputData.getConsumptionList().size() == 0)) {
            return consumptionAmountAndConditionList;
        }

        return axisConversionOutputData.getConsumptionList();

    }

    /**
     * Check if record for SPID/UOM/TOU/SQI combination is exist in Summary Usage Period Group
     * 
     * @param code
     * @param summaryCode
     * @return boolean
     */
    private boolean checkIfCodeExist(String code, String summaryCode) {
        if (isBlankOrNull(code) && isBlankOrNull(summaryCode)) {
            return true;
        }
        if (notBlank(code) && notBlank(summaryCode)) {
            if (code.equals(summaryCode)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkIfDateCodeExist(DateTime dateTime, DateTime summaryDateTime) {
        if (isNull(dateTime) && isNull(summaryDateTime)) {
            return true;
        }
        if (notNull(dateTime) && notNull(summaryDateTime)) {
            if (dateTime.compareTo(summaryDateTime) == 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * This method will determine the Quantity based on the Measures Peak Quantity Lookup of the UOM (This is for Highlight  list purpose)
     * 
     * @param summaryUomCode
     * @param summaryQty
     * @param qty
     * @return BigDecimal
     */
    private BigDecimal determineQuantity(String summaryUomCode, BigDecimal summaryQty, BigDecimal qty) {
        BigDecimal quantity = qty;
        UnitOfMeasureD1 uom = new UnitOfMeasureD1_Id(summaryUomCode).getEntity();
        MeasuresPeakQuantityLookup measPeakQuantityFlag = uom.getMeasuresPeakQuantity();

        if (measPeakQuantityFlag.isMeasurePeakQuantity()) {
            determineHighlightUpdateAction(summaryQty, qty);
            if (highlightTypeOnlyMin.isTrue()) {
                quantity = summaryQty.min(quantity);
            } else {
                quantity = summaryQty.max(quantity);
            }
        }
        if (measPeakQuantityFlag.isDoesNotMeasurePeakQuantity()) {
            quantity = quantity.add(summaryQty);
            clearHighlightList = Bool.TRUE;
        }
        return quantity;
    }

    /**
     *  This method will return SP Identifier for the given SPId
     *  
     *  @param spId
     *  @return String
     */
    private String getSPIdentifier(String spId, ServicePointIdentifierTypeLookup spIdentifierTypeLookup) {
        ServicePointIdentifier_Id spIdentifierId = new ServicePointIdentifier_Id(new ServicePointD1_Id(spId),
                spIdentifierTypeLookup);
        ServicePointIdentifier spIdentifier = spIdentifierId.getEntity();
        if (isNull(spIdentifier)) {
            return null;
        }
        return spIdentifier.getIdValue();
    }

    /**
     *  This method will fetch the size of the current highlight list and new highlight lists.Accordingly identifies if highlight types across both the lists is only Max/Only Min/Mixed.
     *  
     */
    private void checkHighlightTypes(
            List<CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight> usgPeriodHighlightDateTimeList,
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight> highlightDateTimeList) {

        usgPeriodMaxHighlightTypeCount = 0;
        usgPeriodMinHighlightTypeCount = 0;
        summaryUsgPeriodMaxHighlightTypeCount = 0;
        summaryUsgPeriodMinHighlightTypeCount = 0;
        highlightListSize = 0;
        usgPeriodHighlightListSize = 0;
        highlightTypeOnlyMin = Bool.FALSE;
        highlightTypeOnlyMax = Bool.FALSE;
        highlightTypeMixed = Bool.FALSE;

        if (notNull(highlightDateTimeList)) highlightListSize = highlightDateTimeList.size();
        if (notNull(usgPeriodHighlightDateTimeList))
            usgPeriodHighlightListSize = usgPeriodHighlightDateTimeList.size();

        if (checkForMixedHighlightTypes.isTrue()) {
            if (notNull(usgPeriodHighlightDateTimeList) && usgPeriodHighlightListSize > 0
                    && notNull(highlightDateTimeList) && highlightListSize > 0) {
                for (CmUsagePeriodsInputData.UsagePeriod.ServiceQuantity.Highlight usgPeriodHighlightData : usgPeriodHighlightDateTimeList) {
                    if (usgPeriodHighlightData.getHighlightType().equals(maxHighlightType))
                        usgPeriodMaxHighlightTypeCount++;
                    if (usgPeriodHighlightData.getHighlightType().equals(minHighlightType))
                        usgPeriodMinHighlightTypeCount++;
                }
                for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight summaryUsgHighlightList : highlightDateTimeList) {
                    if (summaryUsgHighlightList.getHighlightType().equals(maxHighlightType))
                        summaryUsgPeriodMaxHighlightTypeCount++;
                    if (summaryUsgHighlightList.getHighlightType().equals(minHighlightType))
                        summaryUsgPeriodMinHighlightTypeCount++;
                }
                if (usgPeriodMaxHighlightTypeCount == usgPeriodHighlightListSize
                        && summaryUsgPeriodMaxHighlightTypeCount == highlightListSize) {
                    highlightTypeOnlyMax = Bool.TRUE;
                } else if (usgPeriodMinHighlightTypeCount == usgPeriodHighlightListSize
                        && summaryUsgPeriodMinHighlightTypeCount == highlightListSize) {
                    highlightTypeOnlyMin = Bool.TRUE;
                } else {
                    highlightTypeMixed = Bool.TRUE;
                }
            }
        }
    }

    private void determineHighlightUpdateAction(BigDecimal summaryQty, BigDecimal qty) {
        BigDecimal quantity = qty;
        updateHighlightList = Bool.FALSE;
        clearHighlightList = Bool.FALSE;
        modifyHighlightList = Bool.FALSE;

        if (usgPeriodHighlightListSize > 0 && highlightListSize > 0) {
            //If all the highlight entries contain Max highlight type then set the maximum quantity                   
            if (highlightTypeOnlyMax.isTrue()) {
                if (quantity.doubleValue() > summaryQty.doubleValue()) {
                    updateHighlightList = Bool.TRUE;
                }
                if (summaryQty.doubleValue() == quantity.doubleValue()) {
                    modifyHighlightList = Bool.TRUE;
                }
            }
            //if all the highlight entries contain Min highlight type then set the minimum quantity
            else if (highlightTypeOnlyMin.isTrue()) {
                if (quantity.doubleValue() < summaryQty.doubleValue()) {
                    updateHighlightList = Bool.TRUE;
                }
                if (summaryQty.doubleValue() == quantity.doubleValue()) {
                    modifyHighlightList = Bool.TRUE;
                }
            }
            //if the highlight entries contain a mix of Max and Min highlight types then pick the maximum quantity and do not update the highlight list
            else if (highlightTypeMixed.isTrue()) {
                clearHighlightList = Bool.TRUE;
            } else {
                if (quantity.doubleValue() > summaryQty.doubleValue()) {
                    updateHighlightList = Bool.TRUE;
                }
                if (summaryQty.doubleValue() == quantity.doubleValue()) {
                    modifyHighlightList = Bool.TRUE;
                }
            }
        }
        // If previous entry is does not have highight date/time list then if the new quantity is greater than old then update the highlight list
        else if (highlightListSize == 0 && usgPeriodHighlightListSize > 0) {
            if ((quantity.doubleValue() > summaryQty.doubleValue())
                    || (summaryQty.doubleValue() == quantity.doubleValue())) {
                updateHighlightList = Bool.TRUE;
            }
        }
        // This covers the cases : When both new and old doesnt have highlight list and old one has and new one doesnt have a highlight list.
        else {
            if (quantity.doubleValue() > summaryQty.doubleValue()) {
                clearHighlightList = Bool.TRUE;
            }
        }
    }

    /**
     *  This method will mix the Interval Summary Usage Period with the overlapped Scalar Summary Usage Period.
     * 
     * @param summaryUsagePeriodList
     * @return List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> 
     */
    private List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> retrieveMixedIntervalAndScalarSummaryUsagePeriods(
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> summaryUsagePeriodList) {
        List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> mixedSummaryUsagePeriodList = new ArrayList<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod>();
        List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> intervalSummaryUsagePeriodList = new ArrayList<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod>();
        List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> scalarSummaryUsagePeriodList = new ArrayList<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod>();

        if (isNull(summaryUsagePeriodList) || summaryUsagePeriodList.size() == 0) {
            return summaryUsagePeriodList;
        }
        // Seggregate Interval and Scalar Summary Usage Periods.
        for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod summaryUsagePeriodListData : summaryUsagePeriodList) {
            if (notNull(summaryUsagePeriodListData.getUsageType())
                    && summaryUsagePeriodListData.getUsageType().isInterval()) {
                intervalSummaryUsagePeriodList.add(summaryUsagePeriodListData);
            } else if (notNull(summaryUsagePeriodListData.getUsageType())
                    && summaryUsagePeriodListData.getUsageType().isScalar()) {
                scalarSummaryUsagePeriodList.add(summaryUsagePeriodListData);
            }
        }
        // Merge overlap interval summary usage periods
        if (intervalSummaryUsagePeriodList.size() > 0) {
            intervalSummaryUsagePeriodList = mergeOverlapSummaryUsagePeriods(intervalSummaryUsagePeriodList);
        }
        // Merge overlap scalar summary usage periods
        if (scalarSummaryUsagePeriodList.size() > 0) {
            scalarSummaryUsagePeriodList = mergeOverlapSummaryUsagePeriods(scalarSummaryUsagePeriodList);
        }
        // Return the interval summary usage period list if the scalar summary usage period list is empty
        if (intervalSummaryUsagePeriodList.size() > 0 && scalarSummaryUsagePeriodList.size() == 0) {
            return intervalSummaryUsagePeriodList;
        }
        // Return the scalar summary usage period list if the interval summary usage period list is empty
        if (scalarSummaryUsagePeriodList.size() > 0 && intervalSummaryUsagePeriodList.size() == 0) {
            return scalarSummaryUsagePeriodList;
        }

        // Process for mixed interval and scalar summary usage periods
        
        boolean isOverlapSummaryUsagePeriodExists = checkForOverlapSummaryUsagePeriods(intervalSummaryUsagePeriodList,
                scalarSummaryUsagePeriodList);
        if (!isOverlapSummaryUsagePeriodExists) {
            return summaryUsagePeriodList;
        }
        mixedSummaryUsagePeriodList = processIntervalUsagePeriodsFirst(intervalSummaryUsagePeriodList,
                scalarSummaryUsagePeriodList);

        return mixedSummaryUsagePeriodList;
    }

    /**
     *  This method will mix the overlapped Scalar Summary Usage Period with the Interval Summary Usage Period.
     * 
     * @param intervalSummaryUsagePeriodList
     * @param scalarSummaryUsagePeriodList     
     * @ return List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod>
     */
    private List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> processIntervalUsagePeriodsFirst(
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> intervalSummaryUsagePeriodList,
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> scalarSummaryUsagePeriodList) {
        List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> mixedSummaryUsagePeriodList = new ArrayList<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod>();
        long currDiffInSeconds = 0;
        long prevDiffInSeconds = 0;
        int intervalUsgPeriodIndex = 0;
        int overlapIndex = -1;

        //Move Interval Summary Usage Period List data  to Output Summary Usage Period List.
        for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod intervalSummaryUsagePeriod : intervalSummaryUsagePeriodList) {
            intervalSummaryUsagePeriod.setSequence(new BigDecimal(mixedSummaryUsagePeriodList.size() + 1));
            mixedSummaryUsagePeriodList.add(intervalSummaryUsagePeriod);
        }
        for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod scalarSummaryUsagePeriod : scalarSummaryUsagePeriodList) {
            currDiffInSeconds = 0;
            prevDiffInSeconds = 0;
            intervalUsgPeriodIndex = 0;
            overlapIndex = 0;

            for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod intervalSummaryUsagePeriod : intervalSummaryUsagePeriodList) {
                if ((scalarSummaryUsagePeriod.getStartDateTime().isSameOrAfter(intervalSummaryUsagePeriod
                        .getStartDateTime()))
                        && (scalarSummaryUsagePeriod.getEndDateTime().isSameOrBefore(intervalSummaryUsagePeriod
                                .getEndDateTime()))) {
                    overlapIndex = intervalUsgPeriodIndex;
                    break;
                }
                if (isOverlapSummaryUsagePeriod(intervalSummaryUsagePeriod, scalarSummaryUsagePeriod)) {
                    if (overlapIndex == -1) {
                        currDiffInSeconds = (intervalSummaryUsagePeriod.getEndDateTime()
                                .difference(scalarSummaryUsagePeriod.getStartDateTime())).getAsSeconds();
                        prevDiffInSeconds = currDiffInSeconds;
                        overlapIndex = intervalUsgPeriodIndex;
                    } else {
                        currDiffInSeconds = (scalarSummaryUsagePeriod.getEndDateTime()
                                .difference(intervalSummaryUsagePeriod.getStartDateTime())).getAsSeconds();
                        if (currDiffInSeconds == prevDiffInSeconds) {
                            // If the current usage period overlaps equally with more than 1 summary usage period (Interval Usage Periods)
                            overlapIndex = -1;
                            break;
                        } else if (currDiffInSeconds > prevDiffInSeconds) {
                            prevDiffInSeconds = currDiffInSeconds;
                            overlapIndex = intervalUsgPeriodIndex;
                        }
                    }
                }
                intervalUsgPeriodIndex++;
            }
            if (overlapIndex > -1) {
                CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod overlappedSmryUsgPeriod = mixedSummaryUsagePeriodList
                        .get(overlapIndex);
                insertOrUpdateSummaryUsagePeriodList(overlappedSmryUsgPeriod, scalarSummaryUsagePeriod, true);
                mixedSummaryUsagePeriodList.set(overlapIndex, overlappedSmryUsgPeriod);
            } else {
                scalarSummaryUsagePeriod.setSequence(new BigDecimal(mixedSummaryUsagePeriodList.size() + 1));
                mixedSummaryUsagePeriodList.add(scalarSummaryUsagePeriod);
            }
        }

        return mixedSummaryUsagePeriodList;
    }

    /**
     *  This method will update the Summary Usage Period SQs / SP SQs List if a matching Scalar Usage Period found for "UOM/TOU/SQI" and "SP Id/UOM/TOU/SQI" combinations,  
     *  otherwise insert a new entry into Summary Usage Period SQs/sPSQs List.
     *  
     * @param mixedSummaryUsagePeriod
     * @param scalarSummaryUsagePeriod
     */
    private void insertOrUpdateSummaryUsagePeriodList(
            CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod mixedSummaryUsagePeriod,
            CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod scalarSummaryUsagePeriod, boolean isMixedIntervalScalar) {
    	if (isMixedIntervalScalar) {
        // Update Usage Type to D2IS
        mixedSummaryUsagePeriod.setUsageType(UsageTypeD2Lookup.constants.MIXED_INTERVAL_AND_SCALAR);
    	}

        // Mix SQsList based on UOM/SQI/TOU
        List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity> scalarSummaryUsagePeriodSQsList = scalarSummaryUsagePeriod
                .getServiceQuantityList();
        List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity> mixedSummaryUsagePeriodSQsList = mixedSummaryUsagePeriod
                .getServiceQuantityList();
        for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity scalarSQs : scalarSummaryUsagePeriodSQsList) {
            boolean isUpdate = false;
            for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity mixedSQs : mixedSummaryUsagePeriodSQsList) {
                if (checkIfCodeExist(scalarSQs.getUom(), mixedSQs.getUom())
                        && checkIfCodeExist(scalarSQs.getTou(), mixedSQs.getTou())
                        && checkIfCodeExist(scalarSQs.getSqi(), mixedSQs.getSqi())) {
                    UnitOfMeasureD1 uom = new UnitOfMeasureD1_Id(mixedSQs.getUom()).getEntity();
                    BigDecimal quantity = (uom.getMeasuresPeakQuantity().isMeasurePeakQuantity()) ? mixedSQs
                            .getQuantity().max(scalarSQs.getQuantity()) : mixedSQs.getQuantity().add(
                            scalarSQs.getQuantity());

                    mixedSQs.setQuantity(quantity);

                    isUpdate = true;
                    break;
                }
            }
            if (!isUpdate) {
                scalarSQs.setSequence(new BigDecimal(mixedSummaryUsagePeriodSQsList.size() + 1));
                mixedSummaryUsagePeriodSQsList.add(scalarSQs);
            }
        }
        mixedSummaryUsagePeriod.setServiceQuantityList(mixedSummaryUsagePeriodSQsList);

        //Mix SpSQsList based on SPID/UOM/SQI/TOU
        List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity> scalarSummaryUsagePeriodSpSQsList = scalarSummaryUsagePeriod
                .getSPServiceQuantityList();
        List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity> mixedSummaryUsagePeriodSpSQsList = mixedSummaryUsagePeriod
                .getSPServiceQuantityList();
        for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity scalarSpSQs : scalarSummaryUsagePeriodSpSQsList) {
            boolean isUpdate = false;
            int spSQsSeqNo = 1;
            for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity mixedSpSQs : mixedSummaryUsagePeriodSpSQsList) {
                if (checkIfCodeExist(scalarSpSQs.getSpId(), mixedSpSQs.getSpId())) {
                    spSQsSeqNo++;
                }

                if (checkIfCodeExist(scalarSpSQs.getSpId(), mixedSpSQs.getSpId())
                        && checkIfCodeExist(scalarSpSQs.getUom(), mixedSpSQs.getUom())
                        && checkIfCodeExist(scalarSpSQs.getTou(), mixedSpSQs.getTou())
                        && checkIfCodeExist(scalarSpSQs.getSqi(), mixedSpSQs.getSqi())) {
                    boolean updatedHighlightDttmList = false;
                    boolean modifyHighlightDttmList = false;
                    boolean clearHighlightDttmList = false;
                    BigDecimal quantity = BigDecimal.ZERO;

                    UnitOfMeasureD1 uom = new UnitOfMeasureD1_Id(mixedSpSQs.getUom()).getEntity();
                    if (uom.getMeasuresPeakQuantity().isMeasurePeakQuantity()) {
                        int mixedHighlightListSize = 0;
                        int scalarHighlightListSize = 0;
                        if (notNull(mixedSpSQs.getHighlightList())) {
                            mixedHighlightListSize = mixedSpSQs.getHighlightList().size();
                        }
                        if (notNull(scalarSpSQs.getHighlightList())) {
                            scalarHighlightListSize = scalarSpSQs.getHighlightList().size();
                        }
                        if (mixedHighlightListSize > 0 && scalarHighlightListSize > 0) {
                            if (scalarSpSQs.getQuantity().doubleValue() > mixedSpSQs.getQuantity().doubleValue()) {
                                updatedHighlightDttmList = true;
                            }
                            if (scalarSpSQs.getQuantity().doubleValue() == mixedSpSQs.getQuantity().doubleValue()) {
                                modifyHighlightDttmList = true;
                            }
                        } else if (mixedHighlightListSize == 0 && scalarHighlightListSize > 0) {
                            if ((scalarSpSQs.getQuantity().doubleValue() > mixedSpSQs.getQuantity().doubleValue())
                                    || (scalarSpSQs.getQuantity().doubleValue() == mixedSpSQs.getQuantity()
                                            .doubleValue())) {
                                updatedHighlightDttmList = true;
                            }
                        } else if (scalarSpSQs.getQuantity().doubleValue() > mixedSpSQs.getQuantity().doubleValue()) {
                            clearHighlightDttmList = true;
                        }

                        quantity = mixedSpSQs.getQuantity().max(scalarSpSQs.getQuantity());
                    } else {
                        quantity = mixedSpSQs.getQuantity().add(scalarSpSQs.getQuantity());
                    }

                    mixedSpSQs.setQuantity(quantity);

                    if (updatedHighlightDttmList) {
                        mixedSpSQs.setHighlightList(scalarSpSQs.getHighlightList());
                    }
                    if (modifyHighlightDttmList) {
                        List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight> highlightDateTimeList = new ArrayList<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight>();
                        highlightDateTimeList.addAll(mixedSpSQs.getHighlightList());
                        int mixedSpSQsHighlightListSeq = highlightDateTimeList.size();
                        for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight scalarSPSQshighlightList : scalarSpSQs
                                .getHighlightList()) {
                            int listSize = 0;
                            for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight mixedSPSQsHighlightList : mixedSpSQs
                                    .getHighlightList()) {
                                if (!mixedSPSQsHighlightList.getHighlightDateTime().equals(
                                        scalarSPSQshighlightList.getHighlightDateTime())) {
                                    listSize++;
                                }
                            }
                            if (listSize == highlightDateTimeList.size()) {
                                mixedSpSQsHighlightListSeq++;
                                CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight highlightDateTimeListNode = new CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight();
                                highlightDateTimeListNode.setSequence(new BigDecimal(mixedSpSQsHighlightListSeq));
                                highlightDateTimeListNode.setHighlightDateTime(scalarSPSQshighlightList
                                        .getHighlightDateTime());
                                highlightDateTimeListNode.setHighlightType(scalarSPSQshighlightList.getHighlightType());
                                highlightDateTimeListNode.setHighlightCondition(scalarSPSQshighlightList
                                        .getHighlightDerivedCondition());
                                highlightDateTimeListNode.setHighlightDerivedCondition(scalarSPSQshighlightList
                                        .getHighlightDerivedCondition());
                                highlightDateTimeList.add(highlightDateTimeListNode);
                            }
                        }
                        mixedSpSQs.setHighlightList(highlightDateTimeList);
                    }
                    if (clearHighlightDttmList) {
                        mixedSpSQs.setHighlightList(null);
                    }
                    isUpdate = true;
                    break;
                }
            }
            if (!isUpdate) {
                scalarSpSQs.setSequence(new BigDecimal(spSQsSeqNo));
                mixedSummaryUsagePeriodSpSQsList.add(scalarSpSQs);
            }
        }
        mixedSummaryUsagePeriod.setSPServiceQuantityList(mixedSummaryUsagePeriodSpSQsList);
    }

    /**
     * This method will check for the Overlap of Interval and Scalar Summary Usage Periods.
     * 
     * @param intervalSummaryUsagePeriodList
     * @param scalarSummaryUsagePeriodList
     * @return boolean
     */
    private boolean checkForOverlapSummaryUsagePeriods(
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> intervalSummaryUsagePeriodList,
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> scalarSummaryUsagePeriodList) {
        for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod scalarSummaryUsagePeriod : scalarSummaryUsagePeriodList) {
            for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod intervalSummaryUsagePeriod : intervalSummaryUsagePeriodList) {
                if (isOverlapSummaryUsagePeriod(scalarSummaryUsagePeriod, intervalSummaryUsagePeriod)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     *  This method will check for Overlap Summary Usage Periods.
     * 
     *  @param sourceUsagePeriod
     *  @param targetUsagePeriod
     * 
     */
    private boolean isOverlapSummaryUsagePeriod(
            CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod sourceSummaryUsagePeriod,
            CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod targetSummaryUsagePeriod) {
        DateTime sourceStartDttm = sourceSummaryUsagePeriod.getStartDateTime();
        DateTime sourceEndDttm = sourceSummaryUsagePeriod.getEndDateTime();
        DateTime targetStartDttm = targetSummaryUsagePeriod.getStartDateTime();
        DateTime targetEndDttm = targetSummaryUsagePeriod.getEndDateTime();

        if ((sourceStartDttm.isAfter(targetStartDttm) && sourceStartDttm.isBefore(targetEndDttm))
                || (sourceEndDttm.isAfter(targetStartDttm) && sourceEndDttm.isBefore(targetEndDttm))
                || (sourceStartDttm.isSameOrBefore(targetStartDttm) && sourceEndDttm.isSameOrAfter(targetEndDttm))) {
            return true;
        }
        return false;
    }
    
    /**
     *  This method will creates the UT Export Master Configuration BO Instance, if it is not already created.
     */
    private void retrieveUTExportConfigBOInstance(){
    	if (isNull(utExportConfigObjectInstance)) {
            utExportConfigObjectInstance = BusinessObjectInstance.create(USAGE_TRAN_EXPORT_CONFIG_BO);
            utExportConfigObjectInstance.set("bo", USAGE_TRAN_EXPORT_CONFIG_BO);
            utExportConfigObjectInstance = BusinessObjectDispatcher
                    .read(utExportConfigObjectInstance, true);
        }
    }
    /**
     *  This method  will returns the merged summary usage periods of the same usage type i.e. interval or scalar
     * 
     * @param summaryUsagePeriodsList
     * @return
     */
    private List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> mergeOverlapSummaryUsagePeriods(
            List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> summaryUsagePeriodsList) {
        int periodIndex = 0;
        List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> mergedSummaryUsagePeriodsList = new ArrayList<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod>();
        for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod summaryUsagePeriod : summaryUsagePeriodsList) {
            if (periodIndex == 0) {
                mergedSummaryUsagePeriodsList.add(summaryUsagePeriod);
                periodIndex++;
            } else {
                boolean isInsert = true;
                for (CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod mergedSummaryUsagePeriod : mergedSummaryUsagePeriodsList) {
                    if (isOverlapSummaryUsagePeriod(summaryUsagePeriod, mergedSummaryUsagePeriod)) {
                        //Insert or update the matched summary usage period
                        insertOrUpdateSummaryUsagePeriodList(mergedSummaryUsagePeriod, summaryUsagePeriod, false);

                        //Update the summary usage period start and end date times
                        DateTime mergedSummaryUsgPerStartDttm = mergedSummaryUsagePeriod.getStartDateTime();
                        DateTime mergedSummaryUsgPerEndDttm = mergedSummaryUsagePeriod.getEndDateTime();
                        DateTime summaryUsgPerStartDttm = summaryUsagePeriod.getStartDateTime();
                        DateTime summaryUsgPerEndDttm = summaryUsagePeriod.getEndDateTime();
                        if (summaryUsgPerStartDttm.isBefore(mergedSummaryUsgPerStartDttm)) {
                            mergedSummaryUsagePeriod.setStartDateTime(summaryUsgPerStartDttm);
                            mergedSummaryUsagePeriod.setStandardStartDateTime(summaryUsagePeriod
                                    .getStandardStartDateTime());
                        }
                        if (summaryUsgPerEndDttm.isAfter(mergedSummaryUsgPerEndDttm)) {
                            mergedSummaryUsagePeriod.setEndDateTime(summaryUsgPerEndDttm);
                            mergedSummaryUsagePeriod
                                    .setStandardEndDateTime(summaryUsagePeriod.getStandardEndDateTime());
                        }

                        isInsert = false;
                        break;
                    }
                }
                if (isInsert) {
                    mergedSummaryUsagePeriodsList.add(summaryUsagePeriod);
                }
            }
        }
        return mergedSummaryUsagePeriodsList;
    }
}
