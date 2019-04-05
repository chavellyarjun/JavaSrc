package com.splwg.cm.domain.admin.veeRule.applyVeeRule;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import com.splwg.base.domain.common.businessObject.BusinessObject;
import com.splwg.base.domain.common.timeZone.TimeZone;
import com.splwg.base.domain.common.timeZone.TimeZone_Id;
import com.splwg.d1.domain.admin.exceptionType.entities.ExceptionType;
import com.splwg.d1.domain.admin.exceptionType.entities.ExceptionType_Id;
import com.splwg.d1.domain.admin.measuringComponentType.entities.MeasuringComponentType;
import com.splwg.d1.domain.admin.veeRule.applyVeeRule.ApplyVeeRuleAlgorithmInputData;
import com.splwg.d1.domain.admin.veeRule.applyVeeRule.ApplyVeeRuleAlgorithmInputOutputData;
import com.splwg.d1.domain.admin.veeRule.applyVeeRule.ApplyVeeRuleAlgorithmSpot;
import com.splwg.d1.domain.admin.veeRule.applyVeeRule.VeeRuleExceptionData;
import com.splwg.d1.domain.admin.veeRule.applyVeeRule.VeeRuleVariablesData;
import com.splwg.d1.domain.deviceManagement.deviceConfiguration.entities.DeviceConfiguration;
import com.splwg.d1.domain.deviceManagement.measuringComponent.entities.MeasuringComponent;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointD1;
import com.splwg.d1.domain.measurement.initialMeasurementData.routines.DetermineStartRead;
import com.splwg.d1.domain.measurement.measurement.entities.Measurement;
import com.splwg.d1.domain.measurement.measurement.entities.Measurement_Id;
import com.ibm.icu.math.BigDecimal;
import com.splwg.d2.domain.admin.MessageRepository;
import com.splwg.d1.api.lookup.DataSourceClassLookup;
import com.splwg.d1.api.lookup.DataSrcLookup;
import com.splwg.d1.api.lookup.ExceptionSeverityLookup;
import com.splwg.cm.api.lookup.ApplyrulefordeviceLookup;
import com.splwg.cm.api.lookup.OverUnderLimitCheckTypeLookup;
import com.splwg.cm.domain.CMOnSpotBillingErrorMessagesRepository;
import com.splwg.cm.domain.CMOnSpotBillingWarningMessagesRepository;
//import com.splwg.cm.domain.customMessages.CmMessageRepository;
import com.splwg.d1.api.lookup.TraceOnLookup;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.businessObject.COTSInstanceList;
import com.splwg.base.api.businessObject.COTSInstanceListNode;
import com.splwg.base.api.businessObject.COTSInstanceNode;
import com.splwg.base.api.businessObject.SchemaInstance;
import com.splwg.base.api.businessService.BusinessServiceDispatcher;
import com.splwg.base.api.businessService.BusinessServiceInstance;
import com.splwg.base.api.installation.InstallationHelper;
import com.splwg.d1.domain.common.data.ShiftDateTimeWithDuplicateHourInputOutputData;
import com.splwg.d1.domain.common.routines.ServerMessageToMdmMessageConverter;
import com.splwg.d1.domain.common.routines.ShiftDateTimeWithDuplicateHour;
import com.splwg.d1.domain.common.routines.TimeZoneDeriver;
import com.splwg.d1.domain.common.routines.TimeZoneDeriver.Factory;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.d1.domain.admin.veeRule.applyVeeRule.VeeRuleTraceData;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Abjayon
 *
 @AlgorithmComponent (softParameters = { @AlgorithmSoftParameter (name =
 *                     overLimitCheckStatusCode, type = string) , @AlgorithmSoftParameter
 *                     (name = underLimitCheckStatusCode1, type = string) , @AlgorithmSoftParameter
 *                     (name = underLimitCheckStatusCode2, type = string)})
 */

public class CmOverUnderLimitConsumptionValidationAlgComp_Impl extends CmOverUnderLimitConsumptionValidationAlgComp_Gen implements 
ApplyVeeRuleAlgorithmSpot{
	
	private static final Logger logger = LoggerFactory
			.getLogger(CmOverUnderLimitConsumptionValidationAlgComp_Impl.class);
	
	private static final String IS_TRACE_ON_XPATH = "isTraceOn";
	private static final BigDecimal OVER_LIMIT_CHECK_1 = new BigDecimal(5);
	private static final BigDecimal OVER_LIMIT_CHECK_2 = new BigDecimal(8);
	private static final String IMA_DATA_SRC = "CMIM";
	private static final String SYS_DATA_SRC = "CMSY";
	private static final BigDecimal OVER_LIMIT_STATUS_CODE = new BigDecimal(6099);
	
	private ApplyVeeRuleAlgorithmInputData applyVeeRuleAlgorithmInputData;
	private ApplyVeeRuleAlgorithmInputOutputData applyVeeRuleAlgorithmInputOutputData;
	
	private BigDecimal lowConsumptionLimit;
	private MeasuringComponent measuringComponent;
	private TraceOnLookup isTraceOn;
	private TimeZone installationTimeZone;
	private DeviceConfiguration deviceConfiguration;
	private TimeZone mcTimeZone;
	private SchemaInstance initalMeasurementDataBoInstance;
	private COTSInstanceNode postVeeGroup;
	private COTSInstanceNode syncIMDOtherInfo;
	private DateTime measurementStartDateTime;
	private DateTime measurementEndDateTime;
	private COTSInstanceNode measurementDataGroup;
	private COTSInstanceNode imaDetailsGroup;
	private COTSInstanceList measurementDataList;
	private COTSInstanceList imaStatusCodesList;
	private COTSInstanceList sysStatusCodesList;
	private List<VeeRuleTraceData> veeRuleTraceList;
	private List<VeeRuleVariablesData> veeRuleVariablesDataList;
	private MeasuringComponentType measrCompType;
	private BusinessObjectInstance veeRuleOverUnderLimitCheckBoInstance;
	private boolean isExit;
	private boolean noHistoricalDataFoundException;
	private boolean overLimitException;
	private boolean underLimitException;
	private BigDecimal calculatedConsumption;
	private boolean isEmptyCalculatedConsumption;
	private OverUnderLimitCheckTypeLookup overUnderLimitCheck;
	private BigDecimal startMeasurement;
	private BigDecimal endMeasurement;
	private BigDecimal overLimitValue;
	private BigDecimal overLimitValue2;
	private Boolean isFirstRead;
	private String overLimitCheckStatusCode;
	private String underLimitCheckStatusCode1;
	private String underLimitCheckStatusCode2;
	private String dataSource;
	private String applyRule;
	private Boolean isApplyRule;
	
	
	
	public CmOverUnderLimitConsumptionValidationAlgComp_Impl() {
		this.lowConsumptionLimit = null;
		this.isTraceOn = null;
		this.installationTimeZone = null;
		this.deviceConfiguration = null;
		this.mcTimeZone = null;
		this.isExit = false;
		this.noHistoricalDataFoundException = false;
		this.overLimitException = false;
		this.underLimitException = false;
		this.calculatedConsumption = BigDecimal.ZERO;
		this.isEmptyCalculatedConsumption = false;
		this.startMeasurement = BigDecimal.ZERO;
		this.endMeasurement = BigDecimal.ZERO;
		this.overLimitValue = BigDecimal.ZERO;
		this.overLimitValue2 = BigDecimal.ZERO;
		this.overLimitCheckStatusCode = null;
		this.underLimitCheckStatusCode1 = null;
		this.underLimitCheckStatusCode2 = null;
		this.isFirstRead = false;
		this.isApplyRule = true;
	    this.dataSource = null;
	}
	
	public ApplyVeeRuleAlgorithmInputOutputData getApplyVeeRuleAlgorithmInputOutputData() {
		return this.applyVeeRuleAlgorithmInputOutputData;
	}

	public void setApplyVeeRuleAlgorithmInputData(
			ApplyVeeRuleAlgorithmInputData applyVeeRuleAlgorithmInputData) {
		this.applyVeeRuleAlgorithmInputData = applyVeeRuleAlgorithmInputData;
	}

	public void setApplyVeeRuleAlgorithmInputOutputData(
			ApplyVeeRuleAlgorithmInputOutputData applyVeeRuleAlgorithmInputOutputData) {
		this.applyVeeRuleAlgorithmInputOutputData = applyVeeRuleAlgorithmInputOutputData;
	}
	
	public void invoke() {
		this.validateInputParameters();
		if(isFirstRead)
			return;

		try {
			this.mainProcessing();
		} finally {
			this.setTraceList();
		}

	}
	
	private void validateInputParameters() {
		int softParamCount = 0;
		int var7 = softParamCount + 1;
		this.overLimitCheckStatusCode = this.getSoftParameter(softParamCount);
		this.underLimitCheckStatusCode1 = this.getSoftParameter(var7++);
		this.underLimitCheckStatusCode2 = this.getSoftParameter(var7++);
		
		this.validateInputParmVeeRule();
		this.validateInputParamMeasuringComponent();
		
	}
	
	private void validateInputParmVeeRule() {
		if (this.isNull(this.applyVeeRuleAlgorithmInputData.getVeeRule())
				|| this.isBlankOrNull(this.applyVeeRuleAlgorithmInputData
						.getVeeRule().fetchIdVeeRule())) {
			this.addError(MessageRepository.inputParamVeeRuleIdIsMissing());
		}

	}
	
	private void validateInputParamMeasuringComponent() {
		this.measuringComponent = this.applyVeeRuleAlgorithmInputData
				.getMeasuringComponent();
		if (this.isNull(this.measuringComponent)
				|| this.isBlankOrNull(this.measuringComponent.getId()
						.getTrimmedValue())) {
			this.addError(MessageRepository
					.inputParamMeasuringComponenetIsMissing());
		}
		if(this.isNull(this.measuringComponent.getMostRecentMeasurementDateTime())){
			isFirstRead = true;
		}
		this.measrCompType = this.measuringComponent.getMeasuringComponentType();

	}
	
	private void mainProcessing() {
		if (this.notNull(this.applyVeeRuleAlgorithmInputOutputData)
				&& this.notNull(this.applyVeeRuleAlgorithmInputOutputData
						.getVeeInitialMeasurementData())) {
			this.isTraceOn = (TraceOnLookup) this.applyVeeRuleAlgorithmInputOutputData
					.getVeeInitialMeasurementData().getLookup("isTraceOn");
			this.installationTimeZone = (TimeZone) (new TimeZone_Id(
					InstallationHelper.getTimeZoneCode())).getEntity();
			TimeZoneDeriver timeZoneDriver = Factory.newInstance();
			this.deviceConfiguration = this.isNull(this.measuringComponent
					.getDeviceConfigurationId())
					? null
					: (DeviceConfiguration) this.measuringComponent
							.getDeviceConfigurationId().getEntity();
			this.mcTimeZone = timeZoneDriver.getTimeZone((ServicePointD1) null,
					this.deviceConfiguration, this.measuringComponent);
			
			this.readVeeRuleBo();
			
			this.readInitialMeasurementDataBo();
			if(!(this.isApplyRule)){
				return;
			}
			
			this.validateStartDateForScalarMC();
			
			if (this.isExit) {
				this.setTraceListForField("EXECUTION_ELIGIBILITY", "false");
				return;
			}
			
			this.getCalculatedConsumption();
			if (this.isExit) {
				this.setTraceListForField("EXECUTION_ELIGIBILITY", "false");
				return;
			}

			this.getConsumptionValueFromVeeCommonRoutine();
			
		}
	}
	
	private void getConsumptionValueFromVeeCommonRoutine() {
		this.retrieveVeeRuleBoDetails();
		overUnderLimitConditionCheck();
		
	}

	private void overUnderLimitConditionCheck() {
		this.startMeasurement = new Measurement_Id(this.measuringComponent,measurementStartDateTime).getEntity().getMeasurementValue();
		//this.endMeasurement = new Measurement_Id(this.measuringComponent,measurementEndDateTime).getEntity().getMeasurementValue();
		this.endMeasurement = this.calculatedConsumption;
			if(notNull(this.startMeasurement) && notNull(this.endMeasurement))
			{
			if(this.notNull(this.overUnderLimitCheck)){
				if (this.overUnderLimitCheck.isOverLimit()){
					overLimitValidation();
				}
				else if(this.overUnderLimitCheck.isUnderLimit()){
					underLimitValidation();
				}
				else{
					overLimitValidation();
					underLimitValidation();
				}
		     }
			}
	}

	private void underLimitValidation() {
		if(this.endMeasurement.compareTo(lowConsumptionLimit) < 0){
			
			if(notNull(this.imaStatusCodesList) && (this.imaStatusCodesList.getSize() > 0)){
				Boolean isImaStatusCode = checkUnderLimitStatusCode(imaStatusCodesList);
				if(isImaStatusCode)
					return;
			}
			
			else if(notNull(this.sysStatusCodesList) && (this.sysStatusCodesList.getSize() > 0)){
				Boolean isSysStatusCode = checkUnderLimitStatusCode(sysStatusCodesList);
				if(isSysStatusCode)
					return;
			}
			
			this.underLimitException = true;
			this.underLimitCheckException();
		}
		
		
	}

	private void overLimitValidation() {
		//Bypass VEE Rule when start Measurement value is zero.
		if(this.startMeasurement.compareTo(BigDecimal.ZERO) == 0){
			return;
		}
		
		this.overLimitValue = this.startMeasurement.multiply(OVER_LIMIT_CHECK_1);
		if(this.endMeasurement.compareTo(this.overLimitValue) >= 0){
		
			//VEE Rule when data source is IMA and has Over Limit Check Status Code
			if(notNull(this.imaStatusCodesList) && (this.imaStatusCodesList.getSize() > 0)){
				Boolean isImaStatusCode = checkOverLimitStatusCode(imaStatusCodesList);
				if(isImaStatusCode){
					this.overLimitValue2 = this.startMeasurement.multiply(OVER_LIMIT_CHECK_2);
					if(this.endMeasurement.compareTo(this.overLimitValue2) >= 0){
						this.overLimitException = true;
						this.imaDetailsGroup.set("currentStatusCode",OVER_LIMIT_STATUS_CODE);
						this.overLimitCheckException();
						return;
					}else{
						return;
					}
				}
					
			}
			//VEE Rule when data source is SYS and has Over Limit Check Status Code
			else if(notNull(this.sysStatusCodesList) && (this.sysStatusCodesList.getSize() > 0)){
				Boolean isSysStatusCode = checkOverLimitStatusCode(sysStatusCodesList);
				if(isSysStatusCode){
					this.overLimitValue2 = this.startMeasurement.multiply(OVER_LIMIT_CHECK_2);
					if(this.endMeasurement.compareTo(this.overLimitValue2) >= 0){
						this.overLimitException = true;
						this.overLimitCheckException();
						return;
					}else{
						return;
					}
					
				}
					
			}
			this.overLimitException = true;
			this.overLimitCheckException();
			
		}
		
		
	}
	
	private Boolean checkUnderLimitStatusCode(COTSInstanceList statusCodesList) {
		Iterator<COTSInstanceListNode> statusCodesIterator = statusCodesList.iterator();
		while(statusCodesIterator.hasNext()){
			COTSInstanceListNode statusCode = statusCodesIterator.next();
			if(statusCode.getElement().element("customerStatusCode").getTextTrim().equals(this.underLimitCheckStatusCode1)
					|| (statusCode.getElement().element("customerStatusCode").getTextTrim().equals(this.underLimitCheckStatusCode2))){
				
				this.imaDetailsGroup.set("currentStatusCode", new BigDecimal(statusCode.getElement().element("customerStatusCode").getTextTrim()));
				this.applyVeeRuleAlgorithmInputOutputData.setVeeInitialMeasurementData(initalMeasurementDataBoInstance);
				return true;
			}
		}
		return false;
	}
	
	private Boolean checkOverLimitStatusCode(COTSInstanceList statusCodesList) {
		Iterator<COTSInstanceListNode> statusCodesIterator = statusCodesList.iterator();
		while(statusCodesIterator.hasNext()){
			COTSInstanceListNode statusCode = statusCodesIterator.next();
			if(statusCode.getElement().element("customerStatusCode").getTextTrim().equals(this.overLimitCheckStatusCode)){
				this.imaDetailsGroup.set("currentStatusCode", new BigDecimal(statusCode.getElement().element("customerStatusCode").getTextTrim()));
				return true;
			}
		}
		return false;
	}

	private void noDataFoundException() {
		COTSInstanceNode noHistoricalDataFound = this.veeRuleOverUnderLimitCheckBoInstance
				.getGroupFromPath("noHistoricalDataFoundException");
		if (this.notNull(noHistoricalDataFound)) {
			ExceptionType noDataFoundExceptionType = this
					.isNull(noHistoricalDataFound.getFieldAndMDForPath(
							"exceptionType").getValue())
					? null
					: (ExceptionType) (new ExceptionType_Id(
							((String) noHistoricalDataFound
									.getFieldAndMDForPath("exceptionType")
									.getValue()).trim())).getEntity();
			ExceptionSeverityLookup noDataFoundExceptionSeverity = this
					.isNull(noHistoricalDataFound
							.getLookup("exceptionSeverity"))
					? null
					: (ExceptionSeverityLookup) noHistoricalDataFound
							.getLookup("exceptionSeverity");
			this.addNewEntryToVeeExceptionList(noDataFoundExceptionType,
					noDataFoundExceptionSeverity);
		}

	}

	private void retrieveVeeRuleBoDetails() {
		this.overUnderLimitCheck = (OverUnderLimitCheckTypeLookup) this.veeRuleOverUnderLimitCheckBoInstance.getLookup("overUnderLimitCheck");
		this.lowConsumptionLimit = this.veeRuleOverUnderLimitCheckBoInstance.getNumber("lowConsulptionLimit");
		
	}

	private void getCalculatedConsumption() {
		if (this.notNull(this.measrCompType)) {
			this.calculateConsumption();
			this.validateCalculatedConsumption();
		}

	}
	
	private void validateCalculatedConsumption() {
		if (this.measurementDataList.getSize() == 0) {
			COTSInstanceNode insufficientInputDataExceptions = this.veeRuleOverUnderLimitCheckBoInstance
					.getGroupFromPath("insufficientInputDataExceptions");
			if (this.notNull(insufficientInputDataExceptions)) {
				ExceptionType insufficientExceptionType = this
						.isNull(insufficientInputDataExceptions
								.getFieldAndMDForPath("exceptionType")
								.getValue())
						? null
						: (ExceptionType) (new ExceptionType_Id(
								((String) insufficientInputDataExceptions
										.getFieldAndMDForPath("exceptionType")
										.getValue()).trim())).getEntity();
				ExceptionSeverityLookup insufficientExceptionSeverity = this
						.isNull(insufficientInputDataExceptions
								.getLookup("exceptionSeverity"))
						? null
						: (ExceptionSeverityLookup) insufficientInputDataExceptions
								.getLookup("exceptionSeverity");
				this.isEmptyCalculatedConsumption = true;
				this.addNewEntryToVeeExceptionList(insufficientExceptionType,
						insufficientExceptionSeverity);
			}
		}

	}
	
	private void calculateConsumption() {
		Iterator iter = this.measurementDataList.iterator();

		while (iter.hasNext()) {
			COTSInstanceListNode each = (COTSInstanceListNode) iter.next();
			if (this.notNull(each.getNumber("q"))) {
				this.calculatedConsumption = this.calculatedConsumption
						.add(each.getNumber("q"));
			}
		}

	}
	
	private void validateStartDateForScalarMC() {
		if (this.measuringComponent.getMeasuringComponentType().getIntervalScalar().isScalar()
				&& this.isNull(this.measurementStartDateTime)) {
			this.noHistoricalDataFoundException = true;
			this.noDataFoundException();
		}

	}
	
	private void setTraceListForField(String fieldName, String value) {
		if (this.notNull(this.isTraceOn) && this.isTraceOn.isYes()) {
			this.veeRuleTraceList = this.applyVeeRuleAlgorithmInputOutputData
					.getVeeTraceList();
			if (this.notNull(this.veeRuleTraceList)
					&& !this.veeRuleTraceList.isEmpty()) {
				VeeRuleVariablesData veeRuleVariablesData = com.splwg.d1.domain.admin.veeRule.applyVeeRule.VeeRuleVariablesData.Factory
						.newInstance();
				String fieldDesc = this.retrieveFieldDescription(fieldName);
				veeRuleVariablesData.setDescription(fieldDesc);
				if (this.notBlank(value)) {
					veeRuleVariablesData.setValue(value);
				} else {
					veeRuleVariablesData.setValue("NULL");
				}

				this.veeRuleVariablesDataList.add(veeRuleVariablesData);
			}
		}

	}
	
	private String retrieveFieldDescription(String fieldName) {
		BusinessServiceInstance bsInstance = BusinessServiceInstance
				.create("F1-GetFieldLabel");
		bsInstance.set("field", fieldName);
		BusinessServiceInstance result = BusinessServiceDispatcher
				.execute(bsInstance);
		String fieldDescription = result.getString("longLabel");
		return fieldDescription;
	}
	
	private void readVeeRuleBo() {
		BusinessObject veeRuleOverUnderLimitCheckBo = this.applyVeeRuleAlgorithmInputData
				.getVeeRuleBo();
		long startTime = System.currentTimeMillis();
		this.veeRuleOverUnderLimitCheckBoInstance = BusinessObjectInstance
				.create(veeRuleOverUnderLimitCheckBo);
		this.veeRuleOverUnderLimitCheckBoInstance.set("veeGroup",
				this.applyVeeRuleAlgorithmInputData.getVeeRule()
						.fetchIdVeeGroup().getId().getTrimmedValue());
		this.veeRuleOverUnderLimitCheckBoInstance.set("veeRule",
				this.applyVeeRuleAlgorithmInputData.getVeeRule()
						.fetchIdVeeRule().trim());
		this.veeRuleOverUnderLimitCheckBoInstance = BusinessObjectDispatcher
				.read(this.veeRuleOverUnderLimitCheckBoInstance);
		long endTime = System.currentTimeMillis();
		logger.info("**"+this.veeRuleOverUnderLimitCheckBoInstance.getElement().element("applyRuleForDevice"));
		logger.info("**"+this.veeRuleOverUnderLimitCheckBoInstance.getLookup("applyRuleForDevice").value());
		this.applyRule = this.veeRuleOverUnderLimitCheckBoInstance.getElement().element("applyRuleForDevice").getTextTrim();
		logger.info("Reading BO:"
				+ this.veeRuleOverUnderLimitCheckBoInstance.getSchemaName()
				+ ", in HighLowChkAlgComp_Impl (D2-HILO-CHK), Reason: To retrieve the High Low Check Rule values to be used during rule execution. Time taken:"
				+ (endTime - startTime) + " ms");
	}

	private void readInitialMeasurementDataBo() {
		
		this.initalMeasurementDataBoInstance = this.applyVeeRuleAlgorithmInputOutputData.getVeeInitialMeasurementData();
		logger.info("IMD Instance"+this.applyVeeRuleAlgorithmInputOutputData.getVeeInitialMeasurementData().getElement().elementText("currentStatusCode"));
		this.postVeeGroup = this.initalMeasurementDataBoInstance
				.getGroupFromPath("postVEE");
		this.dataSource = this.initalMeasurementDataBoInstance.getElement().element("dataSource").getTextTrim();
		
		//Check if data Source is same as apply rule or if apply rule is Both
		if(!(this.dataSource.equals(this.applyRule)) && (!(this.applyRule.equals(ApplyrulefordeviceLookup.constants.BOTH.getLookupValue().fetchIdFieldValue())))){
		   this.isApplyRule = false;
		   return;
		}
		this.syncIMDOtherInfo = this.initalMeasurementDataBoInstance.getGroupFromPath("syncIMDOtherInfo");
	
		if (this.notNull(this.postVeeGroup)) 
			this.retrieveInitialMeasurementDataBoDetails();
		    
		}

	

	private void retrieveInitialMeasurementDataBoDetails() {
		
		this.measurementStartDateTime = this.postVeeGroup.getDateTime("stDt");
		this.measurementEndDateTime = this.postVeeGroup.getDateTime("enDt");
		this.measurementDataGroup = this.postVeeGroup.getGroupFromPath("msrs");
		if (this.notNull(this.measurementDataGroup)) {
			this.measurementDataList = this.measurementDataGroup.getList("mL");
		}
		this.imaDetailsGroup = this.syncIMDOtherInfo.getGroupFromPath("imaDetails");
		
		if(this.notNull(imaDetailsGroup)){
			if(this.dataSource.equals(IMA_DATA_SRC)){
				this.imaStatusCodesList = this.imaDetailsGroup.getList("IMACustomerStatusCodes");
			}else{
				this.sysStatusCodesList = this.imaDetailsGroup.getList("CustomerStatusCodes");
			}
				
		}
		
		 	

	}
	
	private void setTraceList() {
		this.veeRuleTraceList = this.applyVeeRuleAlgorithmInputOutputData
				.getVeeTraceList();
		if (this.notNull(this.veeRuleTraceList)
				&& !this.veeRuleTraceList.isEmpty()) {
			int veeRuleTraceListSize = this.veeRuleTraceList.size();
			VeeRuleTraceData veeRuleTraceData = (VeeRuleTraceData) this.veeRuleTraceList
					.get(veeRuleTraceListSize - 1);
			veeRuleTraceData.setVeeRuleVariables(this.veeRuleVariablesDataList);
			this.veeRuleTraceList.set(veeRuleTraceListSize - 1,
					veeRuleTraceData);
			this.applyVeeRuleAlgorithmInputOutputData
					.setVeeTraceList(this.veeRuleTraceList);
		}

	}
	
	private void overLimitCheckException() {
		COTSInstanceNode overLimit = this.veeRuleOverUnderLimitCheckBoInstance
				.getGroupFromPath("overLimitCheckException");
		if (this.notNull(overLimitException)) {
			ExceptionType overLimitExceptionType = this
					.isNull(overLimit.getFieldAndMDForPath(
							"exceptionType").getValue())
					? null
					: (ExceptionType) (new ExceptionType_Id(
							((String) overLimit
									.getFieldAndMDForPath("exceptionType")
									.getValue()).trim())).getEntity();
			ExceptionSeverityLookup overLimitExceptionSeverity = this
					.isNull(overLimit
							.getLookup("exceptionSeverity"))
					? null
					: (ExceptionSeverityLookup) overLimit
							.getLookup("exceptionSeverity");
			this.addNewEntryToVeeExceptionList(overLimitExceptionType,
					overLimitExceptionSeverity);
		}

	}
	
	private void underLimitCheckException() {
		COTSInstanceNode underLimit = this.veeRuleOverUnderLimitCheckBoInstance
				.getGroupFromPath("underLimitCheckException");
		if (this.notNull(underLimitException)) {
			ExceptionType underLimitExceptionType = this
					.isNull(underLimit.getFieldAndMDForPath(
							"exceptionType").getValue())
					? null
					: (ExceptionType) (new ExceptionType_Id(
							((String) underLimit
									.getFieldAndMDForPath("exceptionType")
									.getValue()).trim())).getEntity();
			ExceptionSeverityLookup underLimitExceptionSeverity = this
					.isNull(underLimit
							.getLookup("exceptionSeverity"))
					? null
					: (ExceptionSeverityLookup) underLimit
							.getLookup("exceptionSeverity");
			this.addNewEntryToVeeExceptionList(underLimitExceptionType,
					underLimitExceptionSeverity);
		}

	}
	
	private void addNewEntryToVeeExceptionList(ExceptionType exceptionType,
			ExceptionSeverityLookup exceptionSeverity) {
		List<VeeRuleExceptionData> veeExceptions = this.applyVeeRuleAlgorithmInputOutputData
				.getVeeExceptionList();
		if (this.isNull(veeExceptions)) {
			veeExceptions = new ArrayList();
		}

		VeeRuleExceptionData veeRuleExceptionData = com.splwg.d1.domain.admin.veeRule.applyVeeRule.VeeRuleExceptionData.Factory
				.newInstance();
		if (this.notNull(exceptionType)) {
			veeRuleExceptionData.setExceptionType(exceptionType);
		}

		veeRuleExceptionData.setExceptionSeverity(exceptionSeverity);
		veeRuleExceptionData.setVeeRule(this.applyVeeRuleAlgorithmInputData
				.getVeeRule());
		this.setMessageParms(veeRuleExceptionData);
		veeRuleExceptionData.setSequence(BigInteger
				.valueOf((long) (((List) veeExceptions).size() + 1)));
		((List) veeExceptions).add(veeRuleExceptionData);
		this.applyVeeRuleAlgorithmInputOutputData
				.setVeeExceptionList((List) veeExceptions);
		this.isExit = true;
	}
	
	private VeeRuleExceptionData setMessageParms(
			VeeRuleExceptionData veeRuleExceptionData) {
		
		
		if (this.noHistoricalDataFoundException) {
            veeRuleExceptionData.setMdmMessageData(com.splwg.d1.domain.common.routines.ServerMessageToMdmMessageConverter.Factory.newInstance().convertServerMessageToMdmMessage(MessageRepository.historicalDataNotFound()));
             this.noHistoricalDataFoundException = false;
		   }
		
		else if(this.overLimitException){
		
		if(this.overLimitValue2.compareTo(BigDecimal.ZERO) > 0){
			
		veeRuleExceptionData
				.setMdmMessageData(com.splwg.d1.domain.common.routines.ServerMessageToMdmMessageConverter.Factory
						.newInstance()
						.convertServerMessageToMdmMessage(
								CMOnSpotBillingWarningMessagesRepository.overLimitException(this.endMeasurement)));
		logger.info("Message"+veeRuleExceptionData.getMdmMessageData());
		logger.info("Message"+veeRuleExceptionData.getMdmMessageData().getMessageCategory());
		logger.info("Message"+veeRuleExceptionData.getMdmMessageData().getMessageNumber());
		}
		else{
			veeRuleExceptionData
			.setMdmMessageData(com.splwg.d1.domain.common.routines.ServerMessageToMdmMessageConverter.Factory
					.newInstance()
					.convertServerMessageToMdmMessage(
							CMOnSpotBillingErrorMessagesRepository.overLimitException(this.endMeasurement)));
			
		}
		this.overLimitException = false;
	} else if (this.underLimitException) {
		
		veeRuleExceptionData
				.setMdmMessageData(com.splwg.d1.domain.common.routines.ServerMessageToMdmMessageConverter.Factory
						.newInstance()
						.convertServerMessageToMdmMessage(
								CMOnSpotBillingErrorMessagesRepository.underLimitException(this.endMeasurement)));
		logger.info("Message"+veeRuleExceptionData.getMdmMessageData());
		logger.info("Message"+veeRuleExceptionData.getMdmMessageData().getMessageCategory());
		logger.info("Message"+veeRuleExceptionData.getMdmMessageData().getMessageNumber());
		this.underLimitException = false;
	} 
		return veeRuleExceptionData;
	
}

}
