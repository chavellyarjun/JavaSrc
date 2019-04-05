package com.splwg.cm.domain.admin.measuringComponentType.valueDerivation;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.sql.PreparedStatement;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map.Entry;

import com.splwg.base.api.datatypes.Time;
import com.splwg.base.api.Query;
import com.splwg.base.api.QueryResultRow;
import com.ibm.icu.math.MathContext;
import com.ibm.icu.text.DecimalFormat;
import com.splwg.ccb.api.lookup.DaysOfWeekLookup;
import com.splwg.ccb.domain.customerinfo.premise.entity.PremiseCharacteristic;
import com.splwg.ccb.domain.customerinfo.servicePoint.entity.ServicePoint;
import com.splwg.cm.api.lookup.DisconnectSourceFlagLookup;
import com.splwg.d1.api.lookup.OnOffStatusIndicatorLookup;
import com.splwg.d1.domain.admin.measuringComponentType.valueDerivation.ValueDerivationAlgorithmSpot;
import com.splwg.d1.domain.admin.measuringComponentType.valueDerivation.ValueDerivationInputData;
import com.splwg.d1.domain.admin.measuringComponentType.valueDerivation.ValueDerivationInputOutputData;
import com.splwg.d1.domain.admin.timeOfUse.entities.TimeOfUseD1;
import com.splwg.d1.domain.admin.timeOfUseMapTemplate.entities.TimeOfUseMapTemplateD1;
import com.splwg.d1.domain.admin.timeOfUseMapTemplate.entities.TimeOfUseMapTemplateD1_Id;
import com.splwg.d1.domain.deviceManagement.device.entities.Device;
import com.splwg.d1.domain.deviceManagement.device.entities.DeviceCharacteristic;
import com.splwg.d1.domain.deviceManagement.measuringComponent.entities.MeasuringComponent;
import com.splwg.d1.domain.installation.facility.entities.Facility_Id;
import com.splwg.d1.domain.installation.installEvent.entities.InstallEvent;
import com.splwg.d1.domain.installation.installEvent.entities.InstallEvent_Id;
import com.splwg.d1.domain.installation.installEvent.entities.OnOffHistory;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointCharacteristicD1;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointD1;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointD1_Id;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointFacility;
import com.splwg.d1.domain.measurement.initialMeasurementData.entities.InitialMeasurementData_Id;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.businessObject.COTSInstanceList;
import com.splwg.base.api.businessObject.COTSInstanceListNode;
import com.splwg.base.api.businessObject.COTSInstanceNode;
import com.splwg.base.api.datatypes.DateFormat;
import com.splwg.base.api.datatypes.DateFormatParseException;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.DayInMonth;
import com.splwg.base.api.datatypes.Lookup;
import com.splwg.base.api.datatypes.TimeInterval;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.characteristicType.CharacteristicType;
import com.splwg.base.support.expression.functions.math.RoundFunction;
import com.splwg.d1.domain.admin.MessageRepository;
import com.splwg.shared.common.Dom4JHelper;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;
import com.splwg.d1.domain.common.routines.RoundingHelper;
import com.splwg.d1.domain.common.routines.RoundingHelper.Factory;
import com.splwg.d2.api.lookup.DaysOfTheWeekLookup;
import com.sun.javafx.collections.MappingChange.Map;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;

import java.util.List;

/**
 * @author saisr
 *
 @AlgorithmComponent (softParameters = { @AlgorithmSoftParameter (name =
 *                     targetElement, type = string) , @AlgorithmSoftParameter
 *                     (name = systemVoltageCharType, type = string) , @AlgorithmSoftParameter
 *                     (name = nominalTransformerPowerCharType, type = string) , @AlgorithmSoftParameter
 *                     (name = transformerClassCharType, type = string) , @AlgorithmSoftParameter
 *                     (name = touMapTemplate, type = string)})
 */
public class DeriveMeasurementValueAlgComp_Impl extends
		DeriveMeasurementValueAlgComp_Gen implements
		ValueDerivationAlgorithmSpot {

	public static Logger logger = LoggerFactory
			.getLogger(DeriveMeasurementValueAlgComp_Impl.class);

	private static final String DEDAS_TRNSF_LOSS_MASTER_CONFIG_BO = "CM-DEDASTransfLossMasterConfig";
	private static final String VOLTAGE_TRANSFORMERS = "voltageTransformers";
	private static final String SYSTEM_VOLTAGE = "systemVoltage";
	private static final String NOMINAL_TRNSF_PWR = "nominalTransformerPower";
	private static final String ORIGINAL_IMD = "originalIMD";
	private static final String A_CLASS = "classA";
	private static final String OTHER_CLASS = "otherClass";
	private static final String DISCONNECT_SOURCE = "disconnectSource";
	private static final Double UNTIL_SYSTEM_VOLT_VAL = 15.8;
	private static final Double SYSTEM_VOLT_VAL = 33.0;
	private static final DateFormat dateFormat = new DateFormat(
			"yyyy-MM-dd-HH:mm:ss");

	private ValueDerivationInputData inputData;
	private ValueDerivationInputOutputData inputOutputData;
	private MeasuringComponent measuringComponent;
	private String targetElement;
	private String nominalTransformerPowerCharType;
	private String systemVoltageCharType;
	private String transformerClassCharType;
	private String touMapTemplate;
	private List<BusinessObjectInstance> measurementList;
	private String measurementBoName;
	private BigDecimal transformerLoss;
	private BigDecimal subTransformerLoss = null;
	private BigDecimal readingHours = null;
	private BigDecimal offElectricHours = BigDecimal.ZERO;
	private RoundingHelper rh = Factory.newInstance();
	private String fieldName = "MSRMT_VAL";
	private String offElectricCharValue = null;
	private BigDecimal sec;
	
	private Time startTime = null;
	private Time endTime = null;
	

	RoundingHelper roundingHelper = Factory.newInstance();
	MathContext mathContext = roundingHelper
			.getContext("D1MAX_NBR_FOR_COMPUTATION");

	@Override
	public void invoke() {
		// TODO Auto-generated method stub

		if (this.notNull(this.inputData.getMeasuringComponentId())) {
			this.measuringComponent = (MeasuringComponent) this.inputData
					.getMeasuringComponentId().getEntity();
			this.measurementBoName = this.inputOutputData
					.getMeasurementBOName();
			this.measurementList = this.inputOutputData.getMeasurementList();

			if (this.notBlank(this.measurementBoName)
					&& this.notNull(this.measurementList)
					&& !this.measurementList.isEmpty()) {
				this.validateParameters();
				this.performMainProcessing();
			}
		}

	}

	private void validateParameters() {
		int softParamCount = 0;
		int var7 = softParamCount + 1;
		this.targetElement = this.getSoftParameter(softParamCount);
		this.systemVoltageCharType = this.getSoftParameter(var7++);
		this.nominalTransformerPowerCharType = this.getSoftParameter(var7++);
		this.transformerClassCharType = this.getSoftParameter(var7++);
		this.touMapTemplate = this.getSoftParameter(var7++);

		if (this.isNull(this.targetElement)) {
			this.addError(MessageRepository.targetElementMustBeProvided());
		}
	}

	private void performMainProcessing() {

		ServicePointD1 servicePoint = null;
		String systemVoltage = null;
		String nominalTranformPower = null;
		String transformerClass = null;
		BigDecimal lossPower = null;
		Facility_Id facilityId = null;
		BigDecimal touTempHours = BigDecimal.ZERO;

		InstallEvent installEvent = this.measuringComponent
				.getDeviceConfigurationId().getEntity()
				.retrieveCurrentInstallationInformation(getProcessDateTime());
		if (notNull(installEvent)) {
			servicePoint = installEvent.getServicePoint();
			Iterator<ServicePointFacility> spFacilityIterator = servicePoint
					.getServicePointFacilities().iterator();
			while (spFacilityIterator.hasNext()) {
				ServicePointFacility spFacility = spFacilityIterator.next();
				facilityId = spFacility.getFacility().getId();
				logger.info("Facility" + facilityId);
			}
			if (isNull(facilityId)) {
				// No Transformer
				return;
			}
			// Retrieve SP Characteristics
			ServicePointCharacteristicD1 spCharSystemVoltage = (ServicePointCharacteristicD1) servicePoint
					.getCharacteristics()
					.createFilter(
							"where CHAR_TYPE_CD = '" + systemVoltageCharType
									+ "' order by EFFDT DESC", "").firstRow();
			if (notNull(spCharSystemVoltage)) {
				systemVoltage = spCharSystemVoltage
						.getAdhocCharacteristicValue();
				logger.info("System Voltage" + systemVoltage);
				ServicePointCharacteristicD1 spCharRLoad = (ServicePointCharacteristicD1) servicePoint
						.getCharacteristics()
						.createFilter(
								"where CHAR_TYPE_CD = '"
										+ nominalTransformerPowerCharType
										+ "' order by EFFDT DESC", "")
						.firstRow();
				if (notNull(spCharRLoad)) {
					nominalTranformPower = spCharRLoad
							.getAdhocCharacteristicValue();
					logger.info("Nominal Transformer Power"
							+ nominalTranformPower);
					ServicePointCharacteristicD1 spCharTrClass = (ServicePointCharacteristicD1) servicePoint
							.getCharacteristics()
							.createFilter(
									"where CHAR_TYPE_CD = '"
											+ transformerClassCharType
											+ "' order by EFFDT DESC", "")
							.firstRow();
					if (notNull(spCharTrClass)) {
						transformerClass = spCharTrClass
								.getCharacteristicValue();
						logger.info("Transformer Class" + transformerClass);
					} else {
						return;
					}
				} else {
					return;
				}
			} else {
				return;
			}
			if (notNull(systemVoltage) && notNull(nominalTranformPower)
					&& notNull(transformerClass)) {
				// Retrieve Loss Rate Per Hour
				BusinessObjectInstance masterConfgiBOInstance = BusinessObjectInstance
						.create(DEDAS_TRNSF_LOSS_MASTER_CONFIG_BO);
				masterConfgiBOInstance.set("bo",
						DEDAS_TRNSF_LOSS_MASTER_CONFIG_BO);
				masterConfgiBOInstance = BusinessObjectDispatcher.read(
						masterConfgiBOInstance, true);

				if (isNull(masterConfgiBOInstance)) {
					return;
				}
				Element voltageTransformersGroup = masterConfgiBOInstance
						.getElement().element(VOLTAGE_TRANSFORMERS);
				if (isNull(voltageTransformersGroup)) {
					return;
				}
				List<Element> voltageTransformerList = voltageTransformersGroup
						.elements();
				if (isNull(voltageTransformerList)
						|| voltageTransformerList.isEmpty()) {
					return;
				}

				Iterator<Element> voltageTransformerListIterator = voltageTransformerList
						.iterator();
				while (voltageTransformerListIterator.hasNext()) {

					Element voltageTransformer = voltageTransformerListIterator
							.next();
					Element systemVoltageBO = voltageTransformer
							.element(SYSTEM_VOLTAGE);
					Double systemVolt = Double.valueOf(systemVoltage);
					Double masterBOSystemVolt = Double.valueOf(systemVoltageBO
							.getData().toString());
					if ((systemVolt.compareTo(masterBOSystemVolt) <= 0)
							&& !((systemVolt > UNTIL_SYSTEM_VOLT_VAL) && (systemVolt < SYSTEM_VOLT_VAL))) {
						// Element nominalTransformerPower =
						// voltageTransformer.element(NOMINAL_TRNSF_PWR);
						BigDecimal nominalTransformerPowerBO = new BigDecimal(
								voltageTransformer.element(NOMINAL_TRNSF_PWR)
										.getData().toString());
						if (nominalTransformerPowerBO.compareTo(new BigDecimal(
								nominalTranformPower)) == 0) {
							if (transformerClass.contains("A-CLASS")) {
								lossPower = new BigDecimal(voltageTransformer
										.element(A_CLASS).getData().toString());
							} else {
								lossPower = new BigDecimal(voltageTransformer
										.element(OTHER_CLASS).getData()
										.toString());
							}
						}
					}
				}
				logger.info("Loss Power" + lossPower);
				if (notNull(lossPower)) {

					ListIterator<BusinessObjectInstance> mListIter = this.measurementList
							.listIterator();
					while (mListIter.hasNext()) {

						BusinessObjectInstance targetBO = (BusinessObjectInstance) mListIter
								.next();
						InitialMeasurementData_Id imd = new InitialMeasurementData_Id(
								targetBO.getString(ORIGINAL_IMD));
						DateTime startDateTime = this.inputData
								.getBaseEffectiveDateTime();

						if (isNull(startDateTime)) {
							return;
						}
						DateTime endDateTime = imd.getEntity().getToDateTime();
						logger.info("Start Date Time" + startDateTime);
						logger.info("End Date Time" + endDateTime);

						if (startDateTime.compareTo(endDateTime) <= 0) {
							TimeOfUseD1 currentTOU = this.measuringComponent
									.getMeasuringComponentType()
									.determinePrimaryMeasurementValueIdentifier()
									.fetchTimeOfUse();
							//If TOU Customer
							if (notNull(currentTOU)) {

								sec = BigDecimal.valueOf(endDateTime
										.difference(startDateTime)
										.getAsSeconds());

								BigDecimal days = BigDecimal
										.valueOf(endDateTime
												.getDate()
												.difference(
														startDateTime.getDate()).getTotalDays());
								logger.info("Days" + days);

								readingHours = retrieveReadingHours(days,startDateTime,endDateTime,installEvent,currentTOU);
								logger.info("Transfor Reading Hours"+readingHours);
								
								//Retrieve Off Electricity Hours
								if (!(readingHours.equals(BigDecimal.ZERO))) {
									
									HashMap<DateTime, DateTime> offelectricMap = retrieveOffElectric(
											startDateTime, endDateTime, installEvent);
									Iterator<Entry<DateTime, DateTime>> mapIterator = offelectricMap.entrySet().iterator();
									
									while(mapIterator.hasNext()){
										 Entry<DateTime, DateTime> entry = (Entry) mapIterator.next();
										 
										 DateTime entryStartDateTime = entry.getKey();
										 DateTime entryEndDateTime = entry.getValue();
										 
										 logger.info("entryStartDateTime"+entryStartDateTime);
										 logger.info("entryEndDateTime"+entryEndDateTime);
										 
										 BigDecimal entryDays = BigDecimal
													.valueOf(entryEndDateTime
															.getDate()
															.difference(
																	entryStartDateTime.getDate()).getTotalDays());
										 logger.info("Entry Days" + entryDays);
										 
										 offElectricHours = offElectricHours.add(retrieveReadingHours(entryDays,entryStartDateTime,entryEndDateTime,installEvent,currentTOU));
										 logger.info("entryOffElectricHours"+offElectricHours);
									}
								}
								//Subtract Off electricity Hours
								readingHours = readingHours.subtract(offElectricHours);

							} else {
								//Non TOU Customer
								logger.info("Non TOU Customer");
								BigDecimal difference = BigDecimal.ZERO;
								sec = BigDecimal.valueOf(endDateTime
										.difference(startDateTime)
										.getAsSeconds());
								readingHours = sec.divide(new BigDecimal(3600),
										mathContext);
								logger.info("Non TOU Customer Reading Hours"+readingHours);
								//Retrieve Off Electric Hours 
								if (!(readingHours.equals(BigDecimal.ZERO))) {
									
									HashMap<DateTime, DateTime> offelectricMap = retrieveOffElectric(
											startDateTime, endDateTime, installEvent);
									BigDecimal noOfHours = BigDecimal.ZERO;
									
									for (Iterator var15 = offelectricMap.entrySet().iterator(); var15
											.hasNext(); noOfHours = noOfHours.add(roundingHelper.scaleValue(
											"D1MAX_NBR_FOR_COMPUTATION", difference), mathContext)) {
										Entry<DateTime, DateTime> entry = (Entry) var15.next();
										difference = BigDecimal.valueOf(
												((DateTime) entry.getValue()).difference(
														(DateTime) entry.getKey()).getAsSeconds()).divide(
												BigDecimal.valueOf(3600L), mathContext);
									}
									logger.info("No Of Hours" + noOfHours);
									offElectricHours = roundingHelper.scaleValue("D1MAX_NBR_FOR_COMPUTATION", noOfHours);									
									
								}
								
								//Subtract Off electricity Hours
								readingHours = readingHours.subtract(offElectricHours);
							}
						} else {
							return;
						}
						
						logger.info("Reading Hours" + readingHours);
						
						// Retrive Transformer Loss
						transformerLoss = lossPower.multiply(readingHours,
								mathContext);
						logger.info("Main Transformer Loss" + transformerLoss);
						// Multiple Customer Logic
						// List of Service Points
						subTransformerLoss = retrieveSubTransformerLoss(facilityId,servicePoint);
						
						if(notNull(subTransformerLoss)){
							targetBO.set(this.targetElement, this.rh
									.scaleValue(this.fieldName,
											subTransformerLoss));
							logger.info("Sub Transformer Loss"
									+ subTransformerLoss);
						} else {
							// logger.info("transformer Loss"+transformerLoss);
							targetBO.set(this.targetElement,
									this.rh.scaleValue(this.fieldName,
											transformerLoss));
					
							logger.info("Main Transformer Loss"
									+ transformerLoss);
						}

					}
				}

			} else
				return;

		}
		// Device Not Installed
		return;
	}

	private BigDecimal retrieveSubTransformerLoss(Facility_Id facilityId, ServicePointD1 servicePoint) {
		BigDecimal installPower = null;
		BigDecimal installPowerCurrent = null;
		BigDecimal sumInstallPower = BigDecimal.ZERO;
		
		List<SQLResultRow> multipleCustomerSpList = retriveMultipleServicePoints(facilityId);

		if ((notNull(multipleCustomerSpList))
				&& (multipleCustomerSpList.size() > 1)) {

			// Current SP Install Power
			ServicePointCharacteristicD1 installPwrCurrentSp = (ServicePointCharacteristicD1) servicePoint
					.getCharacteristics()
					.createFilter(
							"where CHAR_TYPE_CD = 'CM-RLOAD' order by EFFDT DESC",
							"").firstRow();
			if (notNull(installPwrCurrentSp)) {
				installPowerCurrent = new BigDecimal(
						installPwrCurrentSp
								.getAdhocCharacteristicValue());
			}

			Iterator<SQLResultRow> multipleCustomerSpListItr = retriveMultipleServicePoints(
					facilityId).iterator();

			while (multipleCustomerSpListItr.hasNext()) {
				SQLResultRow multipleSpRow = multipleCustomerSpListItr
						.next();
				ServicePointD1 multipleSp = new ServicePointD1_Id(
						multipleSpRow.getString("D1_SP_ID"))
						.getEntity();
				ServicePointCharacteristicD1 installPowerChar = (ServicePointCharacteristicD1) multipleSp
						.getCharacteristics()
						.createFilter(
								"where CHAR_TYPE_CD = 'CM-RLOAD' order by EFFDT DESC",
								"").firstRow();
				if (notNull(installPowerChar)) {
					installPower = new BigDecimal(
							installPowerChar
									.getAdhocCharacteristicValue());
					sumInstallPower = sumInstallPower.add(
							installPower, mathContext);
				} else
					return null;
			}
			logger.info("Install Sum Power" + sumInstallPower);
			BigDecimal ratio = sumInstallPower.divide(
					installPowerCurrent, mathContext);
			logger.info("Ratio" + ratio);
			subTransformerLoss = transformerLoss.divide(ratio,
					mathContext);
			if(notNull(subTransformerLoss)){
				return subTransformerLoss;
			}
			
		   return null;
		}
		  return null;
	}

	private BigDecimal retrieveReadingHours(BigDecimal days, DateTime startDateTime, DateTime endDateTime, InstallEvent installEvent, TimeOfUseD1 currentTOU) {
		
		BigDecimal touHours;
		
		// Less Than one day
		if (days.compareTo(BigDecimal.ZERO) <= 0) {
			BigDecimal touTempHours = retrieveTOUHours(
					startDateTime, currentTOU,
					BigDecimal.ZERO,
					startDateTime.getTime(),
					endDateTime.getTime(),installEvent);
			touHours = touTempHours;
			logger.info("touCase1Hours" + touTempHours);

		}
		// Greater than or equal to one day and less
		// than 2 days
		else if ((days.compareTo(BigDecimal.ONE) >= 0)
				&& ((days.compareTo(new BigDecimal(2))) < 0)) {
			BigDecimal touTempHoursStart = retrieveTOUHours(
					startDateTime, currentTOU,
					BigDecimal.ZERO,
					startDateTime.getTime(), new Time(
							23, 59, 59),installEvent);
			
			logger.info("touCase2Hours Start"
					+ touTempHoursStart);
			
			BigDecimal touTempHoursEnd = retrieveTOUHours(
					endDateTime, currentTOU,
					BigDecimal.ZERO, new Time(0, 0, 0),
					endDateTime.getTime(),installEvent);
			touHours = touTempHoursStart.add(touTempHoursEnd);
			
			logger.info("touCase2Hours End"
					+ touTempHoursEnd);

		} else {
			BigDecimal touTempHoursStart = retrieveTOUHours(
					startDateTime, currentTOU,
					BigDecimal.ZERO,
					startDateTime.getTime(), new Time(
							23, 59, 59),installEvent);
			
			logger.info("touCase3Hours Start"
					+ touTempHoursStart);
			
			BigDecimal touTempHoursEnd = retrieveTOUHours(
					endDateTime, currentTOU,
					BigDecimal.ZERO, new Time(0, 0, 0),
					endDateTime.getTime(),installEvent);
			
			logger.info("touCase3Hours End"
					+ touTempHoursEnd);
			
			BigDecimal middleDays = days.subtract(BigDecimal.ONE);
			
			BigDecimal touTempHoursMiddle = retrieveTOUHours(
					startDateTime, currentTOU,
					middleDays,
					startDateTime.getTime(), new Time(
							23, 59, 59),installEvent);
			
			logger.info("touCase3Hours Middle"
					+ touTempHoursMiddle);
			
			touHours = touTempHoursStart.add(touTempHoursEnd).add(middleDays.multiply(touTempHoursMiddle, mathContext));
			logger.info("Reading Hours Middle"+touHours);
				
			}		
		return touHours;
	}

	private BigDecimal retrieveTOUHours(DateTime dateTime,
			TimeOfUseD1 currentTOU, BigDecimal days, Time actualStartTime,
			Time actualEndTime, InstallEvent installEvent) {
		
		BigDecimal touHours = null;
		
        if(isNull(startTime) && (isNull(endTime))){
		retrieveTouStartAndEndTime(currentTOU,dateTime);
        }
		

			if ((notNull(startTime) && notNull(endTime)) && (days.compareTo(BigDecimal.ONE) < 0)) {
				try {
					touHours = retrieveTouHoursForDay(startTime, endTime,
							dateTime, actualStartTime,
							actualEndTime,installEvent);
				} catch (DateFormatParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return touHours;

			} else if((notNull(startTime) && notNull(endTime)) && (days.compareTo(BigDecimal.ONE) >= 0)){
				
				if (endTime.compareTo(startTime) < 0) {
					
					touHours = BigDecimal.valueOf(endTime.getHours())
							.add(BigDecimal.valueOf(24))
							.subtract(BigDecimal.valueOf(startTime.getHours()));
					return touHours;
				}
				
				touHours = BigDecimal.valueOf(endTime.difference(startTime)
						.getHours());
				
			
				return touHours;
			}
			return null;
		}
		
	

	private void retrieveTouStartAndEndTime(TimeOfUseD1 currentTOU, DateTime dateTime) {
		DateTime touStartDateTime = null;
		DateTime touEndDateTime = null;
		
		String templateTou = null;
		BigDecimal touHours = null;
		DateTime templateStartDateTime = null;
		DateTime templateEndDateTime = null;

		String currentTouStr = currentTOU.getId().getIdValue();

		TimeOfUseMapTemplateD1_Id touTemplate = new TimeOfUseMapTemplateD1_Id(
				touMapTemplate);
		BusinessObjectInstance boInstance = BusinessObjectInstance
				.create(touTemplate.getEntity().getBusinessObject());
		boInstance.set("touMapTemplate", touMapTemplate);
		boInstance = BusinessObjectDispatcher.read(boInstance);
		logger.info("**********" + boInstance);
		COTSInstanceNode touScheduleGroup = boInstance
				.getGroupFromPath("touSchedule");
		COTSInstanceList weekRangeList = touScheduleGroup
				.getList("weekRangeList");
		Iterator itrWeekRange = weekRangeList.iterator();

		COTSInstanceListNode listWeekRow = null;
		COTSInstanceListNode listWeekDayRow = null;
		COTSInstanceListNode listHourRangeRow = null;

		while (itrWeekRange.hasNext()) {

			listWeekRow = (COTSInstanceListNode) itrWeekRange.next();

			if (listWeekRow != null) {
				String stDt = listWeekRow.getString("startDate");
				String enDt = listWeekRow.getString("endDate");
				logger.info("startDate" + stDt);
				logger.info("endDate" + enDt);

				// Build Start Date Time
				touStartDateTime = buildDateTime(stDt, dateTime.getYear());
				// Build End Date Time
				touEndDateTime = buildDateTime(enDt, dateTime.getYear());
				logger.info("TOU Start Date Time" + touStartDateTime);
				logger.info("TOU End Date Time" + touEndDateTime);

				if ((dateTime.isSameOrAfter(touStartDateTime))
						&& (dateTime.isSameOrBefore(touEndDateTime))) {

					logger.info("Inside Second List");
					COTSInstanceList weekDayRangeList = listWeekRow
							.getList("weekDayRangeList");

					Iterator itrWeekDay = weekDayRangeList.iterator();
					while (itrWeekDay.hasNext()) {
						listWeekDayRow = (COTSInstanceListNode) itrWeekDay
								.next();
						if (listWeekDayRow != null) {

							String startWeekDay = listWeekDayRow
									.getLookup("startWeekDay").getLookupValue()
									.fetchIdFieldValue();
							String endWeekDay = listWeekDayRow
									.getLookup("endWeekDay").getLookupValue()
									.fetchIdFieldValue();

							int startWeekdayOption = Integer
									.parseInt(startWeekDay
											.substring(startWeekDay.length() - 1));
							int endWeekdayOption = Integer.parseInt(endWeekDay
									.substring(endWeekDay.length() - 1));
							int currentWeekOption = dateTime.getDayOfWeek()
									.getDayNumber();

							if ((currentWeekOption >= startWeekdayOption)
									&& (currentWeekOption <= endWeekdayOption)) {

								logger.info("Inside Third List");

								COTSInstanceList hourRangeList = listWeekDayRow
										.getList("hourRangeList");
								Iterator itrHourRange = hourRangeList
										.iterator();

								while (itrHourRange.hasNext()) {
									listHourRangeRow = (COTSInstanceListNode) itrHourRange
											.next();
									templateTou = listHourRangeRow
											.getString("tou");
									logger.info("template TOU" + templateTou);
									logger.info("Current TOU" + currentTouStr);

									if (templateTou
											.contentEquals(currentTouStr)) {
										startTime = listHourRangeRow
												.getTime("startTime");
										endTime = listHourRangeRow
												.getTime("endTime");

										logger.info("Start Time" + startTime);
										logger.info("End Time" + endTime);
										break;
									}
								}

							}
						}
					}

				}
			}
		}
		
	}

	private BigDecimal retrieveTouHoursForDay(Time startTime, Time endTime,
			DateTime dateTime, Time actualStartTime,
			Time actualEndTime, InstallEvent installEvent) throws DateFormatParseException {

		DateTime touStartDateTime = null;
		DateTime touEndDateTime = null;

		DateTime templateStartDateTime = null;
		DateTime templateEndDateTime = null;

		BigDecimal touHours = null;

		try {
			templateStartDateTime = dateFormat.parseDateTime(dateTime.getYear()
					+ "-" + dateTime.getMonth() + "-" + dateTime.getDay() + "-"
					+ startTime.getHours() + ":" + startTime.getMinutes() + ":"
					+ startTime.getSeconds());
			templateEndDateTime = dateFormat.parseDateTime(dateTime.getYear()
					+ "-" + dateTime.getMonth() + "-" + dateTime.getDay() + "-"
					+ endTime.getHours() + ":" + endTime.getMinutes() + ":"
					+ endTime.getSeconds());
			touStartDateTime = dateFormat.parseDateTime(dateTime.getYear()
					+ "-" + dateTime.getMonth() + "-" + dateTime.getDay() + "-"
					+ actualStartTime.getHours() + ":"
					+ actualStartTime.getMinutes() + ":"
					+ actualStartTime.getSeconds());
			touEndDateTime = dateFormat.parseDateTime(dateTime.getYear() + "-"
					+ dateTime.getMonth() + "-" + dateTime.getDay() + "-"
					+ actualEndTime.getHours() + ":"
					+ actualEndTime.getMinutes() + ":"
					+ actualEndTime.getSeconds());

		} catch (DateFormatParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (templateEndDateTime.compareTo(templateStartDateTime) < 0) {
			Time slotTime1 = new Time(23, 59, 59);
			Time slotTime2 = new Time(0, 0, 0);
			DateTime templateEndDateTime1 = dateFormat.parseDateTime(dateTime
					.getYear()
					+ "-"
					+ dateTime.getMonth()
					+ "-"
					+ dateTime.getDay()
					+ "-"
					+ slotTime1.getHours()
					+ ":"
					+ slotTime1.getMinutes() + ":" + slotTime1.getSeconds());
			BigDecimal hour1 = overlapHours(templateStartDateTime,
					templateEndDateTime1, touStartDateTime, touEndDateTime,
					startTime, slotTime1,installEvent);
			
			logger.info("Hour1" + hour1);
			templateStartDateTime = dateFormat.parseDateTime(dateTime.getYear()
					+ "-" + dateTime.getMonth() + "-" + dateTime.getDay() + "-"
					+ slotTime2.getHours() + ":" + slotTime2.getMinutes() + ":"
					+ slotTime2.getSeconds());
			
			BigDecimal hour2 = overlapHours(templateStartDateTime,
					templateEndDateTime, touStartDateTime, touEndDateTime,
					slotTime2, endTime,installEvent);
		
			logger.info("Hour2" + hour2);
			touHours = hour1.add(hour2);
		} else {
			touHours = overlapHours(templateStartDateTime, templateEndDateTime,
					touStartDateTime, touEndDateTime, startTime, endTime,installEvent);

		}
		return touHours;
	}

	private BigDecimal overlapHours(DateTime templateStartDateTime,
			DateTime templateEndDateTime, DateTime touStartDateTime,
			DateTime touEndDateTime, Time startTime, Time endTime, InstallEvent installEvent) {
		logger.info("templateStartDateTime" + templateStartDateTime);
		logger.info("templateEndDateTime" + templateEndDateTime);
		logger.info("touStartDateTime" + touStartDateTime);
		logger.info("touEndDateTime" + touEndDateTime);

		BigDecimal touOverLapHours = null;
		if ((templateEndDateTime.compareTo(touStartDateTime) < 0) || (touEndDateTime.compareTo(templateStartDateTime) < 0)) {
			return BigDecimal.ZERO;
		}

		if ((templateStartDateTime.compareTo(touStartDateTime) < 0)) {
			startTime = touStartDateTime.getTime();
			logger.info("Inside change in start Time");
		}

		if (templateEndDateTime.compareTo(touEndDateTime) > 0) {
			endTime = touEndDateTime.getTime();
			logger.info("Inside change in End Time");
		}

		logger.info("Start Time" + startTime);
		logger.info("End Time" + endTime);
		
		MathContext m = new MathContext(2);
		
		logger.info("touOverLapHours"+BigDecimal.valueOf(
				endTime.difference(startTime).getAsSeconds()).divide(
						new BigDecimal(3600)));

		touOverLapHours = BigDecimal.valueOf(
				endTime.difference(startTime).getAsSeconds()).divide(
				new BigDecimal(3600));
		return touOverLapHours;
	}
	
	
	
	private DateTime buildDateTime(String dt, int yr) {
		DateTime buildDateTime = null;
		String[] stringList = dt.split("-");
		int startMonth = Integer.parseInt(stringList[0]);
		int startDay = Integer.parseInt(stringList[1]);
		int year = yr;

		try {
			buildDateTime = dateFormat.parseDateTime(year + "-" + startMonth
					+ "-" + startDay + "-00:00:00");
		} catch (DateFormatParseException e) {
			e.printStackTrace();
		}

		return buildDateTime;
	}

	private HashMap<DateTime, DateTime> retrieveOffElectric(DateTime startDateTime,
			DateTime endDateTime, InstallEvent installEvent) {

		logger.info("Inside Method");

		StringBuffer queryStringBuffer = new StringBuffer();
		queryStringBuffer.append(" from OnOffHistory oh ");
		queryStringBuffer.append(" where oh.id.installEvent =:installEvent ");
		queryStringBuffer.append(" and oh.eventDateTime >= :startDateTime ");
		queryStringBuffer.append(" and oh.eventDateTime < :endDateTime ");
		Query<QueryResultRow> query = this.createQuery(queryStringBuffer
				.toString());
		query.bindEntity("installEvent", installEvent);
		query.bindDateTime("startDateTime", startDateTime);
		query.bindDateTime("endDateTime", endDateTime);
		query.addResult("onOffStatusIndicator", "oh.onOffStatusIndicator");
		query.addResult("eventDateTime", "oh.eventDateTime");
		query.addResult("sequence", "oh.id.sequence");
		query.addResult("boDataArea", "oh.businessObjectDataArea");
		query.orderBy("eventDateTime", -1);

		HashMap<DateTime, DateTime> includedEvents = new HashMap();
		DateTime lastEventEndDateTime = endDateTime;

		Iterator iter = query.list().iterator();

		while (iter.hasNext()) {
			
			String disconnectSource = null;
			
			QueryResultRow row = (QueryResultRow) iter.next();
			OnOffStatusIndicatorLookup currentStatus = (OnOffStatusIndicatorLookup) row
					.getLookup("onOffStatusIndicator",
							OnOffStatusIndicatorLookup.class);
			DateTime currentEventDateTime = row.getDateTime("eventDateTime");
			BigInteger sequence = row.getInteger("sequence");
			
			//Retrive Disconnect Source
			
			if(currentStatus.compareTo(OnOffStatusIndicatorLookup.constants.OFF) == 0 && notBlank(row.getString("boDataArea"))){
				try {
					Document boDataArea = Dom4JHelper.parseText("<root>"
							+ row.getString("boDataArea") + "</root>");
					
					disconnectSource = boDataArea.getRootElement().element(DISCONNECT_SOURCE).getText();
					logger.info("disconnectSource"+disconnectSource);
					
				} catch (DocumentException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if (notNull(disconnectSource) && DisconnectSourceFlagLookup.constants.CUSTOMER_REQUEST.getLookupValue().fetchIdFieldValue().equals(disconnectSource)){
					includedEvents.put(currentEventDateTime, lastEventEndDateTime);
				} 
				
			}
			else {
				lastEventEndDateTime = currentEventDateTime;
			}
				
				
		    
		}
		logger.info("HashMap"+includedEvents.entrySet());

		return includedEvents;

	}

	private List<SQLResultRow> retriveMultipleServicePoints(
			Facility_Id facilityId) {

		StringBuilder queryString = new StringBuilder();
		queryString.append("SELECT D1_SP_ID FROM D1_SP_FACILITY ");
		queryString.append("WHERE FACILITY_ID = '" + facilityId.getIdValue()
				+ "'");
		PreparedStatement statement = createPreparedStatement(
				queryString.toString(), "Retrieve Service Points");
		List<SQLResultRow> resultRow = statement.list();
		if (notNull(resultRow)) {
			return resultRow;
		}
		return null;

	}

	@Override
	public ValueDerivationInputOutputData getValueDerivationInputOutputData() {

		return this.inputOutputData;
	}

	@Override
	public void setValueDerivationInputData(ValueDerivationInputData input) {

		this.inputData = input;
	}

	@Override
	public void setValueDerivationInputOutputData(
			ValueDerivationInputOutputData inputOutput) {

		this.inputOutputData = inputOutput;

	}

}
