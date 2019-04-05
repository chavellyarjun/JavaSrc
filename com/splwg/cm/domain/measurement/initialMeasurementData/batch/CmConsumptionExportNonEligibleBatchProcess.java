package com.splwg.cm.domain.measurement.initialMeasurementData.batch;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.QueryIterator;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.businessObject.COTSInstanceNode;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.database.field.Field_Id;
import com.splwg.cm.api.lookup.ConsumptionTypeFlagLookup;
import com.splwg.d1.api.lookup.IntervalScalarLookup;
import com.splwg.d1.domain.admin.measuringComponentType.entities.MeasuringComponentTypeValueIdentifier;
import com.splwg.d1.domain.deviceManagement.deviceConfiguration.entities.DeviceConfiguration;
import com.splwg.d1.domain.deviceManagement.measuringComponent.entities.MeasuringComponent;
import com.splwg.d1.domain.installation.installEvent.entities.InstallEvent;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointD1_Id;
import com.splwg.d1.domain.measurement.initialMeasurementData.entities.InitialMeasurementData;
import com.splwg.d1.domain.measurement.measurement.entities.Measurement;
import com.splwg.base.api.datatypes.DateFormat;
import com.splwg.base.api.datatypes.DateFormatParseException;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

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
public class CmConsumptionExportNonEligibleBatchProcess extends
CmConsumptionExportNonEligibleBatchProcess_Gen {

	private static Logger logger = LoggerFactory.getLogger(CmConsumptionExportNonEligibleBatchProcess.class);

	public JobWork getJobWork() {
		// TODO Auto-generated method stub
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		StringBuilder queryString = new StringBuilder();
		queryString.append("SELECT DISTINCT SPCHAR.ADHOC_CHAR_VAL ");
		queryString.append("FROM D1_SP_IDENTIFIER SPI,D1_SP_CHAR SPCHAR,CI_PREM_CHAR PREMCHAR ");
		queryString.append("WHERE PREMCHAR.CHAR_TYPE_CD = 'CM-ELGPR' ");
		queryString.append("AND PREMCHAR.CHAR_VAL = 'N' ");
		queryString.append("AND PREMCHAR.PREM_ID = SPI.ID_VALUE ");
		queryString.append("AND SPI.D1_SP_ID = SPCHAR.D1_SP_ID ");
		queryString.append("AND SPCHAR.CHAR_TYPE_CD = 'CM-ETSO' ");

		PreparedStatement statement = createPreparedStatement(queryString.toString(),"Retrieve Metering Point EIC");
		try {
			List<SQLResultRow> resultsList = statement.list();
			if(isNull(resultsList)){
				logger.info("Their is no Metering Poit EIC Code");
			}
			for (SQLResultRow resultRow : resultsList) {
				Field_Id id = new Field_Id(resultRow.getString("ADHOC_CHAR_VAL"));
				ThreadWorkUnit unit = new ThreadWorkUnit(id);
				threadWorkUnitList.add(unit);
				logger.info("Worl Unit"+unit.getPrimaryId());
			}              
		} 
		finally {
			if (notNull(statement)) {
				statement.close();
			}
		}
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	public Class<CmConsumptionExportNonEligibleBatchProcessWorker> getThreadWorkerClass() {
		return CmConsumptionExportNonEligibleBatchProcessWorker.class;
	}

	public static class CmConsumptionExportNonEligibleBatchProcessWorker extends
	CmConsumptionExportNonEligibleBatchProcessWorker_Gen {

		private static String requestType = null;
		private static BigInteger consumptionHorizonDays = null;
		private static Date periodEndDate = null;
		private static DateTime lastMeasureDate = null;
		private static DateFormat dataFormat = new DateFormat("yyyy-MM-dd-HH:mm:ss");
		static DateTime tempStartDateTime = null;
		static DateTime firstMeasureDate = null;
		static DateTime consumptionExtractDttm =  null;
		static Boolean consumptionRetrieved = false;
		static BusinessObjectInstance spBoInstance =null;

		public ThreadExecutionStrategy createExecutionStrategy() {
			// TODO Auto-generated method stub
			return new StandardCommitStrategy(this);
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			// TODO Auto-generated method stub
			logger.info("Inside Execute Work Unit"+unit.getPrimaryId());
			int offset = unit.getPrimaryId().toString().length();
			String meteringPointEIC = unit.getPrimaryId().toString().substring(9, offset-1);
			logger.info("Matering Point EIC"+meteringPointEIC);

			BigDecimal dayTimeConsumption = BigDecimal.ZERO;
			BigDecimal nightTimeConsumption = BigDecimal.ZERO;
			BigDecimal peakConsumption = BigDecimal.ZERO;

			requestType = getParameters().getRequestType();
			consumptionHorizonDays = getParameters().getConsumptionImportHorizonDays();

			StringBuilder query = new StringBuilder();
			query.append("SELECT DISTINCT SPI.D1_SP_ID  FROM ");
			query.append("D1_SP_IDENTIFIER SPI,D1_SP_CHAR SPCHAR,CI_PREM_CHAR PREMCHAR ");
			query.append("WHERE PREMCHAR.CHAR_TYPE_CD = 'CM-ELGPR' ");
			query.append("AND PREMCHAR.CHAR_VAL = 'N' ");
			query.append("AND PREMCHAR.PREM_ID = SPI.ID_VALUE ");
			query.append("AND SPI.D1_SP_ID = SPCHAR.D1_SP_ID ");
			query.append("AND SPCHAR.CHAR_TYPE_CD = 'CM-ETSO' ");
			query.append("AND SPCHAR.ADHOC_CHAR_VAL = '"+meteringPointEIC+"'");

			PreparedStatement statement = createPreparedStatement(query.toString(),"Retrieve SP ID");
			logger.info("Statement"+statement);
			try{
				QueryIterator<SQLResultRow> spIds = statement.iterate();

				while(spIds.hasNext()){
					SQLResultRow spIdRow = spIds.next();
					ServicePointD1_Id spId = new ServicePointD1_Id(spIdRow.getString("D1_SP_ID"));
					logger.info("Service Point ID"+spId.getIdValue());
					InstallEvent installEvent = spId.getEntity().getCurrentDeviceInstallation(getSystemDateTime());
					if(notNull(installEvent))
					{                
						DeviceConfiguration deviceConfig = installEvent.getDeviceConfiguration(); 
						List<MeasuringComponent> scalarMcList = deviceConfig.retrieveMeasuringComponents(IntervalScalarLookup.constants.SCALAR);
						spBoInstance = BusinessObjectInstance.create("X1D-ServicePoint");//consumptionNettingDate
						spBoInstance.set("spId", spId.getIdValue());
						spBoInstance = BusinessObjectDispatcher.read(spBoInstance);
						consumptionExtractDttm = spBoInstance.getDateTime("consumptionExportDttm");
						if(isNull(consumptionExtractDttm)){
							consumptionExtractDttm = spBoInstance.getDateTime("creationDateTime");
						}
						firstMeasureDate = consumptionExtractDttm;
						periodEndDate = getProcessDateTime().getDate();
						int year =  periodEndDate.getYear();
						int month = periodEndDate.getMonth();
						int days = periodEndDate.getMonthValue().getFirstDayOfMonth().getDay();
						try {
							lastMeasureDate = dataFormat.parseDateTime(year+"-"+month+"-"+days+"-00:00:00");
						}              
						catch (DateFormatParseException e) { 
							e.printStackTrace();
						} 
						if (notNull(consumptionHorizonDays)
								&& consumptionHorizonDays.compareTo(BigInteger.ZERO) > 0) {
							tempStartDateTime = lastMeasureDate.addDays(-consumptionHorizonDays.intValue());
							logger.info("tempStartDateTime "+tempStartDateTime);
							if (notNull(consumptionExtractDttm)) {
								if (tempStartDateTime.isBefore(consumptionExtractDttm)) {
									firstMeasureDate = tempStartDateTime;
								}
							}
						} 

						logger.info("First Measure Date"+firstMeasureDate);
						logger.info("Last Measure Date"+ lastMeasureDate);
						if(firstMeasureDate.compareTo(lastMeasureDate) < 0)
						{
							if(notNull(scalarMcList))
							{
								Iterator<MeasuringComponent> scalarMcListIterator = scalarMcList.iterator();
								while(scalarMcListIterator.hasNext()){
									MeasuringComponent scalarMc = scalarMcListIterator.next();
									logger.info("Measuring Component"+scalarMc+"Scalar MC List size"+scalarMcList.size());
									if(notNull(scalarMc.getInitialMeasurementDataForPeriod(firstMeasureDate, lastMeasureDate)))
									{
										Iterator<InitialMeasurementData> imdIterator = scalarMc.getInitialMeasurementDataForPeriod(firstMeasureDate, lastMeasureDate).iterator();
										logger.info("IMD Size"+scalarMc.getInitialMeasurementDataForPeriod(firstMeasureDate, lastMeasureDate).size());

										while(imdIterator.hasNext())
										{
											InitialMeasurementData imd = imdIterator.next();
											logger.info("imd.."+imd.getId().getIdValue());
											if(notNull(imd.retrieveMeasurements(Boolean.TRUE)))
											{
												Iterator<Measurement> measurementIterator = imd.retrieveMeasurements(Boolean.TRUE).iterator();
												while(measurementIterator.hasNext())
												{
													Measurement msrmt = measurementIterator.next();
													if(notNull(msrmt)){
														consumptionRetrieved = true;
														MeasuringComponentTypeValueIdentifier mcValIdentifier = scalarMc.getMeasuringComponentType().determinePrimaryMeasurementValueIdentifier();
														String tou = mcValIdentifier.fetchTimeOfUse().getId().getIdValue();
														if (tou.contentEquals("T1")) {
															if(isNull(dayTimeConsumption)){
																dayTimeConsumption = msrmt.getMeasurementValue();
															}else{
																dayTimeConsumption = dayTimeConsumption.add(msrmt.getMeasurementValue());
															}

														}
														else if (tou.contentEquals("T2")) {
															if(isNull(nightTimeConsumption)){
																nightTimeConsumption = msrmt.getMeasurementValue();
															}else{
																nightTimeConsumption = nightTimeConsumption.add(msrmt.getMeasurementValue());
															}
														}
														else if (tou.contentEquals("T3")){ 
															if(isNull(peakConsumption)){
																peakConsumption = msrmt.getMeasurementValue();
															}else{
																peakConsumption = peakConsumption.add(msrmt.getMeasurementValue());
															}
														}
													}
													else
													{
														logger.info("No Measurement for this date");
													}
												}
											}
										}
									}                                                        
								}
							}
						}
					}
				}
				if(consumptionRetrieved){
					spBoInstance.set("consumptionExportDttm",lastMeasureDate);
					spBoInstance = BusinessObjectDispatcher.update(spBoInstance);
					logger.info("Updated SP BO"+spBoInstance.getDocument().asXML());
					invokeConsumptionAddRequestBO(dayTimeConsumption,nightTimeConsumption,peakConsumption,meteringPointEIC);                                                                                
				}
				logger.info("dayTimeConsumption"+dayTimeConsumption);
				logger.info("nightTimeConsumption"+nightTimeConsumption);
				logger.info("peakConsumption"+peakConsumption);

			}              finally {
				if (notNull(statement)) {
					statement.close();
				}
			}

			return true;
		}

		private void invokeConsumptionAddRequestBO(BigDecimal dayTimeConsumption, BigDecimal nightTimeConsumption, BigDecimal peakConsumption, String meteringPointEIC){
			BusinessObjectInstance boInstance = BusinessObjectInstance.create("CM-DEDASConExpRequest");
			boInstance.set("bo", "CM-DEDASConExpRequest");
			boInstance.set("requestType",requestType);
			boInstance.set("consumptionType",ConsumptionTypeFlagLookup.constants.CONSUMPTION);
			COTSInstanceNode sendDetailsGroup = boInstance.getGroupFromPath("sendDetails");
			COTSInstanceNode consumptionGroup = sendDetailsGroup.getGroupFromPath("consumption");
			consumptionGroup.set("meteringPointEIC",meteringPointEIC);
			consumptionGroup.set("setlementPeriod",firstMeasureDate);
			COTSInstanceNode consumptionDataGroup = consumptionGroup.getGroupFromPath("consumptionData");
			consumptionDataGroup.set("dayTimeConsumption",dayTimeConsumption);
			consumptionDataGroup.set("nightTimeConsumption",nightTimeConsumption);
			consumptionDataGroup.set("peakConsumption",peakConsumption);
			logger.info(boInstance.getDocument().asXML());
			BusinessObjectDispatcher.add(boInstance);


		}

	}

}
