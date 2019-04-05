package com.splwg.cm.domain.admin.veeRule.applyVeeRule;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.businessObject.COTSInstanceList;
import com.splwg.base.api.businessObject.COTSInstanceListNode;
import com.splwg.base.api.businessObject.COTSInstanceNode;
import com.splwg.base.api.businessObject.SchemaInstance;
import com.splwg.base.api.businessService.BusinessServiceDispatcher;
import com.splwg.base.api.businessService.BusinessServiceInstance;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.cm.domain.CMOnSpotBillingWarningMessagesRepository;
import com.splwg.d1.api.lookup.ExceptionSeverityLookup;
import com.splwg.d1.api.lookup.IntervalScalarLookup;
import com.splwg.d1.domain.admin.exceptionType.entities.ExceptionType;
import com.splwg.d1.domain.admin.measuringComponentType.entities.MeasuringComponentType;
import com.splwg.d1.domain.admin.veeRule.applyVeeRule.ApplyVeeRuleAlgorithmInputData;
import com.splwg.d1.domain.admin.veeRule.applyVeeRule.ApplyVeeRuleAlgorithmInputOutputData;
import com.splwg.d1.domain.admin.veeRule.applyVeeRule.ApplyVeeRuleAlgorithmSpot;
import com.splwg.d1.domain.admin.veeRule.applyVeeRule.VeeRuleExceptionData;
import com.splwg.d1.domain.admin.veeRule.applyVeeRule.VeeRuleTraceData;
import com.splwg.d1.domain.admin.veeRule.applyVeeRule.VeeRuleVariablesData;
import com.splwg.d1.domain.common.data.MdmMessageData;
import com.splwg.d1.domain.common.routines.ServerMessageToMdmMessageConverter;
import com.splwg.d1.domain.deviceManagement.deviceConfiguration.entities.DeviceConfiguration;
import com.splwg.d1.domain.deviceManagement.measuringComponent.entities.MeasuringComponent;
import com.splwg.d1.domain.deviceManagement.measuringComponent.entities.MeasuringComponent_Id;
import com.splwg.d1.domain.measurement.initialMeasurementData.routines.ScalarImdDataRetrieveInputData;
import com.splwg.d1.domain.measurement.initialMeasurementData.routines.ScalarImdDataRetriever;
import com.splwg.d1.domain.measurement.initialMeasurementData.routines.ScalarImdDataRetrieverOutputData;
import com.splwg.shared.common.ServerMessage;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author 
 *  Algorithm ID: CM-INDXBCWCK
 * 
 * History:
 * 
 * Date			By				Reason
 * 
 * 25-03-2019				On Spot Billing VEE Rule - Scalar
 * 	
 * @AlgorithmComponent ()
 */
public class CMIndexBackwardValidationVEERule_Impl extends CMIndexBackwardValidationVEERule_Gen implements ApplyVeeRuleAlgorithmSpot {

	//logger
	private Logger logger = LoggerFactory.getLogger(CMIndexBackwardValidationVEERule_Impl.class);

	private final String TO_DATE_TIME = "toDateTime";
	//private final String PREVEE_GRP = "preVEE";
	private static final String POSTVEE_GRP = "postVEE";
	private static final String END_DTTM = "enDt";
	private static final String MEASR_COMP_ID = "measuringComponentId";
	private static final String INIT_MSRMT_DATA_ID = "initialMeasurementDataId";
	private static final String CONSUMPTION_QUANTITY = "q";
	private static final String MSRMT_GRP = "msrs";
	private static final String MSRMT_LIST = "mL";
	private static final String INDEX_BACKWARD_TOU_EXCP = "indexBackwardTOUException";
	private static final String EXCEPTION_TYPE = "exceptionType";
	private static final String EXCEPTION_SEVERITY = "exceptionSeverity";
	private static final String PRIOR_MEASUREMENT_DTTM = "PRIOR_MEASUREMENT_DTTM";



	private ApplyVeeRuleAlgorithmInputData applyVeeRuleAlgorithmInputData = null;
	private ApplyVeeRuleAlgorithmInputOutputData applyVeeRuleAlgorithmInputOutputData = null;
	private List<VeeRuleVariablesData> veeRuleVariablesDataList = new ArrayList<VeeRuleVariablesData>();
	private List<VeeRuleTraceData> veeRuleTraceList;


	@Override
	public void invoke() {
		BusinessObjectInstance veeRuleInstance = getVEERuleBoInstance();
		SchemaInstance imdBoInstance = applyVeeRuleAlgorithmInputOutputData.getVeeInitialMeasurementData();	
		String initialMeasurementDataId = imdBoInstance.getXMLString(INIT_MSRMT_DATA_ID);
		String measuringCompId = imdBoInstance.getString(MEASR_COMP_ID);
		MeasuringComponent mcEntity = new MeasuringComponent_Id(measuringCompId).getEntity();
		if (isNull(mcEntity)) {
			logger.error(" MC not found " + initialMeasurementDataId);
			return;
		}
		MeasuringComponentType mcTypeForIncommingIMD = mcEntity.getMeasuringComponentType();
		DateTime imdEndDateTime = imdBoInstance.getDateTime(TO_DATE_TIME);
		COTSInstanceNode postVeeGroup = imdBoInstance.getGroupFromPath(POSTVEE_GRP);
		if(isNull(imdEndDateTime)) {
			imdEndDateTime = postVeeGroup.getDateTime(END_DTTM);
		}


		BigDecimal consQuantityFromIncommingIMD = getScalarConsumption(postVeeGroup, mcEntity, imdEndDateTime);
		if(consQuantityFromIncommingIMD.compareTo(BigDecimal.ZERO) < 0) {
			COTSInstanceNode group = veeRuleInstance.getGroupFromPath(INDEX_BACKWARD_TOU_EXCP);
			ExceptionType indexBackwardExceptionType = group.getEntity(EXCEPTION_TYPE, ExceptionType.class);
			ExceptionSeverityLookup indexBackwardExceptionSeverity = (ExceptionSeverityLookup) group.getLookup(EXCEPTION_SEVERITY);

			ServerMessage msg = CMOnSpotBillingWarningMessagesRepository.indexBackwardMessage
					(mcTypeForIncommingIMD.fetchLanguageDescription());
			addException(indexBackwardExceptionType,
					indexBackwardExceptionSeverity, msg);

			return;
		} 

		/*-----------------------
		 Received positive IMD
			1. Determine the IMD for other valid MC Type
			2. If those are in Error 
		-----------------------
		 */

		ArrayList<String> validMCTypes = getValidMCTypeFromVEERuleConfig(veeRuleInstance);

		DeviceConfiguration dvcConf = mcEntity.getDeviceConfigurationId().getEntity();

		List<MeasuringComponent> mcList = dvcConf.retrieveMeasuringComponents(IntervalScalarLookup.constants.SCALAR);
		LinkedHashMap<String, String> imdListToComplete = new LinkedHashMap<String, String>();
		for( MeasuringComponent mc : mcList) {
			MeasuringComponentType mcType = mc.getMeasuringComponentType();
			if(validMCTypes.contains(mcType.getId().getIdValue()) && !mcType.getId().equals(mcTypeForIncommingIMD.getId())) {

				StringBuilder imdRetrivalBuilder = new StringBuilder();
				imdRetrivalBuilder.append("select INIT_MSRMT_DATA_ID , BUS_OBJ_CD from D1_INIT_MSRMT_DATA " );
				imdRetrivalBuilder.append(" where BO_STATUS_CD = :exceptionState ");
				imdRetrivalBuilder.append("  and MEASR_COMP_ID = :mcId ");
				imdRetrivalBuilder.append("  and D1_TO_DTTM >= :reportDtFrom ");
				imdRetrivalBuilder.append(" and D1_TO_DTTM <= :reportDtTo" );
				
				PreparedStatement imdRetrivalQuery = createPreparedStatement(imdRetrivalBuilder.toString(), "Query For getting IMD for mc " + mc.getId().getIdValue());
				imdRetrivalQuery.bindString("exceptionState", "VEEEXCP", "BO_STATUS_CD");
				imdRetrivalQuery.bindEntity("mcId", mc);
				imdRetrivalQuery.bindDateTime("reportDtFrom", imdEndDateTime.startOfDay());
				imdRetrivalQuery.bindDateTime("reportDtTo", imdEndDateTime.startOfDay().addDays(1));

				for(SQLResultRow queryResultRow : imdRetrivalQuery.list()) {
					String initialMsrmtId = queryResultRow.getString("INIT_MSRMT_DATA_ID").trim() ;
					String boName = queryResultRow.getString("BUS_OBJ_CD").trim();
					imdListToComplete.put(initialMsrmtId, boName);
				}
				imdRetrivalQuery.close(); // its important to close query early to consider DB time out.
			}
		}

		forceCompleteAllIMD (imdListToComplete);

		applyVeeRuleAlgorithmInputOutputData.setVeeInitialMeasurementData(imdBoInstance);
	}



	private BigDecimal getScalarConsumption(COTSInstanceNode postVeeGroup, MeasuringComponent mcEntity, DateTime imdEndDateTime) {
		BigDecimal consQuantityFromIncommingIMD = null;
		boolean whichOneToExecute = true; //TODO Confirmation with Mithlesh

		if( whichOneToExecute ) {
			COTSInstanceList msrmtListFromPostVEE = postVeeGroup.getGroup(MSRMT_GRP).getList(MSRMT_LIST);
			Iterator<COTSInstanceListNode> msrmtListItrFromPostVEE = msrmtListFromPostVEE.iterator();

			while (msrmtListItrFromPostVEE.hasNext()) {
				COTSInstanceListNode msrmtListNodeFromPostVEE = msrmtListItrFromPostVEE.next();
				consQuantityFromIncommingIMD = msrmtListNodeFromPostVEE.getNumber(CONSUMPTION_QUANTITY);
			}

		} else {

			// ----- OR ---- 

			ScalarImdDataRetriever scalarImdHelper = ScalarImdDataRetriever.Factory.newInstance();
			ScalarImdDataRetrieveInputData preScalarInputData = ScalarImdDataRetrieveInputData.Factory.newInstance();
			preScalarInputData.setMeasuringComponentId(mcEntity.getId().getIdValue());
			preScalarInputData.setEndDateTime(imdEndDateTime);
			ScalarImdDataRetrieverOutputData preScalarOutputData = scalarImdHelper
					.searchPriorMeasurement(preScalarInputData);

			if (isNull(preScalarOutputData) || preScalarOutputData.isFirstReading()
					|| isBlankOrNull(preScalarOutputData.getMeasurementCondition())) {
				setTrace(PRIOR_MEASUREMENT_DTTM, "Not Found");
				consQuantityFromIncommingIMD = BigDecimal.ZERO;
			}

			BigDecimal qtyFromPriorReading = preScalarOutputData.getStartQuantity(); //TODO
			BigDecimal qtyFromCurrentIMD = postVeeGroup.getNumber("enQty");
			consQuantityFromIncommingIMD = qtyFromCurrentIMD.subtract(qtyFromPriorReading);
		}
		return consQuantityFromIncommingIMD;
	}

	/** Force Complete all IMD which are in VEE Exception state
	 * @param imdListToComplete 
	 * @param imdListToComplete.keySet()
	 */
	private void forceCompleteAllIMD (LinkedHashMap<String, String> imdListToComplete) {
		for(String imdId : imdListToComplete.keySet()) {
			BusinessObjectInstance imdInstance = BusinessObjectInstance.create(imdListToComplete.get(imdId));                                                                                                                 
			imdInstance.set("initialMeasurementDataId", imdId);      
			imdInstance = BusinessObjectDispatcher.read(imdInstance);
			COTSInstanceNode postVeeGroup = imdInstance.getGroupFromPath(POSTVEE_GRP);
			postVeeGroup.set("enQty", postVeeGroup.getNumber("stQty"));   
			
			COTSInstanceList msrmtListFromPostVEE = postVeeGroup.getGroup(MSRMT_GRP).getList(MSRMT_LIST);
			Iterator<COTSInstanceListNode> msrmtListItrFromPostVEE = msrmtListFromPostVEE.iterator();

			while (msrmtListItrFromPostVEE.hasNext()) {
				COTSInstanceListNode msrmtListNodeFromPostVEE = msrmtListItrFromPostVEE.next();
				msrmtListNodeFromPostVEE.set(CONSUMPTION_QUANTITY, BigDecimal.ZERO);
			}
			
			imdInstance.set("boStatus", "FORCECMP");     
			imdInstance = BusinessObjectDispatcher.update(imdInstance);
		}
	}


	/** Return all the valid MC Types from VEE Rule Configuration
	 * @param veeRuleInstance 
	 * @return
	 */
	private ArrayList<String> getValidMCTypeFromVEERuleConfig(BusinessObjectInstance veeRuleInstance) {
		COTSInstanceList mcTypeList = veeRuleInstance.getList("mcTypeList");
		Iterator<COTSInstanceListNode> itrMcTypeList = mcTypeList.iterator();

		ArrayList<String> validMCTypes = new ArrayList<String>();
		while (itrMcTypeList.hasNext()) {
			COTSInstanceListNode mcTypeListNode = itrMcTypeList.next();
			String mcType = mcTypeListNode.getString("mcType");
			validMCTypes.add(mcType);
		}
		return validMCTypes;
	}

	/** get VEE Rule BO Instance
	 * @return
	 */
	private BusinessObjectInstance getVEERuleBoInstance() {
		BusinessObjectInstance veeRuleInstance = BusinessObjectInstance.create(applyVeeRuleAlgorithmInputData.getVeeRuleBo());                                                                                                                 
		veeRuleInstance.set("veeGroup", applyVeeRuleAlgorithmInputData.getVeeGroup().getId().getTrimmedValue());                                                                             
		veeRuleInstance.set("veeRule", applyVeeRuleAlgorithmInputData.getVeeRule().fetchIdVeeRule().trim());                                                                                 
		veeRuleInstance = BusinessObjectDispatcher.read(veeRuleInstance);
		return veeRuleInstance;
	}

	/** Add Exception 
	 * @param exceptionType
	 * @param exceptionSeverity
	 * @param serverMessage
	 */
	private void addException(ExceptionType exceptionType, ExceptionSeverityLookup exceptionSeverity,
			ServerMessage serverMessage) {
		List<VeeRuleExceptionData> veeExceptions = applyVeeRuleAlgorithmInputOutputData.getVeeExceptionList();

		if (isNull(veeExceptions)) {
			veeExceptions = new ArrayList<VeeRuleExceptionData>();
		}
		VeeRuleExceptionData veeRuleExceptionData = VeeRuleExceptionData.Factory.newInstance();

		if (notNull(exceptionType) || notNull(exceptionSeverity)) {
			veeRuleExceptionData.setExceptionType(exceptionType);
			veeRuleExceptionData.setExceptionSeverity(exceptionSeverity);
		}
		veeRuleExceptionData.setVeeRule(applyVeeRuleAlgorithmInputData.getVeeRule());

		ServerMessageToMdmMessageConverter msgConv = ServerMessageToMdmMessageConverter.Factory.newInstance();
		MdmMessageData mdmMsg = msgConv.convertServerMessageToMdmMessage(serverMessage);
		veeRuleExceptionData.setMdmMessageData(mdmMsg);

		veeRuleExceptionData.setSequence(BigInteger.valueOf(veeExceptions.size() + 1));

		veeExceptions.add(veeRuleExceptionData);
		applyVeeRuleAlgorithmInputOutputData.setVeeExceptionList(veeExceptions);
	}


	/**
	 * @param fieldName
	 * @param value
	 */
	private void setTrace(String fieldName, String value) {
		veeRuleTraceList = applyVeeRuleAlgorithmInputOutputData.getVeeTraceList();
		if (notNull(veeRuleTraceList) && (!veeRuleTraceList.isEmpty())) {
			if (notNull(value)) {
				VeeRuleVariablesData veeRuleVariablesData = VeeRuleVariablesData.Factory.newInstance();
				String fieldDesc = retrieveFieldDescription(fieldName);
				veeRuleVariablesData.setDescription(fieldDesc);
				veeRuleVariablesData.setValue(value);
				veeRuleVariablesDataList.add(veeRuleVariablesData);
			}
		}
	}

	/**
	 * @param fieldName
	 * @return
	 */
	private String retrieveFieldDescription(String fieldName) {
		BusinessServiceInstance bsInstance = BusinessServiceInstance.create("F1-GetFieldLabel");
		bsInstance.set("field", fieldName);
		BusinessServiceInstance result = BusinessServiceDispatcher.execute(bsInstance);
		String fieldDescription = result.getString("longLabel");

		return fieldDescription;
	}

	@Override
	public ApplyVeeRuleAlgorithmInputOutputData getApplyVeeRuleAlgorithmInputOutputData() {
		return applyVeeRuleAlgorithmInputOutputData;
	}

	@Override
	public void setApplyVeeRuleAlgorithmInputData(
			ApplyVeeRuleAlgorithmInputData arg0) {
		applyVeeRuleAlgorithmInputData = arg0;	
	}

	@Override
	public void setApplyVeeRuleAlgorithmInputOutputData(
			ApplyVeeRuleAlgorithmInputOutputData arg0) {
		applyVeeRuleAlgorithmInputOutputData = arg0;
	}

}
