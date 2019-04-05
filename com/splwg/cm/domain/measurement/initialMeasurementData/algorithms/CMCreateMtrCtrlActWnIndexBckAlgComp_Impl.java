package com.splwg.cm.domain.measurement.initialMeasurementData.algorithms;

import java.math.BigInteger;

import com.splwg.base.api.Query;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.businessObject.BusinessObjectInstanceKey;
import com.splwg.base.api.businessObject.BusinessObjectStatusCode;
import com.splwg.base.api.lookup.BusinessObjectStatusTransitionConditionLookup;
import com.splwg.base.api.lookup.LogEntryTypeLookup;
import com.splwg.base.api.maintenanceObject.MaintenanceObjectLogHelper;
import com.splwg.base.domain.common.businessObject.BusinessObject;
import com.splwg.base.domain.common.businessObject.BusinessObjectEnterStatusAlgorithmSpot;
import com.splwg.base.domain.common.businessObject.BusinessObject_Id;
import com.splwg.base.domain.common.businessObjectStatusReason.BusinessObjectStatusReason_Id;
import com.splwg.base.domain.common.maintenanceObject.MaintenanceObject;
import com.splwg.base.domain.common.maintenanceObject.MaintenanceObject_Id;
import com.splwg.base.domain.common.message.MessageCategory_Id;
import com.splwg.base.domain.common.message.MessageParameters;
import com.splwg.base.domain.common.message.ServerMessageFactory;
import com.splwg.cm.domain.CMOnSpotBillingWarningMessagesRepository;
import com.splwg.d1.domain.communications.activity.entities.Activity;
import com.splwg.d1.domain.communications.activity.entities.Activity_Id;
import com.splwg.d1.domain.deviceManagement.device.entities.Device;
import com.splwg.d1.domain.deviceManagement.deviceConfiguration.entities.DeviceConfiguration;
import com.splwg.d1.domain.deviceManagement.measuringComponent.entities.MeasuringComponent;
import com.splwg.d1.domain.measurement.initialMeasurementData.entities.InitialMeasurementData;
import com.splwg.d1.domain.measurement.initialMeasurementData.entities.InitialMeasurementDataLog;
import com.splwg.d1.domain.measurement.initialMeasurementData.entities.InitialMeasurementData_Id;
import com.splwg.shared.common.ServerMessage;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author 
 * Algorithm ID: CM-CREMTRINX
 * 
 * History:
 * 
 * Date			By				Reason
 * 
 * 25-03-2019				Create meter control Activity for IMD 
 * 	
 * @AlgorithmComponent ()
 */
public class CMCreateMtrCtrlActWnIndexBckAlgComp_Impl 
extends CMCreateMtrCtrlActWnIndexBckAlgComp_Gen 
implements BusinessObjectEnterStatusAlgorithmSpot {

	private static final String INITIAL_MSRMT_DATA_ID = "initialMeasurementDataId";

	//logger
	private Logger logger = LoggerFactory.getLogger(CMCreateMtrCtrlActWnIndexBckAlgComp_Impl.class);

	BusinessObjectInstanceKey inputBusinessObjectKey = null;

	@Override
	public void invoke() {

		String imdIDStr = inputBusinessObjectKey.getString(INITIAL_MSRMT_DATA_ID);

		InitialMeasurementData imdEntity = new InitialMeasurementData_Id(imdIDStr).getEntity();
		MeasuringComponent mcEntity = imdEntity.getMeasuringComponentId().getEntity();
		DeviceConfiguration dvcConfig = mcEntity.getDeviceConfigurationId().getEntity();
		Device dvc = dvcConfig.getDevice();

		StringBuilder mtrCtrlActRetStrBuilder = new StringBuilder();

		mtrCtrlActRetStrBuilder.append(" from  ActivityRelatedObject actRel , Activity act ");
		mtrCtrlActRetStrBuilder.append(" where actRel.id.maintenanceObject.id =:mo ");
		mtrCtrlActRetStrBuilder.append(" and actRel.id.activity.id = act.id ");
		mtrCtrlActRetStrBuilder.append(" and actRel.primaryKeyValue1 =:dvcId ");
		mtrCtrlActRetStrBuilder.append(" and act.businessObject =:bo ");
		mtrCtrlActRetStrBuilder.append(" and act.status =:status");

		Query<Activity_Id> activityIdQuery = createQuery(
				mtrCtrlActRetStrBuilder.toString(),
				"Query For getting IMD for device " + dvc.getId().getIdValue()
				);
		activityIdQuery.bindId("mo", new MaintenanceObject_Id("D1-DEVICE"));
		activityIdQuery.bindEntity("dvcId", dvc);
		activityIdQuery.bindId("bo", new BusinessObject_Id("CM-MrtCtrIndexBackwardActivity"));
		activityIdQuery.bindStringProperty("status", Activity.properties.status,"PENDING");

		activityIdQuery.addResult("activityId", "actRel.id.activity.id");


		for(Activity_Id queryResultRow : activityIdQuery.list()) {
			String actId = queryResultRow.getIdValue() ;
			logger.info("Activity already created before " + actId );
			return;
		}

		BusinessObjectInstance mtrCtrlIndexActivity = BusinessObjectInstance.create("CM-MrtCtrIndexBackwardActivity");
		mtrCtrlIndexActivity.set("bo", "CM-MrtCtrIndexBackwardActivity"); 
		mtrCtrlIndexActivity.set("initialMeasurementDataId", imdIDStr); 
		mtrCtrlIndexActivity.set("deviceId" , dvc.getId().getIdValue());
		mtrCtrlIndexActivity = BusinessObjectDispatcher.add(mtrCtrlIndexActivity);

		String actIdStr =  mtrCtrlIndexActivity.getString("activityId");
		addMOLogEntry(imdIDStr, actIdStr);
	}

	/**
	 * Create mo log to show Conversion IMD ID in the newly created IMD.
	 * @param imdIDStr
	 * @param actIdStr 
	 * @param imdBOInstance
	 */
	private void addMOLogEntry(String imdIDStr, String actIdStr) {

		if(!isBlankOrNull(imdIDStr)) {
			InitialMeasurementData_Id imdId = new InitialMeasurementData_Id(imdIDStr);
			InitialMeasurementData imdEntity = imdId.getEntity();

			if(!isNull(imdEntity)) {
				// Retrieve the mo to be used for adding MO Log
				MaintenanceObject mo = imdEntity.getBusinessObject().getMaintenanceObject();

				// Retrieve the moHelper object to be used for adding MO Log
				MaintenanceObjectLogHelper<InitialMeasurementData, InitialMeasurementDataLog> moHelper = 
						new MaintenanceObjectLogHelper<InitialMeasurementData, InitialMeasurementDataLog>(mo, imdEntity,
								imdEntity.getBusinessObject().getId());

				// Create Server Message
				MessageCategory_Id messageCategoryId = new MessageCategory_Id(new BigInteger(String.valueOf(CMOnSpotBillingWarningMessagesRepository.MESSAGE_CATEGORY)));
				MessageParameters params = new MessageParameters();
				params.addRawString(actIdStr);
				ServerMessageFactory serverMessageFactory = ServerMessageFactory.Factory.newInstance();
				ServerMessage serverMessage = serverMessageFactory.createMessage(messageCategoryId, 101, params);

				// Add the MO Log
				moHelper.addLogEntry(LogEntryTypeLookup.constants.SYSTEM, serverMessage, null, null, imdEntity);

			}
		}
	}

	public void setBusinessObjectKey(BusinessObjectInstanceKey boInstanceKey) 
	{
		inputBusinessObjectKey = boInstanceKey;
	} 


	public BusinessObjectStatusCode getNextStatus()
	{
		return null;
	}

	public BusinessObjectStatusTransitionConditionLookup getNextStatusCondition() {
		return null;
	}

	public boolean getUseDefaultNextStatus() {
		return false;
	}

	@Override
	public void setBusinessObject(BusinessObject arg0) {

	}

	@Override
	public BusinessObjectStatusReason_Id getStatusChangeReasonId() {

		return null;
	}

}
