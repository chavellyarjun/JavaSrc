package com.splwg.cm.domain;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.domain.common.message.MessageParameters;
import com.splwg.d2.domain.admin.AdminMessages;
import com.splwg.shared.common.ServerMessage;

public class CMOnSpotBillingWarningMessagesRepository  extends CMOnSpotBillingWarningMessages {
	  private static CMOnSpotBillingWarningMessagesRepository instance;

	  private static CMOnSpotBillingWarningMessagesRepository getInstance() {

			if (instance == null) {
				  instance = new CMOnSpotBillingWarningMessagesRepository();
			}
			return instance;
	  }

	  public static final int INDEX_BACKWARD_IDENTIFIED = 565;
	  public static final int HIGH_USAGE_FOUND = 542;
	  
	  public static ServerMessage indexBackwardMessage(String mCTypeDescription) {
			MessageParameters messageParameters = new MessageParameters();
			messageParameters.addRawString(mCTypeDescription);
			return getInstance().getMessage(INDEX_BACKWARD_IDENTIFIED, messageParameters);
	  }
	  
	  public static ServerMessage overLimitException(BigDecimal consumption) {
			MessageParameters parms = new MessageParameters();
			parms.addBigDecimal(consumption);
		    return getInstance().getMessage(HIGH_USAGE_FOUND, parms);
		}

	  
}
