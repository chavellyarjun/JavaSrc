package com.splwg.cm.domain;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.domain.common.message.MessageParameters;
import com.splwg.d2.domain.admin.AdminMessages;
import com.splwg.shared.common.ServerMessage;

public class CMOnSpotBillingErrorMessagesRepository  extends CMOnSpotBillingErrorMessages {
	  private static CMOnSpotBillingErrorMessagesRepository instance;

	  private static CMOnSpotBillingErrorMessagesRepository getInstance() {

			if (instance == null) {
				  instance = new CMOnSpotBillingErrorMessagesRepository();
			}
			return instance;
	  }
	  public static final int HIGH_USAGE_FOUND_EXCEPTION = 509;
	  public static final int LOW_USAGE_FOUND_EXCEPTION = 558;
	
		
		public static ServerMessage overLimitException(BigDecimal consumption) {
			MessageParameters parms = new MessageParameters();
			parms.addBigDecimal(consumption);
			return getInstance().getMessage(HIGH_USAGE_FOUND_EXCEPTION, parms);
		}

		public static ServerMessage underLimitException(BigDecimal consumption) {
			MessageParameters parms = new MessageParameters();
			parms.addBigDecimal(consumption);
			return getInstance().getMessage(LOW_USAGE_FOUND_EXCEPTION, parms);
		}

	  
	  
}
