/*
 * This file is for message constants.
 **************************************************************************************
 * CHANGE HISTORY:                                                
 *                                                                
 * Date:        by:     		Reason:                                     
 * YYYY-MM-DD   IN      		Reason text.                                
 * 2015-09-21   Bharadwaj.V    Modified to add modification history.
 * 2015-11-09	Anitha G	      Added Messages as part of PF040-009
 * *************************************************************************************
 */
package com.splwg.cm.domain.customMessages;

import com.splwg.base.domain.common.message.AbstractMessageRepository;

public class CmMessages extends AbstractMessageRepository
{
	public static final int MESSAGE_CATEGORY = 90000;

	public CmMessages(){
		
		super(MESSAGE_CATEGORY);
	}
	public static final int INVALID_FILE_PATH=90830;
	public static final int INVALID_FILE_NAME=90831;
	public static final int MEASR_REPLACEMENT_EXCEPTION = 99300;
	public static final int TO_DO_REQUIRED = 99306;
	
	public static final int SOFT_PARAMS_NOT_PASSED = 606;

}
