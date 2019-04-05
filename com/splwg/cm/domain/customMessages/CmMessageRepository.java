/*
 * This class is a repository of message methods.

 */
package com.splwg.cm.domain.customMessages;
import com.ibm.icu.math.BigDecimal;
import com.splwg.base.domain.common.message. MessageParameters;
import com.splwg.shared.common.ServerMessage;
public class CmMessageRepository extends CmMessages {
	
	private static CmMessageRepository instance;

	private static CmMessageRepository getInstance(){
		
		if (instance == null){
			instance = new CmMessageRepository();
		}
		return instance;
	}
	
		public static ServerMessage invalidFilePath(String filePath) {
			MessageParameters parms = new MessageParameters();
			parms.addRawString(filePath);
			return getInstance().getMessage(INVALID_FILE_PATH,parms);
		}
		
		public static ServerMessage invalidFileName(String fileName) {
			MessageParameters parms = new MessageParameters();
			parms.addRawString(fileName);
			return getInstance().getMessage(INVALID_FILE_NAME,parms);
		}
		public static ServerMessage FinalMeasurementReplacementIssue(String startDate ,String endDate) {
			MessageParameters parms = new MessageParameters();
			parms.addRawString(startDate);
			parms.addRawString(endDate);
			return getInstance().getMessage(MEASR_REPLACEMENT_EXCEPTION,parms);
		}
		public static ServerMessage toDoRequiredValue() {
			MessageParameters parms = new MessageParameters();
			return getInstance().getMessage(TO_DO_REQUIRED,parms);
		}
		public static ServerMessage softParamsNotPassed(){
		   return getInstance().getMessage(SOFT_PARAMS_NOT_PASSED);
		}
		public static ServerMessage invalidFilePth(String filePath)
		  {
		    MessageParameters messParms = new MessageParameters();
		    messParms.addRawString(filePath);
		    return getInstance().getMessage(16000, messParms);
		  }
		
		public static ServerMessage filesNotPresent(String inputPath)
		  {
		    MessageParameters messParms = new MessageParameters();
		    messParms.addRawString(inputPath);
		    return getInstance().getMessage(16010, messParms);
		  }
		
		public static ServerMessage ioExceptionForBatch(String expMessage)
		  {
		    MessageParameters messParms = new MessageParameters();
		    messParms.addRawString(expMessage);
		    return getInstance().getMessage(16020, messParms);
		  }
		
		public static ServerMessage fileNotFoundExceptionForBatch(String expMessage)
		  {
		    MessageParameters messParms = new MessageParameters();
		    messParms.addRawString(expMessage);
		    return getInstance().getMessage(16030, messParms);
		  }
		public static ServerMessage extSrceIdNotPresent(String tenderSrce)
		  {
		    MessageParameters messParms = new MessageParameters();
		    messParms.addRawString(tenderSrce);
		    return getInstance().getMessage(16031, messParms);
		  }
		public static ServerMessage blankAccountID(String accountID)
		  {
		    MessageParameters messParms = new MessageParameters();
		    messParms.addRawString(accountID);
		    return getInstance().getMessage(16032, messParms);
		  }
		public static ServerMessage invalidAccountID(String accountID)
		  {
		    MessageParameters messParms = new MessageParameters();
		    messParms.addRawString(accountID);
		    return getInstance().getMessage(16033, messParms);
		  }
		public static ServerMessage blankBillID(String billID)
		  {
		    MessageParameters messParms = new MessageParameters();
		    messParms.addRawString(billID);
		    return getInstance().getMessage(16034, messParms);
		  }
		public static ServerMessage invalidBillID(String billID)
		  {
		    MessageParameters messParms = new MessageParameters();
		    messParms.addRawString(billID);
		    return getInstance().getMessage(16035, messParms);
		  }
		public static ServerMessage tenderTypeNotPresent(String billID,String accountID)
		  {
		    MessageParameters messParms = new MessageParameters();
		    messParms.addRawString(billID);
		    messParms.addRawString(accountID);
		    return getInstance().getMessage(16036, messParms);
		  }
		public static ServerMessage noFT(String billID)
		  {
		    MessageParameters messParms = new MessageParameters();
		    messParms.addRawString(billID);
		    
		    return getInstance().getMessage(16037, messParms);
		  }
		public static ServerMessage billPayAmtDiffer(String billID,String billAmt,String payAmount)
		  {
		    MessageParameters messParms = new MessageParameters();
		    messParms.addRawString(billID);
		    messParms.addRawString(billAmt);
		    messParms.addRawString(payAmount);
		    return getInstance().getMessage(16038, messParms);
		  }
		public static ServerMessage invoicePayAmtDiffer(String invoiceAmt,String payAmount)
		  {
		    MessageParameters messParms = new MessageParameters();
		    messParms.addRawString(invoiceAmt);
		    messParms.addRawString(payAmount);
		    return getInstance().getMessage(16039, messParms);
		  }
		public static ServerMessage noOfRecordMismatch(String recordCount,String paymentsCount)
		  {
		    MessageParameters messParms = new MessageParameters();
		    messParms.addRawString(recordCount);
		    messParms.addRawString(paymentsCount);
		    return getInstance().getMessage(16040, messParms);
		  }
		public static ServerMessage paymentAlreadyUploaded(String extRefID)
		  {
		    MessageParameters messParms = new MessageParameters();
		    messParms.addRawString(extRefID);
		    return getInstance().getMessage(16041, messParms);
		  }
		public static ServerMessage paymentAlreadyPresent(String extRefID)
		  {
		    MessageParameters messParms = new MessageParameters();
		    messParms.addRawString(extRefID);
		    return getInstance().getMessage(16042, messParms);
		  }
		public static ServerMessage noFooter(String fileName) {
			MessageParameters parms = new MessageParameters();
			parms.addRawString(fileName);
			return getInstance().getMessage(16043,parms);
		}
		public static ServerMessage noHeader(String fileName) {
			MessageParameters parms = new MessageParameters();
			parms.addRawString(fileName);
			return getInstance().getMessage(16044,parms);
		}
		public static ServerMessage dateMismatch(String payDate,String reconDate,String billID)
		  {
		    MessageParameters messParms = new MessageParameters();
		    messParms.addRawString(payDate);
		    messParms.addRawString(reconDate);
		    messParms.addRawString(billID);
		    
		    return getInstance().getMessage(16045, messParms);
		  }
		public static ServerMessage generalExceptionForBatch(String expMessage)
		  {
		    MessageParameters messParms = new MessageParameters();
		    messParms.addRawString(expMessage);
		    return getInstance().getMessage(16045, messParms);
		  }						
}