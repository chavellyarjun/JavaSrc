package com.splwg.cm.domain.batch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;






import com.splwg.base.api.Query;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.SingleTransactionStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.businessService.BusinessServiceDispatcher;
import com.splwg.base.api.businessService.BusinessServiceInstance;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.Money;
import com.splwg.base.api.lookup.MessageSeverityLookup;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.businessService.BusinessService_Id;
import com.splwg.base.domain.common.currency.Currency_Id;
import com.splwg.base.domain.common.message.MessageParameters;
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
import com.splwg.ccb.api.lookup.DepositControlStagingStatusLookup;
import com.splwg.ccb.api.lookup.PaymentTenderStagingStatusLookup;
import com.splwg.ccb.api.lookup.TenderControlStatusLookup;
import com.splwg.ccb.domain.admin.tenderType.entity.TenderType;
import com.splwg.ccb.domain.admin.tenderType.entity.TenderType_Id;
import com.splwg.ccb.domain.billing.bill.entity.Bill_Id;
import com.splwg.ccb.domain.customerinfo.account.entity.Account_Id;
import com.splwg.ccb.domain.payment.depositControlStaging.entity.DepositControlStaging;
import com.splwg.ccb.domain.payment.depositControlStaging.entity.DepositControlStaging_DTO;
import com.splwg.ccb.domain.payment.depositControlStaging.entity.DepositControlStaging_Id;
import com.splwg.ccb.domain.payment.depositControlStaging.entity.TenderControlStaging;
import com.splwg.ccb.domain.payment.depositControlStaging.entity.TenderControlStaging_DTO;
import com.splwg.ccb.domain.payment.depositControlStaging.entity.TenderControlStaging_Id;
import com.splwg.ccb.domain.payment.paymentEvent.entity.PaymentEvent;
import com.splwg.ccb.domain.payment.paymentEvent.entity.PaymentTender;
import com.splwg.ccb.domain.payment.paymentTenderUpload.entity.PaymentTenderUpload_DTO;
import com.splwg.ccb.domain.payment.paymentTenderUpload.entity.PaymentTenderUpload_Id;
import com.splwg.ccb.domain.payment.paymentTenderUpload.entity.PaymentTenderUpload;

import com.splwg.ccb.domain.payment.paymentTenderUpload.entity.PaymentUpload_DTO;
import com.splwg.ccb.domain.payment.paymentTenderUpload.entity.PaymentUpload_Id;
import com.splwg.cm.domain.customMessages.CmMessageRepository;

import com.splwg.shared.common.ServerMessage;
import com.splwg.shared.logging.LoggerFactory;
import com.splwg.shared.logging.Logger;


/**
 * @author neelima
 *
@BatchJob (rerunnable = false,modules = { },
 *      softParameters = { @BatchJobSoftParameter (name = INPUT_FILE_PATH, required = true, type = string)
 *            , @BatchJobSoftParameter (name = PROCESSED_FILE_PATH, required = true, type = string)
 *            , @BatchJobSoftParameter (name = ERROR_FILE_PATH, required = true, type = string)})
 */
public class APayPaymentUpload extends APayPaymentUpload_Gen {

	private static  Logger logger=LoggerFactory.getLogger(APayPaymentUpload.class);
	private static ArrayList<String[]> errorValues = new ArrayList<String[]>();
	//private static final String success = "SUCCESS";
	//private static final String error = "ERROR";
	
	//Soft parameters validation 
	
	public void validateSoftParameters(boolean isNewRun)
	  {
	    logger.info("Start of validateSoftParameters() for PaymentFileProcessingBatch");
	    
	    JobParameters jobParams = getParameters();
	    String inputFilePath = jobParams.getINPUT_FILE_PATH();
	    String errorFilePath = jobParams.getERROR_FILE_PATH();
	    String processedFilePath = jobParams.getPROCESSED_FILE_PATH();
	    
	    File file = new File(inputFilePath);
	    if (!file.isDirectory()) {
	      addError(CmMessageRepository.invalidFilePth(inputFilePath));
	    }
	    file = null;
	    
	    file = new File(errorFilePath);
	    if (!file.isDirectory()) {
	      addError(CmMessageRepository.invalidFilePth(errorFilePath));
	    }
	    file = null;
	    
	    file = new File(processedFilePath);
	    if (!file.isDirectory()) {
	      addError(CmMessageRepository.invalidFilePth(processedFilePath));
	    }
	    file = null;
	    
	    logger.info("End of validateSoftParameters() for PaymentFileProcessingBatch");
	  }
	
	public JobWork getJobWork() {
		// TODO Auto-generated method stub
		JobParameters jobParams = getParameters();
	    String inputFilePath = jobParams.getINPUT_FILE_PATH();
	    File inputfilePth=new File(inputFilePath);
		File[] files = inputfilePth.listFiles();
		List<ThreadWorkUnit> workUnitList=new ArrayList<ThreadWorkUnit>();
		String inputCsv =null;
		logger.info("files length" + files.length );
		if(files.length >0){
			for(File inputFile : files){
				
			logger.info("the input files path in job work is  "+inputFile);
			inputCsv = inputFile.getName();
			logger.info("the file name in job work is  "+inputCsv);
			ThreadWorkUnit threadWorkUnit=new ThreadWorkUnit();		
			threadWorkUnit.addSupplementalData("fileName", inputFile.getName());
			workUnitList.add(threadWorkUnit);
		 }
		}
		else
        {
          addError(CmMessageRepository.filesNotPresent(inputFilePath));
          logger.info("No Files to Process");
        }
		return createJobWorkForThreadWorkUnitList(workUnitList);
				
	}

	public Class<APayPaymentUploadWorker> getThreadWorkerClass() {
		return APayPaymentUploadWorker.class;
	}

	public static class APayPaymentUploadWorker extends
			APayPaymentUploadWorker_Gen {
		private String processedFilePath = null;
	    private String errorFilePath = null;
	    private String inputFilePath = null;
	    private String batchCode = null;
	    private DateTime processDate = null;
	    String inputFile = null;
	    public void initializeThreadWork(boolean initializationPreviouslySuccessful)
	      throws ThreadAbortedException, RunAbortedException
	    {
	      ThreadParameters threadParameters = getParameters();
	      this.inputFilePath = threadParameters.getINPUT_FILE_PATH().trim();
	      this.processedFilePath = threadParameters.getPROCESSED_FILE_PATH().trim();
	      this.errorFilePath = threadParameters.getERROR_FILE_PATH();
	      this.batchCode = getBatchControlId().getIdValue();
	      this.processDate = getProcessDateTime();
	    }

		public ThreadExecutionStrategy createExecutionStrategy() {
			// TODO Auto-generated method stub
			return new SingleTransactionStrategy(this);
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			// TODO Auto-generated method stub
			logger.info("Start of executeWorkUnit() for PaymentFileProcessingBatch");
			
		      File inFile = null;
		      try{
		    	  
		    	  inputFile = (String)unit.getSupplementallData("fileName").toString();
		    	   inFile= new File(this.inputFilePath,inputFile);
		    	   
		    	   
		    	   
		    	   boolean status = processInputFile(inputFile, inFile);
		    	   logger.info("status of process input file "+status);
		    	   if(status){
		    		   //move file to processed folder
		    		   moveFile(inFile, inputFile, this.processedFilePath);
		    		   return true;
		    	   }
		    	   else{
		    		 //move file to error folder
		    		   moveFile(inFile, inputFile, this.errorFilePath);
		    		   return false;
		    	   }
		    		   
		    	
		      }catch(Exception e){
		    	  e.printStackTrace();
		    	  addError(CmMessageRepository.generalExceptionForBatch(e.getMessage()));
				  return false;
		      }
		      //write catch
		      finally{
		    	  
		      }
			
		}

		//This method is to process the input file
		private boolean processInputFile(String inputFile, File inFile) {
			try{
			// TODO Auto-generated method stub
			logger.info("Start of process input file");
			String savepoint = inputFile;
			FrameworkSession session = (FrameworkSession) SessionHolder.getSession();
			session.flush(savepoint);
			session.setSavepoint(savepoint);
			
			List<String> fileLines = new ArrayList<String>();
			String accountNum=null,billId=null,payDateTime=null;
			Double payAmount=null;
			
			//External Source ID: from Tender source Id of the bank
			String externalSourceId=null;
			int lineNum=2; 
			String externalBatchId = null;
			//External Transmission ID:header+processdatetime
	        String externalTransmitId = null;
	        Date accountingDate = null;
	       
	      //External reference Id is bill id;
	        String extReferenceId = null,tenderTypeCd = null,chckNumber = null,bankName = null,bankCode=null,reconcilationDate=null;
	        Money transactionAmount = Money.ZERO;
	        Money totalAmt = Money.ZERO;
	        int totalCount = 0;
	        Currency_Id currency = new Currency_Id("TRY");
	        PaymentTenderUpload_Id paymentTenderUploadId = null;
	        TenderControlStaging_Id tenderControlStagingId=null;
	        DepositControlStaging_Id depositControlStagingId=null;
			
	        String[]  header=null;
	        String[]  footer=null;
	        String matchType="BILL-ID";
	        String toDoType="CM-PUERR";
	        String toDoRole="F1_DFLT";
			try
		      {
				
				//File fileToProcess=  new File(inputFile + "/" + fileName);
		        BufferedReader br = new BufferedReader(new FileReader(inFile));
		        logger.info("buffered reader");
		        String str = null;
		        while ((str = br.readLine()) != null) {
		          if ((!isBlankOrNull(str)) || (!isEmptyOrNull(str))) {
		            fileLines.add(str);
		            logger.info(" the line is "+str);
		           
		          }
		        }
		        br.close();
		      }
		      catch (FileNotFoundException exception)
		      {
		        addError(CmMessageRepository.fileNotFoundExceptionForBatch(exception.getMessage()));
		      }
		      catch (IOException exception)
		      {
		        addError(CmMessageRepository.ioExceptionForBatch(exception.getMessage()));
		      }
			//Fetch the external Source ID, external transmit ID, batch ID
			
			header = fileLines.get(0).split(",");
			if(header[0].equals("H")){
				externalTransmitId=header[0]+"-"+header[2]+"-"+getProcessDateTime();
				logger.info("external transmit id:" + externalTransmitId);
				bankCode=header[1];
				logger.info("the tender bank code "+bankCode);
				externalSourceId = checkExternalSource(bankCode.trim());
				//External source ID is not present `
				if ((isBlankOrNull(externalSourceId)) || (isEmptyOrNull(externalSourceId)))
		          {
		            logger.info("external Source Id not present in system");
		            session.flush(savepoint);session.rollbackToSavepoint(savepoint);session.notifyOfDirectUpdate();
		            createToDo(CmMessageRepository.extSrceIdNotPresent(bankCode),toDoType,toDoRole,accountNum);
		            logMessage(CmMessageRepository.extSrceIdNotPresent(bankCode), MessageSeverityLookup.constants.ERROR);
		            return false;
		          }
				logger.info("external source id:" + externalSourceId);
				//External Batch ID:Tender source-date & time  
				externalBatchId=externalSourceId.trim() +"-"+ this.getProcessDateTime();
				logger.info("external batch id:" + externalBatchId);
				reconcilationDate=header[2];
				tenderTypeCd=header[3];
			}
			//if header is not present in file
			else{
				logger.info("no header");
	            session.flush(savepoint);session.rollbackToSavepoint(savepoint);session.notifyOfDirectUpdate();
	            createToDo(CmMessageRepository.noHeader(inputFile),toDoType,toDoRole,accountNum);
	            logMessage(CmMessageRepository.noHeader(inputFile), MessageSeverityLookup.constants.ERROR);
	            return false;
			}
			
			//Deposit control , Tender control creation
			//startChanges();
	        depositControlStagingId = new DepositControlStaging_Id(externalSourceId, externalTransmitId);
	        logger.info("deposit control id "+depositControlStagingId);
	        DepositControlStaging_DTO depositControlStagingDTO = new DepositControlStaging_DTO();
	        depositControlStagingDTO.setId(depositControlStagingId);
	        depositControlStagingDTO.setTenderControlTotalAmount(totalAmt);
	        depositControlStagingDTO.setTotalTenderControl(BigInteger.ONE);
	        depositControlStagingDTO.setTransmissionDateTime(getProcessDateTime());
	        depositControlStagingDTO.setDepositControlStagingStatus(DepositControlStagingStatusLookup.constants.PENDING);
	        depositControlStagingDTO.setCurrencyId(currency);
	        depositControlStagingDTO.newEntity();
	        //saveChanges();
	        logger.info("deposit control created ");
	        

	        //startChanges();
	         tenderControlStagingId = new TenderControlStaging_Id(depositControlStagingId, externalBatchId);
	        TenderControlStaging_DTO tenderControlStagingDTO = new TenderControlStaging_DTO();
	        tenderControlStagingDTO.setId(tenderControlStagingId);
	        tenderControlStagingDTO.setTotalNumberOfTender(BigInteger.valueOf(totalCount));
	        tenderControlStagingDTO.setTotalTendersAmount(totalAmt);
	        tenderControlStagingDTO.setTenderControlStatus(TenderControlStatusLookup.constants.PENDING);
	        tenderControlStagingDTO.newEntity();
	        //saveChanges();
	        logger.info("tender control created ");
			
			//Processing the payment data
			for (String line : fileLines)
		      {
				
				String[] data = line.split(",");
				logger.info("the element is "+data[0]);
				 //check the line read is Data or not
				if(data[0].equals("D")){
					accountNum=data[1];
					billId=data[2];
					extReferenceId=billId;
					accountingDate = this.getProcessDateTime().getDate();
					
					//tenderTypeCd="CASH";
					payAmount=Double.valueOf(Double.parseDouble(data[3]));
					
					if (payAmount.doubleValue() > 0.0D)
		            {
						payAmount = Double.valueOf(payAmount.doubleValue());
						payAmount=Double.valueOf(Math.round(payAmount* 100D) / 100D);
						transactionAmount = new Money(payAmount.toString(), currency);
						
		            //Math.round(payAmount* 100D) / 100D)	
		            }
		            
					payDateTime=data[4];
					try {
						logger.info("pay date before "+payDateTime);
						logger.info("recon date before "+reconcilationDate);
					java.util.Date reconDate1=new SimpleDateFormat("yyyy-MM-dd").parse(reconcilationDate.substring(0, 10)); 
					java.util.Date payDate1  = new SimpleDateFormat("yyyy-MM-dd").parse(payDateTime.substring(0, 10));
					logger.info("pay date "+payDate1);
					logger.info("recon date "+reconDate1);
					//if (payDate1.compareTo(reconDate1) < 0) {
					if(payDate1.after(reconDate1)){
			            System.out.println("payDate1 is after reconDate");
			            session.flush(savepoint);session.rollbackToSavepoint(savepoint);session.notifyOfDirectUpdate();
			            createToDo(CmMessageRepository.dateMismatch(payDateTime,reconcilationDate,billId),toDoType,toDoRole,accountNum);
						//logMessage(CmMessageRepository.dateMismatch(payDateTime,reconcilationDate,billId), MessageSeverityLookup.constants.ERROR);
						 return false;
			        }
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
					 
					
					logger.info("accountNum,extReferenceId,accountingDate, billId,tenderTypeCd, payAmount,transactionAmount, payDate"
							+accountNum+" , "+extReferenceId+" , "+accountingDate+" , "+billId+" , "+tenderTypeCd+" , "+payAmount+" , "+
							transactionAmount+" , "+payDateTime);
					//Account ID is blank
					if (isBlankOrNull(accountNum.trim())) {
						toDoType="CM-PUACT";
						session.flush(savepoint);session.rollbackToSavepoint(savepoint);session.notifyOfDirectUpdate();
						createToDo(CmMessageRepository.blankAccountID(accountNum),toDoType,toDoRole,billId);
						//logMessage(CmMessageRepository.blankAccountID(accountNum), MessageSeverityLookup.constants.ERROR);
						 return false;
					}
					//Check Account ID exists or not
					else{
						
						// validate the account id exists in CCB
						boolean checkAcctId = validateAccountId(accountNum.trim());
						logger.info("the check acct id "+checkAcctId);
						if (checkAcctId == false) {
							toDoType="CM-PUACT";
								session.flush(savepoint);
								session.rollbackToSavepoint(savepoint);
								session.notifyOfDirectUpdate();
								logger.info("rollbacked");
								
								createToDo(CmMessageRepository.invalidAccountID(accountNum),toDoType,toDoRole,billId);
							logMessage(CmMessageRepository.invalidAccountID(accountNum), MessageSeverityLookup.constants.ERROR);
							return false;
							
						}
					}
					//Bill ID is blank
					if (isBlankOrNull(billId.trim())) {
						session.flush(savepoint);session.rollbackToSavepoint(savepoint);session.notifyOfDirectUpdate();
						createToDo(CmMessageRepository.blankBillID(billId),toDoType,toDoRole,accountNum);
						logMessage(CmMessageRepository.blankBillID(billId), MessageSeverityLookup.constants.ERROR);
						 return false;
					}
					//Check Bill ID exists or not
					else{
						// validate the bill id exists in CCB
						boolean checkBillId = validateBillId(billId.trim());
						if (checkBillId == false) {
							
							session.flush(savepoint);session.rollbackToSavepoint(savepoint);session.notifyOfDirectUpdate();
							createToDo(CmMessageRepository.invalidBillID(billId),toDoType,toDoRole,accountNum);
							logMessage(CmMessageRepository.invalidBillID(billId), MessageSeverityLookup.constants.ERROR);
							 return false;
						}
					}
					//Duplicate payment--pending
					logger.info("duplicate check for ext ref id "+extReferenceId);
					  Query<PaymentTenderUpload> extRefQuery = createQuery("From PaymentTenderUpload pt where pt.id.externalReferenceId=:extRef");
			          
			          extRefQuery.bindStringProperty("extRef", PaymentTenderUpload.properties.externalReferenceId, extReferenceId);
			          extRefQuery.addResult("exRef", "pt");
			          List<PaymentTenderUpload> extRefList = extRefQuery.list();
			          logger.info("ext ref size "+extRefList.size());
			          if (extRefList.size() > 0)
			          {
			            logger.info("external reference id already uploaded to staging table:" + extReferenceId);
			          
			            session.flush(savepoint);session.rollbackToSavepoint(savepoint);session.notifyOfDirectUpdate();
			            createToDo(CmMessageRepository.paymentAlreadyUploaded(extReferenceId),toDoType,toDoRole,accountNum);
			            logMessage(CmMessageRepository.paymentAlreadyUploaded(extReferenceId), MessageSeverityLookup.constants.INFORMATIONAL);
			            return false;
			          }
			          else
			          {
			            Query<PaymentEvent> dupPaymentQuery = createQuery("From PaymentTender paytndr,PaymentEvent payevent  where paytndr.paymentEvent.id=payevent.id  and paytndr.externalReferenceId=:extRefId");
			            

			            dupPaymentQuery.bindStringProperty("extRefId", PaymentTender.properties.externalReferenceId, extReferenceId);
			            dupPaymentQuery.addResult("payevent", "payevent");
			            List<PaymentEvent> dupPaymentList = dupPaymentQuery.list();
			            if (dupPaymentList.size() > 0)
			            {
			              logger.info("payment already present in CC&B:" + extReferenceId);
			              session.flush(savepoint);session.rollbackToSavepoint(savepoint);session.notifyOfDirectUpdate();
			              createToDo(CmMessageRepository.paymentAlreadyPresent(extReferenceId),toDoType,toDoRole,accountNum);
			              logMessage(CmMessageRepository.paymentAlreadyPresent(extReferenceId), MessageSeverityLookup.constants.INFORMATIONAL);
			              return false;
			            } 
			          }
					//check payment amount and bill amount-pending
					String billAmtQuery="SELECT MATCH_EVT_ID,BILL_ID,ACCT_ID,DUE_DT,BILLAMT FROM "
							+ "( SELECT  B.BILL_ID,B.ACCT_ID,B.DUE_DT,SUM(FT.CUR_AMT) AS BILLAMT,MATCH_EVT_ID "
							+ "FROM CI_BILL B,CI_FT FT,CI_BSEG BS,CI_SA SA"
							+ " WHERE B.BILL_ID=BS.BILL_ID "
							+ "AND BS.SA_ID=FT.SA_ID "
							+ "AND FT.SA_ID=SA.SA_ID "
							+ "AND SA_TYPE_CD!='E-LPC' "
							+ "AND B.BILL_ID=" + billId +" "
							+ "AND FT.PARENT_ID=B.BILL_ID "
							+ "AND BILL_STAT_FLG='C' "
							+ "AND BS.BSEG_STAT_FLG !='60' "
							+ "AND FT.FT_TYPE_FLG IN ('BS','BX','AD','AX') "
							+ "GROUP BY B.BILL_ID, B.ACCT_ID,B.DUE_DT,MATCH_EVT_ID) ";
					
			        PreparedStatement billAmtQuerySt=createPreparedStatement(billAmtQuery, "getBillAmt");
			    	logger.info(" statement created ");
			        billAmtQuerySt.execute();
			        logger.info(" statement executed ");
			        List<SQLResultRow> billAmtQueryList = billAmtQuerySt.list();
			        logger.info(" listttt");
					Double billAmt=null;
					if (billAmtQueryList.size() > 0) 
					{
						logger.info(" bill amt list  "+billAmtQueryList.get(0).getString("BILLAMT"));
						billAmt=Double.valueOf(billAmtQueryList.get(0).getString("BILLAMT"));
						if(billAmt.compareTo(payAmount)==0){
							logger.info(" bill amt is same as pay amount");
							billAmtQuerySt.close();
						}
						else{
							logger.info(" bill amt is different from pay amount");
							session.flush(savepoint);session.rollbackToSavepoint(savepoint);session.notifyOfDirectUpdate();
							createToDo(CmMessageRepository.billPayAmtDiffer(billId,billAmt.toString(),payAmount.toString()),toDoType,toDoRole,accountNum);
							logMessage(CmMessageRepository.billPayAmtDiffer(billId,billAmt.toString(),payAmount.toString()), MessageSeverityLookup.constants.ERROR);
							billAmtQuerySt.close(); 
							return false;
						}
					}
					else{
						session.flush(savepoint);session.rollbackToSavepoint(savepoint);session.notifyOfDirectUpdate();
						createToDo(CmMessageRepository.noFT(billId),toDoType,toDoRole,accountNum);
						logMessage(CmMessageRepository.noFT(billId), MessageSeverityLookup.constants.ERROR);
						logger.info(" bill amt list size is 0");
						billAmtQuerySt.close();
						 return false;
					}
					
					
					//create payment
					TenderType_Id tenderTypeId = new TenderType_Id(tenderTypeCd);
					logger.info("tender type id "+tenderTypeId);
		            TenderType tenderType = (TenderType)tenderTypeId.getEntity();
		            if (tenderType == null)
		              {
		                logger.info("tender type not present in CC&B");
		                session.flush(savepoint);session.rollbackToSavepoint(savepoint);session.notifyOfDirectUpdate(); 
		                createToDo(CmMessageRepository.tenderTypeNotPresent(tenderTypeCd,accountNum),toDoType,toDoRole,accountNum);
		                logMessage(CmMessageRepository.tenderTypeNotPresent(tenderTypeCd,accountNum), MessageSeverityLookup.constants.ERROR);
		                return false;
		            }
		            //create payment
		            else
		              {
		            	//startChanges();
		            	logger.info(" the tender control staging id, ext reference id, accounting date,  tender type id, transaction amount  "
		            			+ "check number , bank name, account num "+tenderControlStagingId +" ,"+extReferenceId+" "
		            					+ ","+accountingDate+" ,"+tenderTypeId+" ,"+transactionAmount+" ,"+chckNumber+" ,"+bankName+" ,"+accountNum);
		            	
		                paymentTenderUploadId = new PaymentTenderUpload_Id(tenderControlStagingId, extReferenceId);
		               
		                logger.info(" the payment tender upload id "+paymentTenderUploadId);
		                PaymentTenderUpload_DTO payTenderUploadDTO = new PaymentTenderUpload_DTO();
		                payTenderUploadDTO.setId(paymentTenderUploadId);
		                
		                payTenderUploadDTO.setAccountingDate(accountingDate);
		                payTenderUploadDTO.setTenderTypeId(tenderTypeId);
		                payTenderUploadDTO.setCustomerId(accountNum);
		                payTenderUploadDTO.setTenderAmount(transactionAmount);
		               
		              
		                if ((!isBlankOrNull(chckNumber)) || (!isEmptyOrNull(chckNumber))) {
		                  payTenderUploadDTO.setCheckNumber(chckNumber);
		                }
		                
		                if ((!isBlankOrNull(bankName)) || (!isEmptyOrNull(bankName))) {
		                  payTenderUploadDTO.setName(bankName);
		                }
		                
		                payTenderUploadDTO.setPaymentTenderStagingStatus(PaymentTenderStagingStatusLookup.constants.PENDING);
		                /*logger.info("status after "+PaymentTenderStagingStatusLookup.constants.PENDING);
		                logger.info(" the payment tender upload dto "+payTenderUploadDTO);*/
		                PaymentTenderUpload paymentTenderUpload= payTenderUploadDTO.newEntity();
		                
		                logger.info("pay tender uploaded");
		                
		                //match value
		                
		                PaymentUpload_DTO payUploadDTO = new PaymentUpload_DTO();
		                logger.info("match value started");
		                PaymentUpload_Id payUploadId = new PaymentUpload_Id(paymentTenderUpload,accountNum,matchType, billId);
		                
		                logger.info("pay tender upload id "+payUploadId);
		                payUploadDTO.setId(payUploadId);
		                payUploadDTO.setPaymentAmount(transactionAmount);
		                payUploadDTO.newEntity();
		                logger.info("pay tender id  uploaded");
		                //saveChanges();
		                totalAmt = totalAmt.add(transactionAmount);
		                logger.info("total amount after"+totalAmt);
		                totalCount++;  
		            }
			            
					// to get the payment record count
					lineNum++;
				}
				
		      }
			
			//fetch the footer data
			logger.info("the line num "+lineNum);
			footer=fileLines.get(lineNum-1).split(",");
			if(footer[0].equals("F")){
			
				Money invoiceAmt= new Money(footer[2], currency);
				logger.info("invoice amt" + invoiceAmt);
				if(invoiceAmt.isEqualTo(totalAmt)){
					//
				}
				else{
					logger.info("the invoice amount does not match with the total amount");
					session.flush(savepoint);session.rollbackToSavepoint(savepoint);session.notifyOfDirectUpdate();
					createToDo(CmMessageRepository.invoicePayAmtDiffer(invoiceAmt.toString(),payAmount.toString()),toDoType,toDoRole,accountNum);
					logMessage(CmMessageRepository.invoicePayAmtDiffer(invoiceAmt.toString(),payAmount.toString()), MessageSeverityLookup.constants.ERROR);
					return false;
				}
				
				//check line count
				
				int billCount=Integer.parseInt(footer[1]);
				logger.info("the bill count "+billCount);
				logger.info("the line count "+(lineNum-2));
				if(billCount!= (lineNum-2)){
					logger.info("the payment count does not match with count in file");
					session.flush(savepoint);session.rollbackToSavepoint(savepoint);session.notifyOfDirectUpdate();
					createToDo(CmMessageRepository.noOfRecordMismatch(""+billCount,""+(lineNum-2)),toDoType,toDoRole,accountNum);
					logMessage(CmMessageRepository.noOfRecordMismatch(""+billCount,""+(lineNum-2)), MessageSeverityLookup.constants.ERROR);
					return false;
				}
			}
			else{
				session.flush(savepoint);session.rollbackToSavepoint(savepoint);session.notifyOfDirectUpdate();
				createToDo(CmMessageRepository.noFooter(inputFile),toDoType,toDoRole,accountNum);
				logMessage(CmMessageRepository.noFooter(inputFile), MessageSeverityLookup.constants.ERROR);
				return false;
			}
	        //startChanges();
	        DepositControlStaging depositControl = (DepositControlStaging)depositControlStagingId.getEntity();
	        DepositControlStaging_DTO depositControlDTO = depositControl.getDTO();
	        depositControlDTO.setTenderControlTotalAmount(totalAmt);
	        depositControlDTO.setTotalTenderControl(BigInteger.ONE);
	        depositControl.setDTO(depositControlDTO);
	        //saveChanges();
	        

	        logger.info("total amt" + totalAmt);
	        //startChanges();
	        TenderControlStaging tenderControl = (TenderControlStaging)tenderControlStagingId.getEntity();
	        TenderControlStaging_DTO tenderControlDTO = tenderControl.getDTO();
	        tenderControlDTO.setTotalTendersAmount(totalAmt);
	        tenderControlDTO.setTotalNumberOfTender(BigInteger.valueOf(totalCount));
	        tenderControl.setDTO(tenderControlDTO);
	        //saveChanges();
			
	        //validate the total amount with the invoice amount in file
	        
	        logger.info("End of of processInputFile() for PaymentFileProcessingBatch");
			return true;
			}
			catch(Exception mainExc){
				mainExc.printStackTrace();
				 addError(CmMessageRepository.generalExceptionForBatch(mainExc.getMessage()));
				 return false;
			}
		}
		//This method is to create To Do
		private void createToDo(ServerMessage serverMsg,
				String toDoType, String toDoRole, String drillKey) {
			logger.info("in to do method");
			BusinessService_Id busSrvIDToDo = new BusinessService_Id("F1-AddToDoEntry"); 
			BusinessServiceInstance busSrvToDo = BusinessServiceInstance.create(busSrvIDToDo.getEntity());
			BigDecimal msgCat = new BigDecimal(serverMsg.getCategory());
			busSrvToDo.set("toDoType", toDoType);
			busSrvToDo.set("toDoRole", toDoRole);
			busSrvToDo.set("drillKey1",drillKey);
			
			busSrvToDo.set("messageCategory",new com.ibm.icu.math.BigDecimal(serverMsg.getCategory()));
			busSrvToDo.set("messageNumber",new com.ibm.icu.math.BigDecimal(serverMsg.getNumber().toString()));
			MessageParameters messageParameters = serverMsg.getMessageParameters();
		    List<Object> parameters = messageParameters.getParameters();
		    int i = 1;
		    logger.info("message parameters size "+messageParameters.getSize());
		    for (int iCount = 0; iCount < messageParameters.getSize(); iCount++)
		    {
		    	busSrvToDo.set("messageParm"+i,parameters.get(iCount).toString());
		    	i++;
		    }
		    String toDoEntryId=BusinessServiceDispatcher.execute(busSrvToDo).getString("toDoEntryId");
			logger.info("ToDoId : "+toDoEntryId);
			
		}

		//This method is to validate Bill ID
		private boolean validateBillId(String billId) {
			Bill_Id bill_id = new Bill_Id(billId);
			if(isNull(bill_id))
				return false;
			else{
			if(isNull(bill_id.getEntity()))
				return false;
			else 
				return true;
			}
		}

		//This method is to validate account ID
		private boolean validateAccountId(String accountNum) {

				Account_Id acctId = new Account_Id(accountNum);
				logger.info("acct id is "+acctId);
				if(isNull(acctId))
					return false;
				else{
				if(isNull(acctId.getEntity()))
					return false;
				else 
					return true;
					}
		}


		// To check the source type
		private String checkExternalSource(String bankCode) {
			String externalSourceId = null;
			String sourceTypeQuery="select EXT_SOURCE_ID AS SRCID from CI_TNDR_SRCE where bank_cd='" + bankCode +"'";
			PreparedStatement sourceTypeQuerySt=createPreparedStatement(sourceTypeQuery, "getsourceType");
	    	logger.info(" sourceTypeQuerySt statement created ");
	    	sourceTypeQuerySt.execute();
	        logger.info(" sourceTypeQuerySt statement executed ");
	        List<SQLResultRow> sourceTypeQueryList = sourceTypeQuerySt.list();
	        logger.info(" sourceTypeQueryList listttt");
			
			if (sourceTypeQueryList.size() > 0) 
			{
			   logger.info(" EXT SRC ID  "+sourceTypeQueryList.get(0).getString("SRCID"));
			   externalSourceId=sourceTypeQueryList.get(0).getString("SRCID");
			   sourceTypeQuerySt.close();
		      }
			
		      return externalSourceId;
		}
		
		// To move the file 
		private boolean moveFile(File file, String fileName, String destinationPath)
	    {
	      boolean moved = false;
	      logger.info("file name"+fileName);
	      String newFilename=fileName.substring(0, fileName.indexOf(".csv")).concat(this.getProcessDateTime().toString())+fileName.substring(fileName.indexOf(".csv"),fileName.length());
	      logger.info("new file name"+newFilename);
	      String outPath = destinationPath + File.separator + newFilename;
	      logger.info("output path " + outPath);
	      try
	      {
	        if (file.renameTo(new File(outPath)))
	        {
	          logger.info("file moved successfully");
	          moved = true;
	        }
	        else
	        {
	         logger.info("file not moved successfully");
	          moved = false;
	        }
	      }
	      catch (Exception e)
	      {
	        logger.info("moving file from " + file.getAbsolutePath() + " to " + outPath + " failed");
	      }
	      return moved;
	    }
		
		
	}

}
