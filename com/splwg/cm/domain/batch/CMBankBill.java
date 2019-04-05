package com.splwg.cm.domain.batch;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.math.BigInteger;





import org.dom4j.Element;

import com.splwg.shared.common.LoggedException;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.changehandling.StringProperty;
import com.splwg.base.api.datatypes.DateFormat;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.Id;
import com.ibm.icu.impl.Row;
import com.ibm.icu.math.BigDecimal;
import com.ibm.icu.text.DecimalFormat;
import com.ibm.icu.text.SimpleDateFormat;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.SingleTransactionStrategy;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.service.DataElement;
import com.splwg.base.api.service.PageHeader;
import com.splwg.base.api.serviceScript.ServiceScriptDispatcher;
import com.splwg.base.api.serviceScript.ServiceScriptInstance;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.web.userMap.ServiceScriptDataObject;
import com.splwg.ccb.domain.admin.autopaySource.entity.AutopaySource;
import com.splwg.ccb.domain.customerinfo.account.entity.Account;
import com.splwg.ccb.domain.customerinfo.account.entity.Account.EntityProperties.Autopays;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;


/**
 * @author SaikumarMamiddi
 *
@BatchJob (rerunnable = false,modules={ }, 
       softParameters = { @BatchJobSoftParameter (name = OUTPUT_FILE_PATH, required = true, type = string)})
 */
public class CMBankBill extends CMBankBill_Gen {

private static final Logger log = LoggerFactory.getLogger(CMBankBill.class); 
  

public JobWork getJobWork() 
	{
		log.info("In JobWork");
		System.out.println("in get job work entered");
		List<ThreadWorkUnit> dummyThreadWorkUnitsList = new ArrayList<ThreadWorkUnit>();
		StringBuilder sq11 = new StringBuilder();
		sq11.append("SELECT BB.CM_ASYN_ID,BB.BANK_ID ");
		sq11.append("FROM CISADM.CM_BANK_BILL BB WHERE BB.CM_STAT='P'");
		PreparedStatement saQuerySt = createPreparedStatement(sq11.toString(), "getPendingBankBills"); 
		List<SQLResultRow> queryList=saQuerySt.list();
		log.info("Query executed");
		if(queryList.size()==0)
		{
			log.info("No more pending bills found");
		}
		else
		{
			for(SQLResultRow row:saQuerySt.list())
			{
				BigDecimal id =row.getBigDecimal("CM_ASYN_ID");
				ThreadWorkUnit workUnit = new ThreadWorkUnit()  ;
				workUnit.addSupplementalData("asynId", row.getString("CM_ASYN_ID"));
				workUnit.addSupplementalData("bankId", row.getString("BANK_ID"));
				System.out.println("async id "+row.getString("CM_ASYN_ID") +"bank id"+row.getString("BANK_ID"));
				dummyThreadWorkUnitsList.add(workUnit); 
			}
		}
		return createJobWorkForThreadWorkUnitList(dummyThreadWorkUnitsList);
	}


public Class<CMBankBillWorker> getThreadWorkerClass() 
{
	return CMBankBillWorker.class;
}
public static class CMBankBillWorker extends CMBankBillWorker_Gen 
{
	public ThreadExecutionStrategy createExecutionStrategy() 
	{
		return new StandardCommitStrategy(this);
	}

	public boolean executeWorkUnit(ThreadWorkUnit unit) throws ThreadAbortedException,RunAbortedException
    {
		System.out.println("ëxecute work unit method");
		PreparedStatement statements=null;
		PreparedStatement QueryList=null;
		try 
		{
			String statement=null;
			String statement3=null;
			System.out.println("in execute work unit");
			String asynid = (String) unit.getSupplementallData("asynId");
			String bankCode = (String) unit.getSupplementallData("bankId");
			System.out.println("asynid is "+asynid);
			System.out.println("bankCode IS "+bankCode);
			statement= "SELECT apay.ACCT_ID, bill.BILL_ID,apay.ENTITY_NAME, SUM(ft.CUR_AMT) AS BILL_AMT, bill.CRE_DTTM, bill.DUE_DT,bill.alt_BILL_ID as SERIALNO "
				+ "FROM CI_ACCT_APAY apay, CI_BILL bill, CI_FT ft " 
				+ "WHERE bill.ACCT_ID = apay.ACCT_ID "
				+ "AND trim(apay.APAY_SRC_CD) = trim("+"\'"+bankCode+"\'"+")"
				+ "AND :PROCESSDATE <= bill.DUE_DT "
				+ "AND ft.PARENT_ID = bill.BILL_ID "
				+ "GROUP BY apay.ACCT_ID, bill.BILL_ID, bill.CRE_DTTM, bill.DUE_DT,apay.ENTITY_NAME,bill.alt_BILL_ID";
			statements=createPreparedStatement(statement,"getBankBill");
			statements.bindDate("PROCESSDATE", getProcessDateTime().getDate());
			System.out.println("statements value is :::: "+statements);
			
			statements.toString().concat(bankCode);
			System.out.println("getProcessDateTime"+getProcessDateTime());
			DateFormat dateFormatFinal = new DateFormat("yyyy-MM-dd HH:mm:ss"); 
			String dateTimeHeader= dateFormatFinal.format(getProcessDateTime());
			System.out.println("dateTimeHeader"+dateTimeHeader);
			DateFormat dateFormat = new DateFormat("yyyyMMddHHmmss") ;
			String dateTime= dateFormat.format(getProcessDateTime());
			System.out.println("dateTime is "+dateTime);
			//adding file name and type
			String fileName=bankCode+"_"+asynid+"_"+dateTime+".csv";
			System.out.println("fileName is "+fileName);
            String filePath =getParameters().getOUTPUT_FILE_PATH();
            String destFilePath = filePath.concat(fileName);
            //creating new file
            File destFile = new File(destFilePath);
            FileWriter outputfile;
            System.out.println("list size statements"+statements.list().size());
            outputfile = new FileWriter(destFile);    
            List<SQLResultRow> accountQueryList = statements.list();
            int count=0;
            
            if (accountQueryList.size() > 0) 
            {
            	BigDecimal sum = new BigDecimal(0);
            	
            	
            	System.out.println("bankCode"+bankCode);
            	outputfile.write(bankCode);
            	outputfile.append(",");
            	destFilePath = "";
            	
            	System.out.println("dateTime"+dateTime);
            	outputfile.write(dateTimeHeader);
            	outputfile.append("\n");
            	destFilePath = "";
            	
            	for (SQLResultRow row : accountQueryList)
            	{ 
            		String billId = row.getString("BILL_ID");
            		BigDecimal BILL_AMT  = new BigDecimal("0");
            		ServiceScriptInstance billAmountScriptInstance = ServiceScriptInstance.create("CM-GetBilBal");
            		billAmountScriptInstance.getElement().addElement("billId").setText(billId);
   					ServiceScriptDispatcher.invoke(billAmountScriptInstance); 
   					Element billAmountresponse = billAmountScriptInstance.getElement();
   					String balanceAmount=billAmountresponse.elementText("balanceAmount");
   					System.out.println("balance amount"+balanceAmount);
   					if(balanceAmount.equalsIgnoreCase("0"))
   					{
   						System.out.println("in continue");
   						continue;
   						
   					}
   					else{
   						System.out.println("in else continue");
   						BILL_AMT  = new BigDecimal(balanceAmount);
   						System.out.println("bill_amt"+BILL_AMT);
   					}
            		
            		
            		// BigDecimal BILL_AMT  = row.getBigDecimal("BILL_AMT");
					 System.out.println("BILL_AMT"+BILL_AMT);
            		 //sum=sum.add(BILL_AMT);
            		 
            		 System.out.println("sum"+sum);
            		 String[] data1= {row.getString("ACCT_ID")};
				     System.out.println("row.getString(ACCT_ID)"+row.getString("ACCT_ID"));
				     outputfile.write(row.getString("ACCT_ID"));
				     outputfile.append(",");
					 destFilePath = "";
					  String[] data6 = {row.getString("ENTITY_NAME")};
	                    System.out.println("row.getString(ENTITY_NAME)"+row.getString("ENTITY_NAME"));
	                    String title=row.getString("ENTITY_NAME");
	                    System.out.println("1st title is ::"+title);
	                    title=title.replaceAll(",", "");
	                    System.out.println("afetr replace title is ::"+title);

	                    outputfile.write(title);
	                    outputfile.append(",");
	                    destFilePath = "";
	                    
					 
				     System.out.println("row.getString(BILL_ID)"+row.getString("BILL_ID"));
				     outputfile.write(row.getString("BILL_ID"));
				     outputfile.append(",");
					 destFilePath = "";
					
					 BigDecimal lpc =new BigDecimal(0);
					 	ServiceScriptInstance scriptInstance = ServiceScriptInstance.create("CM-CalLPS");
	   					scriptInstance.getElement().addElement("billId").setText(billId);
	   					ServiceScriptDispatcher.invoke(scriptInstance); 
	   					Element response = scriptInstance.getElement();

	   					response.elementText("lpsAmount"); 
	   					
	   					lpc=new BigDecimal( response.elementText("lpsAmount"));
	   				
	   	            	System.out.println("scriptInstance.toString()"+response.elementText("lpsAmount"));
	   	            	outputfile.write(response.elementText("lpsAmount"));
	   	            	outputfile.append(",");
	   	            	destFilePath = "";
						//String[] data3 = {row.getString("BILL_AMT")};
						//System.out.println("row.getString(BILL_AMT)"+row.getString("BILL_AMT"));
						outputfile.write(BILL_AMT.toString());
						outputfile.append(",");
						destFilePath = "";
						
						Double BILLDOU = Double.parseDouble(BILL_AMT.toString());
						Double LPCDOU =Double.parseDouble(lpc.toString());
						//int totalAMT;
						Double totalAMT= BILLDOU+LPCDOU;
						DecimalFormat df = new DecimalFormat("#.##");      
						totalAMT = Double.valueOf(df.format(totalAMT));
						
						System.out.println("totalAMT"+totalAMT);
						outputfile.write(totalAMT.toString());
						 outputfile.append(",");
						 destFilePath = "";
						 sum=sum.add(new BigDecimal(totalAMT));
						 
						 String[] data4 = {row.getString("DUE_DT")};
		                    System.out.println("row.getString(DUE_DT)"+row.getString("DUE_DT"));
		                    String DueDateStr=row.getString("DUE_DT");
		                    if(!isEmptyOrNull(row.getString("DUE_DT"))){
		                    	DueDateStr=row.getString("DUE_DT").substring(0, 10);
		                    	System.out.println();
		                    }
		                    outputfile.write(DueDateStr);
		                    outputfile.append(",");
		                    destFilePath = "";
		                    
						String[] BillType ={"BillType"};
						System.out.println("BillType"+BillType);
	                    outputfile.write("R");
	                    outputfile.append(",");
	                    destFilePath = "";
	                    
	                    String[] data9 = {row.getString("SERIALNO")};
	                    System.out.println("row.getString(SERIALNO)"+row.getString("SERIALNO"));
	                    outputfile.write(row.getString("SERIALNO"));
	                    outputfile.append(",");
	                    destFilePath = "";
	                    
	                    String[] data5 = {row.getString("CRE_DTTM")};
	                    String cre=row.getString("CRE_DTTM");
	                    
	                    System.out.println("row.getString(CRE_DTTM)"+row.getString("CRE_DTTM"));
	                    
	                    String Credtt=cre.substring(0, 10)+" "+cre.substring(11,13 )+":"+cre.substring(14,16 )+":"+cre.substring(17,19);
	                    
	                    System.out.println("Credtt"+Credtt);
	                   
	                    outputfile.write(Credtt.toString());
	                    outputfile.append("\n");
	                    destFilePath = "";
	                  
	                    count++;
					 }
      			 System.out.println("count"+count);
     			 outputfile.write(String.valueOf(count));
       			 outputfile.append(",");
       			 destFilePath = "";
       			 sum=sum.setScale(2, BigDecimal.ROUND_HALF_EVEN);
			     System.out.println("sum"+sum);
			     outputfile.write(sum.toString());
			     outputfile.append("\n");
				 destFilePath = "";
				 PreparedStatement update=null;  
				 String statement2= "UPDATE CISADM.CM_BANK_BILL SET CM_STAT='C' , CM_FILE_NAME='"+fileName+"'  WHERE CM_ASYN_ID="+asynid+"";
				 update=createPreparedStatement(statement2, "update");
				 update.toString().concat(asynid);
				 update.executeUpdate();
            	try 
            	{
            		outputfile.close();
            		statements.close();
            		update.close();
            	}
            	catch (FileNotFoundException e) 
            	{             
            		e.printStackTrace();
            	} 
            	catch (IOException e) 
            	{
            		e.printStackTrace();
            	} 
            	catch (Exception e) 
            	{
            		outputfile.close();
            		e.printStackTrace();
            		return true;
            	}
            } 

            	
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			return true;
		}
		finally
		{
			statements.close();
		}
		return true;
    }
}
}