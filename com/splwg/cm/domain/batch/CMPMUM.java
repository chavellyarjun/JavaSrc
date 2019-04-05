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
import com.splwg.shared.common.LoggedException;
import com.splwg.base.api.datatypes.DateFormat;
import com.ibm.icu.impl.Row;
import com.ibm.icu.text.SimpleDateFormat;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.SingleTransactionStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.service.DataElement;
import com.splwg.base.api.service.PageHeader;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;
/**
 * @author MadhaviMannula
 * 
 *
@BatchJob (rerunnable = false,modules={ }, 
       softParameters = { @BatchJobSoftParameter (name = INPUT_FILE_PATH, required = true, type = string)})
 */
public class CMPMUM extends CMPMUM_Gen
{
	private static final Logger log = LoggerFactory.getLogger(CMPMUM.class);
	
	public JobWork getJobWork() 
	{
		 List<ThreadWorkUnit> dummyThreadWorkUnitsList = new ArrayList<ThreadWorkUnit>();
	   	 ThreadWorkUnit dummyThreadWorkUnit = new ThreadWorkUnit();
		 dummyThreadWorkUnitsList.add(dummyThreadWorkUnit);
    	 return createJobWorkForThreadWorkUnitList(dummyThreadWorkUnitsList);	
	}

	public Class<CMPMUMWorker> getThreadWorkerClass() 
	{
		return CMPMUMWorker.class;
	}

	public static class CMPMUMWorker extends CMPMUMWorker_Gen 
	{
		public ThreadExecutionStrategy createExecutionStrategy()
		{
			
			return new SingleTransactionStrategy(this);
		}
		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException 
		{
			String statement="";
			PreparedStatement statements=null;
			try {
			System.out.println("in execute work unit");
			System.out.println("getProcessDateTime().getDate()"+getProcessDateTime().getDate());
			 statement="select distinct adhoc_char_val from ci_sa sa,D1_SP_IDENTIFIER sp,d1_sp_char spchar"
	        		+ " where sa.sa_status_flg='40' AND sa.end_dt=:date AND  sa.CHAR_PREM_ID IN "
	        		+ " (SELECT ID_VALUE FROM D1_SP_IDENTIFIER WHERE SP_ID_TYPE_FLG='D1EP'and d1_sp_id=spchar.d1_sp_id)and "
	        		+ " sp.d1_sp_id=spchar.d1_sp_id and char_type_cd='CM-ETSO' ";
			 
	         statements=createPreparedStatement(statement, "getAccount");
	         statements.bindDate("date", getProcessDateTime().getDate());
	         
	    
			
			DateFormat dateFormat = new DateFormat("yyyy_MM_dd_HH_mm_ss") ;
 			String dateTime= dateFormat.format(getProcessDateTime());
 			//adding file name and type
    		String fileName="PMUM"+dateTime+".xls";

    
            String filePath =getParameters().getINPUT_FILE_PATH();
    		String destFilePath = filePath.concat(fileName);
    	    //creating new file
    		File destFile = new File(destFilePath);
    		 FileWriter outputfile;
			System.out.println("list size"+statements.list().size());
			
				outputfile = new FileWriter(destFile);				
				List<SQLResultRow> accountQueryList = statements.list();
				if (accountQueryList.size() > 0) 
				{
				     for (SQLResultRow row : accountQueryList){
				     String[] header = {row.getString("ADHOC_CHAR_VAL")}; 
				     System.out.println("row.getStrng(ADHOC_CHAR_VAL)"+row.getString("ADHOC_CHAR_VAL")); 
				     //writing data into the excel sheet
				     outputfile.write(row.getString("ADHOC_CHAR_VAL"));
				     outputfile.append("\n");
					 destFilePath = "";
    	        }
    	        try 
    	        {
    	           
    	            outputfile.close();
    	            statements.close();
    	        }
    	        catch (FileNotFoundException e) 
    	        {             
    	            e.printStackTrace();
			    } 
    	        catch (IOException e) 
    	        {
    	        	// TODO Auto-generated catch block
    	        	e.printStackTrace();
			    } 
    	        catch (Exception e) 
    	        {
    	        	// TODO Auto-generated catch block
    	        	outputfile.close();
			        e.printStackTrace();
			        return true;
    	        }                       
				} 
			
		    }
	        catch (Exception e) 
			       {
				         // TODO Auto-generated catch block
				         e.printStackTrace();
				         return true;
		           }
			finally {
				statements.close();
	        }
			       return true;
		    }
		
		    public void finalizeThreadWork() throws ThreadAbortedException,
		    RunAbortedException {}
      }
}