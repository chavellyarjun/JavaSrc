package com.splwg.cm.domain.batch;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import tr.gov.nvi.kpsv2.model.BilesikKutukModel;
import tr.gov.nvi.kpsv2.sample.beans.AyarlarBean;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.ListFilter;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.businessObject.COTSInstanceList;
import com.splwg.base.api.businessObject.COTSInstanceListNode;
import com.splwg.base.api.businessObject.COTSInstanceNode;
import com.splwg.base.api.businessService.BusinessServiceDispatcher;
import com.splwg.base.api.businessService.BusinessServiceInstance;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.LookupHelper;
import com.splwg.base.api.lookup.OutboundMessageProcessingMethodLookup;
import com.splwg.base.api.serviceScript.ServiceScriptDispatcher;
import com.splwg.base.api.serviceScript.ServiceScriptInstance;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.businessService.BusinessService;
import com.splwg.base.domain.common.businessService.BusinessService_Id;
import com.splwg.ccb.domain.customerinfo.customerContact.entity.CustomerContactCharacteristic;
import com.splwg.ccb.domain.customerinfo.customerContact.entity.CustomerContact_Id;
import com.splwg.ccb.domain.customerinfo.person.entity.PersonCharacteristics;
import com.splwg.ccb.domain.customerinfo.person.entity.PersonId;
import com.splwg.ccb.domain.customerinfo.person.entity.PersonId_Id;
import com.splwg.ccb.domain.customerinfo.person.entity.PersonIds;
import com.splwg.ccb.domain.customerinfo.person.entity.Person_Id;
import com.splwg.shared.common.ApplicationError;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Arnab
 *
@BatchJob (rerunnable = false,modules = { },
 *      softParameters = { @BatchJobSoftParameter (name = STATUS, type = string)
 *            , @BatchJobSoftParameter (name = USER_NAME, type = string)
 *            , @BatchJobSoftParameter (name = PASSWORD, type = string)})
 */

public class CMIdValidationBatch extends CMIdValidationBatch_Gen{
	private static Logger logger = LoggerFactory.getLogger(CMIdValidationBatch.class);
	private static COTSInstanceNode bo1Group = null;
	
	public JobWork getJobWork() {
		logger.info("in getJobWork");
		String status = getParameters().getSTATUS();
		
		
		List<ThreadWorkUnit> workUnits = new ArrayList<ThreadWorkUnit>();
		String per_ids = "SELECT * FROM CI_PER_CHAR where char_type_cd='CM-IDVAL' and (ADHOC_CHAR_VAL ='"+status+"' or CHAR_VAL ='"+status+"')";
		//4290521103
		//9676198136
		PreparedStatement per_idsst = createPreparedStatement(per_ids,
				"getper_ids");
		per_idsst.execute();
		List<SQLResultRow> per_idsList = per_idsst.list();
		logger.info("in getJobWork COUNT" + per_idsList.size());
		if (per_idsList.size() > 0) {
			for (SQLResultRow row : per_idsList) {
				ThreadWorkUnit workUnit = new ThreadWorkUnit();
				workUnit.addSupplementalData("per_id", row.getString("PER_ID"));

				workUnits.add(workUnit);
			}

		}
		return createJobWorkForThreadWorkUnitList(workUnits);
	}

	public Class<CMIdValidationBatchWorker> getThreadWorkerClass() {
		return CMIdValidationBatchWorker.class;
	}

	public static class CMIdValidationBatchWorker extends CMIdValidationBatchWorker_Gen {
		String per_id;
		private static COTSInstanceList boList = null;

		public ThreadExecutionStrategy createExecutionStrategy() {
			// TODO Auto-generated method stub
			return new StandardCommitStrategy(this);
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			logger.info("PRINTING HERE");
			logger.info("in JAVA main");
			
			String 	loginName = getParameters().getUSER_NAME().trim(); //KRM-12131640
			//String 	loginName = "KRM-12131640";

			String 	password = getParameters().getPASSWORD().trim(); //.dep632334saS
			// String 	password = ".dep632334saS"; //.dep632334saS

			per_id = (String) unit.getSupplementallData("per_id");
			Person_Id perid = new Person_Id(per_id);
			logger.info("per_id is::::"+per_id);
			logger.info("per_id is!!!!::::"+per_id);
			
			PersonIds perIds = perid.getEntity().getIds();
			
            for(Iterator<PersonId> personId = perIds.iterator();personId.hasNext();)
            {
                            
            	PersonId pid = personId.next();
            	
            	String IdType = pid.fetchIdIdType().getId().getIdValue().toString().trim();
            	String IdVal = pid.getPersonIdNumber().trim();
            	String personIdS=pid.getId().toString();
            	logger.info("pid is:::"+pid);

            	logger.info("person ID is:::"+per_id);
            	logger.info("IdType is::::: "+IdType);
            	
            	
            	logger.info("IdVal is::::: "+IdVal);
            	 long turkishId = Long.parseLong(IdVal);
            	if(IdType.equalsIgnoreCase("VKN")){
            		
            		ServiceScriptInstance scriptInstance = ServiceScriptInstance
   							.create("CM-IDBatVal");
   					scriptInstance.getElement().addElement("idnumber")
   					.setText(IdVal);
   					scriptInstance.getElement().addElement("IdType")
   					.setText("VKN");
   					scriptInstance.getElement().addElement("perId")
   					.setText(per_id);
   					ServiceScriptDispatcher.invoke(scriptInstance);
            		
            	}
            	else if(IdType.equalsIgnoreCase("TCN")){
            		String dob=null;
          		  String place=null;
          		  String nviStatus="N";
          		  String motherName=null;
          		  String fatherName=null;
//            		BusinessService_Id bsID = new BusinessService_Id("CM-NVIDataRetrieve");
//                    BusinessServiceInstance ins = BusinessServiceInstance.create(bsID.getEntity());
//                    
//                    COTSInstanceNode group = ins.getGroup("request");
//                    group.set("tcKimlikNo", IdVal);
//                    group.set("userName", "KRM-10766780");
//                    group.set("password", "Eks@Dic34!");
//                    logger.info("bs executed====");
//                    BusinessServiceInstance bsInstance = BusinessServiceDispatcher.execute(ins);    
//                    COTSInstanceNode   result = bsInstance.getGroup("response");
//                    logger.info("the list"+result);
//                    System.out.println("value"+result.getXMLString("response"));
////                    
            		logger.info("user Name is ::: "+loginName);
            		logger.info("password is  ::: "+password);
            		
            		 AyarlarBean ayarlarBean = new AyarlarBean();
            		 ayarlarBean.setUsername(loginName);
            		 ayarlarBean.setPassword(password);
            		 logger.info("started testing!!!");
            	      BilesikKutukModel result = ayarlarBean.ayarla(turkishId);
            	      logger.info("After ayarlarBean.ayarla----"+result);
                   // for selected SA multiple debt age
            		  if(result!=null){
            			  
            		      String Ad= result.getAd();
            		      String DogumYeri = result.getDogumYeri();
            		      System.out.println("Ad is:"+Ad);

            		      String Soyad= result.getSoyad();
            		      System.out.println("Soyad is:"+Soyad);

            		      String annaAd= result.getAnnaAd();
            		      String babaAd= result.getBabaAd();
            		      System.out.println("annaAd is:"+annaAd);
            		      System.out.println("babaAd is:"+babaAd);
            		      String DogumTarih=null;
            		      if(result.getDogumTarih()!=null){
            		      DogumTarih= result.getDogumTarih().toString();
            		      }
            		      System.out.println("DogumTarih is:"+DogumTarih);

            		      long CuzdanTcKimlikNoL= result.getCuzdanTcKimlikNo();
            		      String CuzdanTcKimlikNo=String.valueOf(CuzdanTcKimlikNoL);
            		      System.out.println("CuzdanTcKimlikNo is:"+CuzdanTcKimlikNo);

            		      String CuzdanAd= result.getCuzdanAd();
            		      System.out.println("CuzdanAd is:"+CuzdanAd);

            		      String CuzdanSoyad= result.getCuzdanSoyad();
            		      System.out.println("CuzdanSoyad is:"+CuzdanSoyad);
            		      String CuzdanAnnaAd= result.getCuzdanAnnaAd();

            		      String CuzdanDogumYeri= result.getCuzdanDogumYeri();
            		      String CuzdanBabaAd= result.getCuzdanBabaAd();
            		      System.out.println("CuzdnAnnaAd is:"+CuzdanAnnaAd);
            		      System.out.println("CuzdanBabaAd is:"+CuzdanBabaAd);
            		      
            		      
            		      String Seri= result.getSeri();
            		      System.out.println("Seri is:"+Seri);

            		      int NoI= result.getNo();
            		      String No = String.valueOf(NoI);
            		      System.out.println("No is:"+No);

            		      long TckkTcKimlikNoL= result.getTckkTcKimlikNo();
            		      String TckkTcKimlikNo = String.valueOf(TckkTcKimlikNoL);
            		      System.out.println("TckkTcKimlikNo is:"+TckkTcKimlikNo);

            		      String TckkAd= result.getTckkAd();
            		      System.out.println("TckkAd is:"+TckkAd);

            		      String TckkDogumYeri= result.getTckkDogumYeri();

            		      
            		      String TckkSoyad= result.getTckkSoyad();
            		      System.out.println("TckkSoyad is:"+TckkSoyad);

            		      String TckkAnnaAd= result.getTckkAnnaAd();
            		      String TckkBabaAd= result.getTckkBabaAd();
            		      System.out.println("TckkAnnaAd is:"+TckkAnnaAd);
            		      System.out.println("TckkBabaAd is:"+TckkBabaAd);
            		      
            		      
            		      String TckkSeriNo= result.getTckkSeriNo();
            		      System.out.println("TckkSeriNo is:"+TckkSeriNo);

            		      long GeciciKimlikTcKimlikNoL= result.getGeciciKimlikTcKimlikNo();
            		      String GeciciKimlikTcKimlikNo = String.valueOf(GeciciKimlikTcKimlikNoL);
            		      System.out.println("GeciciKimlikTcKimlikNo is:"+GeciciKimlikTcKimlikNo);

            		      String GeciciKimlikAd= result.getGeciciKimlikAd();
            		      System.out.println("GeciciKimlikAd is:"+GeciciKimlikAd);

            		      String GeciciKimlikSoyad= result.getGeciciKimlikSoyad();
            		      System.out.println("GeciciKimlikSoyad is:"+GeciciKimlikSoyad);

            		      String GeciciKimlikAnnaAd= result.getGeciciKimlikAnnaAd();
            		      String GeciciKimlikBabaAd= result.getGeciciKimlikBabaAd();
            		      System.out.println("GeciciKimlikAnnaAd is:"+GeciciKimlikAnnaAd);
            		      System.out.println("GeciciKimlikBabaAd is:"+GeciciKimlikBabaAd);
            		      
            		      
            		      
            		      long MaviKartliKimlikNoL= result.getMaviKartliKimlikNo();
            		      String MaviKartliKimlikNo = String.valueOf(MaviKartliKimlikNoL);
            		      System.out.println("MaviKartliKimlikNo is:"+MaviKartliKimlikNo);

            		      String MaviKartliAd= result.getMaviKartliAd();
            		      String MaviKartliDogumYeri= result.getMaviKartliDogumYeri();
            	 
            		      System.out.println("MaviKartliAd is:"+MaviKartliAd);

            		      String MaviKartliSoyad= result.getMaviKartliSoyad();
            		      System.out.println("MaviKartliSoyad is:"+MaviKartliSoyad);

            		      String MaviKartliAnnaAd= result.getMaviKartliAnnaAd();
            		      String MaviKartliBabaAd= result.getMaviKartliBabaAd();
            		      System.out.println("MaviKartliAnnaAd is:"+MaviKartliAnnaAd);
            		      System.out.println("MaviKartliBabaAd is:"+MaviKartliBabaAd);
            		      
            		      
            		      String MaviKartliDogumTarih=null;

            		      if(result.getMaviKartliDogumTarih()!=null){
            		      MaviKartliDogumTarih= result.getMaviKartliDogumTarih().toString();
            		      }
            		      System.out.println("MaviKartliDogumTarih is:"+MaviKartliDogumTarih);

            		      long MaviKartKimlikNoL= result.getMaviKartKimlikNo();
            		      String MaviKartKimlikNo = String.valueOf(MaviKartKimlikNoL);
            		      System.out.println("MaviKartKimlikNo is:"+MaviKartKimlikNo);

            		      String MaviKartAd= result.getMaviKartAd();
            		      String MaviKartDogumYeri= result.getMaviKartDogumYeri();
            	 
            		      System.out.println("MaviKartAd is:"+MaviKartAd);

            		      String MaviKartSoyad= result.getMaviKartSoyad();
            		      System.out.println("MaviKartSoyad is:"+MaviKartSoyad);

            		      String MaviKartAnnaAd= result.getMaviKartAnnaAd();
            		      String MaviKartBabaAd= result.getMaviKartBabaAd();
            		      System.out.println("MaviKartAnnaAd is:"+MaviKartAnnaAd);
            		      System.out.println("MaviKartBabaAd is:"+MaviKartBabaAd);
            		      
            		      
            		      String MaviKartSeri= result.getMaviKartSeri();
            		      System.out.println("MaviKartSeri is:"+MaviKartSeri);

            		      long MaviKartNoL= result.getMaviKartNo();
            		      String MaviKartNo = String.valueOf(MaviKartNoL);
            		      System.out.println("MaviKartNo is:"+MaviKartNo);

            		      long YbKimlikNoL = result.getYbKimlikNo();
            		      String YbKimlikNo = String.valueOf(YbKimlikNoL);
            		      System.out.println("YbKimlikNo is:"+YbKimlikNo);

            		      String YbAd= result.getYbAd();
            		      String YbDogumYeri= result.getYbDogumYeri();
            	 
            		      System.out.println("YbAd is:"+YbAd);

            		      String YbSoyad= result.getYbSoyad();
            		      System.out.println("YbSoyad is:"+YbSoyad);
            		      
            		      String YbAnnaAd= result.getYbAnnaAd();
            		      String YbBabaAd= result.getYbBabaAd();
            		      System.out.println("YbAnnaAd is:"+YbAnnaAd);
            		      System.out.println("YbBabaAd is:"+YbBabaAd);
            		      
            		      String YbDogumTarih=null;
            		      if(result.getYbDogumTarih()!=null){
            		      YbDogumTarih= result.getYbDogumTarih().toString();
            		      }
            		  
            		  
            		  if(DogumTarih!=null || MaviKartliDogumTarih!=null || YbDogumTarih!=null){
            			  
if(DogumTarih!=null){
 dob =   DogumTarih;  
 nviStatus = "Y";
            			  }
if(MaviKartliDogumTarih!=null){
	 dob =   MaviKartliDogumTarih;         				  
	 nviStatus = "Y";
}
			if(YbDogumTarih!=null){
	 dob =   YbDogumTarih;         				  
	 nviStatus = "Y";
}
            		  }
System.out.println("dob is:: "+dob);
logger.info("dob is:: "+dob);

if(DogumYeri!=null || CuzdanDogumYeri!=null || TckkDogumYeri!=null || MaviKartliDogumYeri!=null || MaviKartDogumYeri!=null || YbDogumYeri!=null){
	  
if(DogumYeri!=null){
place =   DogumYeri;  
nviStatus = "Y";
	  }
if(CuzdanDogumYeri!=null){
	place =   CuzdanDogumYeri;         				  
nviStatus = "Y";
}
if(TckkDogumYeri!=null){
	place =   TckkDogumYeri;         				  
nviStatus = "Y";
}
if(MaviKartliDogumYeri!=null){
	place =   MaviKartliDogumYeri;  
nviStatus = "Y";
	  }
if(MaviKartDogumYeri!=null){
	place =   MaviKartDogumYeri;         				  
nviStatus = "Y";
}
if(YbDogumYeri!=null){
	place =   YbDogumYeri;         				  
nviStatus = "Y";
}
}
System.out.println("place is:: "+place);
logger.info("place is:: "+place);


if(annaAd!=null || CuzdanAnnaAd!=null || TckkAnnaAd!=null || GeciciKimlikAnnaAd!=null || MaviKartliAnnaAd!=null || MaviKartAnnaAd!=null || YbAnnaAd!=null){
	  
if(annaAd!=null){
	motherName =   annaAd;  
nviStatus = "Y";
	  }
if(CuzdanAnnaAd!=null){
	motherName =   CuzdanAnnaAd;         				  
nviStatus = "Y";
}
if(TckkAnnaAd!=null){
	motherName =   TckkAnnaAd;         				  
nviStatus = "Y";
}
if(GeciciKimlikAnnaAd!=null){
	motherName =   GeciciKimlikAnnaAd;  
nviStatus = "Y";
	  }
if(MaviKartliAnnaAd!=null){
	motherName =   MaviKartliAnnaAd;         				  
nviStatus = "Y";
}
if(MaviKartAnnaAd!=null){
	motherName =   MaviKartAnnaAd;         				  
nviStatus = "Y";
}
if(YbAnnaAd!=null){
	motherName =   YbAnnaAd;         				  
nviStatus = "Y";
}
}
System.out.println("motherName is:: "+motherName);
logger.info("motherName is:: "+motherName);


if(babaAd!=null || CuzdanBabaAd!=null || TckkBabaAd!=null || GeciciKimlikBabaAd!=null || MaviKartliBabaAd!=null || MaviKartBabaAd!=null || YbBabaAd!=null){
	  
if(babaAd!=null){
	fatherName =   babaAd;  
nviStatus = "Y";
	  }
if(CuzdanBabaAd!=null){
	fatherName =   CuzdanBabaAd;         				  
nviStatus = "Y";
}
if(TckkBabaAd!=null){
	fatherName =   TckkBabaAd;         				  
nviStatus = "Y";
}
if(GeciciKimlikBabaAd!=null){
	fatherName =   GeciciKimlikBabaAd;  
nviStatus = "Y";
	  }
if(MaviKartliBabaAd!=null){
	fatherName =   MaviKartliBabaAd;         				  
nviStatus = "Y";
}
if(MaviKartBabaAd!=null){
	fatherName =   MaviKartBabaAd;         				  
nviStatus = "Y";
}
if(YbBabaAd!=null){
	fatherName =   YbBabaAd;         				  
nviStatus = "Y";
}
}
System.out.println("fatherName is:: "+fatherName);
logger.info("fatherName is:: "+fatherName);



            		  ServiceScriptInstance scriptInstance = ServiceScriptInstance
     							.create("CM-IDBatVal");
     					scriptInstance.getElement().addElement("idnumber")
     					.setText(IdVal);
     					scriptInstance.getElement().addElement("IdType")
     					.setText("TCN");
     					scriptInstance.getElement().addElement("perId")
     					.setText(per_id);
     					scriptInstance.getElement().addElement("birthDate")
     					.setText(dob);
     					scriptInstance.getElement().addElement("nviStatus")
     					.setText(nviStatus);
     					scriptInstance.getElement().addElement("fatherName")
     					.setText(fatherName);
     					scriptInstance.getElement().addElement("motherName")
     					.setText(motherName);
     					scriptInstance.getElement().addElement("birthPlace")
     					.setText(place);
     					ServiceScriptDispatcher.invoke(scriptInstance);        		  
            		  

            	}
            	}
            }
            return true;

		}
		public void finalizeThreadWork() throws ThreadAbortedException,
		RunAbortedException {
		}

	}

}
