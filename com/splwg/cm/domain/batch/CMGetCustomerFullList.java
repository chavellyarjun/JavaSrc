package com.splwg.cm.domain.batch;


import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory; 
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.io.File;
import java.io.FilenameFilter;
import com.splwg.base.api.ListFilter;
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
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.DateFormat;
import com.splwg.base.api.datatypes.EntityId;
import com.splwg.base.api.datatypes.LookupHelper;
import com.splwg.base.api.lookup.OutboundMessageProcessingMethodLookup;
import com.splwg.base.domain.common.businessService.BusinessService_Id;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.base.domain.outboundMessage.outboundMessage.OutboundMessage;
import com.splwg.ccb.domain.customerinfo.account.entity.Account;
import com.splwg.ccb.domain.customerinfo.account.entity.AccountCharacteristic;
import com.splwg.ccb.domain.customerinfo.account.entity.AccountCharacteristic_DTO;
import com.splwg.ccb.domain.customerinfo.account.entity.AccountCharacteristic_Per;
import com.splwg.ccb.domain.customerinfo.account.entity.Account_Id;
import com.splwg.ccb.domain.payment.paymentEvent.entity.PaymentTenderCharacteristic_DTO;
import com.splwg.shared.common.ServerMessage;

import java.io.*; 
import java.nio.file.Files; 
import java.nio.file.*; 

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * @author Koushik
 *
@BatchJob (rerunnable = false,modules = { },
 *      softParameters = { @BatchJobSoftParameter (name = INPUT_FILE_PATH, type = string)
 *            , @BatchJobSoftParameter (name = PROCESSED_FILE_PATH, type = string)
 *            , @BatchJobSoftParameter (name = ERROR_FILE_PATH, type = string)
 *            , @BatchJobSoftParameter (name = CORPORATE_CODE, type = string)
 *            , @BatchJobSoftParameter (name = LOGIN_NAME, type = string)
 *            , @BatchJobSoftParameter (name = PASSWORD, type = string)})
 */
public class CMGetCustomerFullList extends CMGetCustomerFullList_Gen {
	static File filePath;
	private static Logger logger = LoggerFactory.getLogger(CMGetCustomerFullList.class); 
	private static COTSInstanceNode bo1Group = null;
	private static COTSInstanceNode bo2Group = null;
	public void validateSoftParameters() 
	{
		//Make error directory if it is not created
		File errorFileDirectory = new File(getParameters().getERROR_FILE_PATH());
		if (!errorFileDirectory.isDirectory()) {
			errorFileDirectory.mkdir();
		}
		
		//Make processed directory if it is not created
		File processedFileDirectory = new File(getParameters().getPROCESSED_FILE_PATH());
		if (!processedFileDirectory.isDirectory()) {
			processedFileDirectory.mkdir();
		}
	}
	public JobWork getJobWork() {
		logger.info("Inside getJobWork");
		List<ThreadWorkUnit> workUnitList= new ArrayList<ThreadWorkUnit>();
		
		//Read all files from input file directory
		 filePath= new File(getParameters().getINPUT_FILE_PATH());
		 ThreadWorkUnit workUnit = new ThreadWorkUnit();
			
			
		 workUnit.addSupplementalData("filePath", filePath);
		 workUnitList.add(workUnit);
		
		System.out.println("file path is "+filePath);
		
		
		return createJobWorkForThreadWorkUnitList(workUnitList);
	}

	public Class<CMGetCustomerFullListWorker> getThreadWorkerClass() {
		return CMGetCustomerFullListWorker.class;
	}

	public static class CMGetCustomerFullListWorker extends
			CMGetCustomerFullListWorker_Gen {

		String corporateCode;
		String loginName;
		String password;
		String inputFilePath;
		String processedFilePath;
		String errorFilePath;
		public ThreadExecutionStrategy createExecutionStrategy() {
			// TODO Auto-generated method stub
			return new StandardCommitStrategy(this);
		}
		private String getVerificationTokenInvoice(String corporateCode, String loginName, String password){
			logger.info("Inside getVerificationTokenInvoice with String corporateCode, String loginName, String password as"+corporateCode+", "+loginName+", "+password);

	 BusinessObjectInstance  businessObjectInstance1= BusinessObjectInstance.create("CM-GetVerificationTokenInvoice");
	 
	 businessObjectInstance1.set("externalSystem", "CM-INVOICE");
	 businessObjectInstance1.set("outboundMessageType", "CM-GETVR");
	 
	 OutboundMessageProcessingMethodLookup addressTypeLookup=LookupHelper.getLookupInstance(OutboundMessageProcessingMethodLookup.class,"F1RT"); 
	 System.out.println("lookup::::::::::::::::"+addressTypeLookup);
	 logger.info("lookup::::::::::::::::"+addressTypeLookup);
	 businessObjectInstance1.set("processingMethod", addressTypeLookup);

	 bo1Group = businessObjectInstance1.getGroup("sendDetails").getGroup("requestMessage").
			 getGroup("getFormsAuthenticationTicket");

	 bo1Group.set("corporateCode", corporateCode);
	 bo1Group.set("loginName", loginName);
	 bo1Group.set("password", password);

	 logger.info("before calling BusinessObjectDispatcher.add");
 	BusinessObjectInstance bsResult1 = BusinessObjectDispatcher.add(businessObjectInstance1);
	 logger.info("after calling BusinessObjectDispatcher.add");

 	String formsAuthenticationTicket = bsResult1.getGroup("responseDetail").getGroup("responseMessage").getGroup("getFormsAuthenticationTicketResponse").getString("getFormsAuthenticationTicketResult");
 		
 	System.out.println("formsAuthenticationTicket is:::"+formsAuthenticationTicket);
 	logger.info("formsAuthenticationTicket is:::"+formsAuthenticationTicket);
	return formsAuthenticationTicket;
}
		
		public void getCustFullList (String formsAuthenticationTicket, String inputFilePath,
				String basePath, String errorFilePath){
			
			 BusinessObjectInstance  businessObjectInstance2= BusinessObjectInstance.create("CM-GetCustFullList");
			 businessObjectInstance2.set("externalSystem", "CM-INVOICE");
			 businessObjectInstance2.set("outboundMessageType", "CM-CUSTFULL");
			 OutboundMessageProcessingMethodLookup addressTypeLookup=LookupHelper.getLookupInstance(OutboundMessageProcessingMethodLookup.class,"F1RT"); 
			 System.out.println("lookup2::::::::::::::::"+addressTypeLookup);
			 logger.info("lookup2::::::::::::::::"+addressTypeLookup);
			 businessObjectInstance2.set("processingMethod", addressTypeLookup);

			 bo2Group = businessObjectInstance2.getGroup("sendDetails").getGroup("requestMessage").
					 getGroup("getEInvoiceCustomerFullList");

			 bo2Group.set("ticket", formsAuthenticationTicket);
			 	BusinessObjectInstance bsResult2 = BusinessObjectDispatcher.add(businessObjectInstance2);

			 	String ServiceResult = bsResult2.getGroup("responseDetails").getGroup("responseMessage").
			 			getGroup("getEInvoiceCustomerFullListResponse").getString("ServiceResult");
				 logger.info("ServiceResult is ::::::::::::::::"+ServiceResult);

			 	if(ServiceResult.equalsIgnoreCase("S")){
			 		try{
			 		//	basePath = "D:\\test";
			 			List<String> zipName =zipFiles(basePath);
						 logger.info("zipName is ::::::::::::::::"+zipName.get(0));

			 			String zipFilePath = basePath + "/" + zipName.get(0);
						 logger.info("zipFilePath is ::::::::::::::::"+zipFilePath);

			 	      //  String destDir = "D:\\result";
			 		   String destDir = inputFilePath;
			 	        unzip(zipFilePath, destDir);
			 			
			 		}
			 		
			 		catch(Exception e){
			 			e.printStackTrace();
			 			
			 		}
			 		
		 	
			 		
			 		
			 	}
			
		}
		
		List<String> zipFiles(String directory) {
			  List<String> textFiles = new ArrayList<String>();
			  File dir = new File(directory);
			  for (File file : dir.listFiles()) {
			    if (file.getName().endsWith((".zip"))) {
			      textFiles.add(file.getName());
			    }
			  }
			  return textFiles;
			}
		
		List<String> xmlFiles(String directory) {
			  List<String> xmlFiles = new ArrayList<String>();
			  File dir = new File(directory);
			  for (File file : dir.listFiles()) {
			    if (file.getName().endsWith((".xml"))) {
			    	xmlFiles.add(file.getName());
			    }
			  }
			  return xmlFiles;
			}
		
		
		
		private static void unzip(String zipFilePath, String destDir) {
	        File dir = new File(destDir);
	        // create output directory if it doesn't exist
	        if(!dir.exists()) dir.mkdirs();
	        FileInputStream fis;
	        //buffer for read and write data to file
	        byte[] buffer = new byte[1024];
	        try {
	            fis = new FileInputStream(zipFilePath);
	            ZipInputStream zis = new ZipInputStream(fis);
	            ZipEntry ze = zis.getNextEntry();
	            while(ze != null){
	                String fileName = ze.getName();
	                File newFile = new File(destDir + File.separator + fileName);
	                System.out.println("Unzipping to "+newFile.getAbsolutePath());
	                //create directories for sub directories in zip
	                new File(newFile.getParent()).mkdirs();
	                FileOutputStream fos = new FileOutputStream(newFile);
	                int len;
	                while ((len = zis.read(buffer)) > 0) {
	                fos.write(buffer, 0, len);
	                }
	                fos.close();
	                //close this ZipEntry
	                zis.closeEntry();
	                ze = zis.getNextEntry();
	            }
	            //close last ZipEntry
				 logger.info("close last ZipEntry ::::::::::::::::");

	            zis.closeEntry();
	            zis.close();
	            fis.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	        
	    }
		
		
		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			
			
			// TODO Auto-generated method stub
		   	corporateCode = getParameters().getCORPORATE_CODE();
		   	loginName = getParameters().getLOGIN_NAME();
		   	password = getParameters().getPASSWORD();
		   	
		   	inputFilePath = getParameters().getINPUT_FILE_PATH();
			processedFilePath=getParameters().getPROCESSED_FILE_PATH();
			errorFilePath=getParameters().getERROR_FILE_PATH();
	
			 try {
				 String verificationTokenInvoice = getVerificationTokenInvoice(corporateCode,loginName,password);
				 String basePath="/scratch/sploutput/inbound/invoice";
				// String basePath="C:\\Users\\Arnab\\Desktop\\D1\\filePath";
				// C:\\Users\\Arnab\\Desktop\\D1\\filePath
				 // String basePath="D:\\test";
					 getCustFullList(verificationTokenInvoice,inputFilePath,basePath,errorFilePath);
				 
					File [] files = filePath.listFiles();
					String fileName = null;
					
					if (files != null) {
						System.out.println("in files");
						//Process the input files
						DateFormat dateFormat = new DateFormat("yyyy_MM_dd_HH_mm_ss") ;
						String dateTime= dateFormat.format(getProcessDateTime());
						
						for (File file : files){
							fileName = file.getName();

							String inputXml = fileName;
							System.out.println("file name is "+fileName);
							if(inputXml.contains(".")){
								inputXml = inputXml.split("\\.")[0];
								System.out.println("file name is "+inputXml);
							}
							
							else{
								//
							}
							String processedRecFileName = inputXml + "_processsed_" + dateTime + ".xml";
							String errorRecFileName = inputXml + "_error_" + dateTime + ".xml";
							
							String inputFilePath = getParameters().getINPUT_FILE_PATH().trim();
							File inFile = new File(inputFilePath + "/" + fileName);
							
							try 
							{
								 logger.info("In try!!!!!!!!! line 326");

							    File fXmlFile = new File(fileName);
							    System.out.println("file path is "+fXmlFile);
							    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
							    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
							    System.out.println("/////");
							    Document doc = dBuilder.parse(inFile);
							    System.out.println("document is "+doc);
							    doc.getDocumentElement().normalize();
								 

							    System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
							    NodeList nList = doc.getElementsByTagName("EInvoiceUser");
							    System.out.println("----------------------------");
								 logger.info("before for!!!!!!!!! line 341 length is::"+nList.getLength());

							    for (int temp = 0; temp < nList.getLength(); temp++) {
							        Node nNode = nList.item(temp);
							        System.out.println("\nCurrent Element :" + nNode.getNodeName());
							        if (nNode.getNodeType() == Node.ELEMENT_NODE) {
							            Element eElement = (Element) nNode;
							            //System.out.println("Staff id : "
							              //                 + eElement.getAttribute("id"));
							            System.out.println("vkntckn : "
							                               + eElement.getElementsByTagName("vkntckn")
							                                 .item(0).getTextContent());
							            String vkntckn = eElement.getElementsByTagName("vkntckn")
				                                 .item(0).getTextContent();
							             System.out.println("length is:::"+vkntckn.length());
							            if(vkntckn.length() == 10)
							            {
							            	BusinessServiceInstance  businessServiceInstance= BusinessServiceInstance.create("CM-ACCTTUR");
							            	businessServiceInstance.set("idType", "VKN");
							            	businessServiceInstance.set("perIdNumber", vkntckn);
							            	
							            	BusinessServiceInstance bsResult = BusinessServiceDispatcher.execute(businessServiceInstance);
							            	COTSInstanceList resultList = bsResult.getList("results");
							            	Iterator<COTSInstanceListNode> resultListIter = resultList.iterator();
							            	System.out.println("Size of the result list is " + resultList.getSize());
							            	
							            	if(resultList.getSize() != 0)
							            	{
							            		COTSInstanceListNode ListNode = resultListIter.next();
							                    System.out.println(ListNode.getString("perId"));
							                    System.out.println(ListNode.getString("acctId"));
							                    
							                    Account account = new Account_Id(ListNode.getString("acctId")).getEntity();
							                    System.out.println("After Account"+ListNode.getString("acctId"));

							                    ListFilter<AccountCharacteristic> AccountCharsFilter = account.getCharacteristics().createFilter("where this.id.characteristicType = :chrTypeCode ", "");
							                    CharacteristicType_Id chId  = new CharacteristicType_Id("CM-EBILL");
							                    AccountCharsFilter.bindId("chrTypeCode", chId);
							                
							                    AccountCharacteristic AccountChar= AccountCharsFilter.firstRow();
							                
							                    
							                    if(isNull(AccountChar))
							                    
							                    {
							                    	System.out.println("insode adding char");
							                    BusinessObjectInstance  businessObjectInstance1= BusinessObjectInstance.create("C1AccountPhysical");
							                    businessObjectInstance1.set("accountId", ListNode.getString("acctId"));
							                    COTSInstanceList payPsListForBS = businessObjectInstance1.getList("accountCharacteristic"); 
							            		COTSInstanceListNode nodePayPs = payPsListForBS.newChild(); 
							            		//nodePayPs.set("accountId", "acctId");
							                    nodePayPs.set("characteristicType", "CM-EBILL");
							                    nodePayPs.set("characteristicValue", "Y");
							                    nodePayPs.set("effectiveDate", getProcessDateTime().getDate());
							                 
								            	
								            	BusinessObjectInstance bsResult1 = BusinessObjectDispatcher.update(businessObjectInstance1);
								            
								            	System.out.println("Added account chars"+bsResult1.getDocument().asXML().toString());								            			//.execute(businessServiceInstance1);
							            	}
														
							                    else if (AccountChar.fetchCharacteristicValue().getId().getCharacteristicValue().trim().equals("N"))
							                    {
							                    	  
							                    	AccountCharacteristic_Per accountCharacteristicPer= (AccountCharacteristic_Per) account.getCharacteristics().createFilter( "where this.id.characteristicType = 'CM-EBILL'" , "").firstRow();
							                    	accountCharacteristicPer.setCharacteristicValue("Y");
							                    									            			//.execute(businessServiceInstance1);
							            	}
							                    
							            }
							            }
							            
							            else if(vkntckn.length() == 11)
							            {
							            	System.out.println("length 11");
							            	BusinessServiceInstance  businessServiceInstance1= BusinessServiceInstance.create("CM-ACCTTUR");
							            	businessServiceInstance1.set("idType", "TCN");
							            	businessServiceInstance1.set("perIdNumber", vkntckn);
							            	
							            	BusinessServiceInstance bsResult1 = BusinessServiceDispatcher.execute(businessServiceInstance1);
							            	COTSInstanceList resultList1 = bsResult1.getList("results");
							            	Iterator<COTSInstanceListNode> resultListIter1 = resultList1.iterator();
							            	System.out.println("Size of the result list is " + resultList1.getSize());
							            	
							            	if(resultList1.getSize() != 0)
							            	{
							            		COTSInstanceListNode ListNode = resultListIter1.next();
							                    System.out.println(ListNode.getString("perId"));
							                    System.out.println(ListNode.getString("acctId"));
							                    
							                    Account account = new Account_Id(ListNode.getString("acctId")).getEntity();
							                    System.out.println("After Account"+ListNode.getString("acctId"));

							                    ListFilter<AccountCharacteristic> AccountCharsFilter = account.getCharacteristics().createFilter("where this.id.characteristicType = :chrTypeCode ", "");
							                    CharacteristicType_Id chId  = new CharacteristicType_Id("CM-EBILL");
							                    AccountCharsFilter.bindId("chrTypeCode", chId);
							                
							                    AccountCharacteristic AccountChar= AccountCharsFilter.firstRow();
							                
							                    
							                    if(isNull(AccountChar))
							                    
							                    {
							                    	System.out.println("insode adding char");
							                    BusinessObjectInstance  businessObjectInstance1= BusinessObjectInstance.create("C1AccountPhysical");
							                    businessObjectInstance1.set("accountId", ListNode.getString("acctId"));
							                    COTSInstanceList payPsListForBS = businessObjectInstance1.getList("accountCharacteristic"); 
							            		COTSInstanceListNode nodePayPs = payPsListForBS.newChild(); 
							            		//nodePayPs.set("accountId", "acctId");
							                    nodePayPs.set("characteristicType", "CM-EBILL");
							                    nodePayPs.set("characteristicValue", "Y");
							                    nodePayPs.set("effectiveDate", getProcessDateTime().getDate());
							                 
								            	
								            	BusinessObjectInstance bsResult11 = BusinessObjectDispatcher.update(businessObjectInstance1);
								            
								            	System.out.println("Added account chars"+bsResult11.getDocument().asXML().toString());//.execute(businessServiceInstance1);
							            	}
														
							                    else if (AccountChar.fetchCharacteristicValue().getId().getCharacteristicValue().trim().equals("N"))
							                    {
							                    	
							                    	AccountCharacteristic_Per accountCharacteristicPer= (AccountCharacteristic_Per) account.getCharacteristics().createFilter( "where this.id.characteristicType = 'CM-EBILL'" , "").firstRow();
							                    	accountCharacteristicPer.setCharacteristicValue("Y");
							                    									            			//.execute(businessServiceInstance1);
							            	}
							            	}
							            	
							            }
							            
							            
							            
							            
							        }
							        
							    }
							    
							    
							    List<String> xmlNameL =xmlFiles(inputFilePath);
								 String xmlName=xmlNameL.get(0);
								 logger.info("xmlName is::::::"+xmlName);
								 String fromPath= inputFilePath + "/" + xmlName;
					            
								 String toPath= processedFilePath + "/" + xmlName;
					            
					            Path tempPath = Files.move 
					                    (Paths.get(fromPath),  
					                    Paths.get(toPath)); 
					            
					            logger.info("Move status is::::::"+tempPath);
					          		
							    
							}catch (FileNotFoundException exception) {
								ServerMessage message= new ServerMessage();
								message.setMessageText(exception.getMessage());
							} catch (IOException exception) {
								//Move the input file to error directory
								File errorPath = new File(getParameters().getERROR_FILE_PATH() + "/" + errorRecFileName);
								if(inFile.renameTo(errorPath)){
									inFile.delete();
									
								}
							}
						}
				 
					} 
					else{
						System.out.println("no files");
					}
					
					 
				 
			 } catch (Exception e) {
				 e.printStackTrace();
				 logger.info("In Catch!!!!!!!!! line 473"+e);
					
				    }
		
		
		    return true;
			
		}
		
		private String addSupplementalData(String string, String string2) {
			// TODO Auto-generated method stub
			return null;
		}

	}

}
