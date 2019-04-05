package com.splwg.cm.domain.GIB.validateGIB;


import java.util.HashMap;

import com.splwg.base.api.service.DataElement;
import com.splwg.base.api.service.PageHeader;
import com.splwg.base.support.service.metainfo.PageHeaderField;
import com.splwg.shared.common.ApplicationError;
import com.splwg.shared.common.ServerMessage;


/**
 * @author Arnab
 *
** @PageMaintenance (secured = false, service = GIBVAL,
*      body = @DataElement (contents = { @DataField (name = CM_TAXSTATUS)}),
*      actions = { "read"},
*      header = { @DataField (name = CM_GIBID)},
*      headerFields = { @DataField (name = CM_GIBID)},
*     
*      modules = {})     
*/

public class GIBNumberValidation extends GIBNumberValidation_Gen{
	@Override
	public DataElement read(PageHeader header) throws ApplicationError {
		DataElement element = new DataElement();
		String taxId1 = header.getString(STRUCTURE.HEADER.CM_GIBID);

   	 try{
   	 HashMap<Integer, Long> storeDigits=new HashMap<Integer, Long>();
			//String taxId1 = "2950469551";
			 
			String actualNumber=taxId1.trim();
		      int seq=1;
		      String result="false";
		      
		     if(actualNumber.matches("[0-9]+"))
		     {
		       
		     if(actualNumber.length()==10)
		     {
		      
		        
		        int counter = 10;
		        
		        long m,a;
		      
		        
		        m=Long.parseLong(actualNumber);
		        
		       
		        while(m > 0)
		        {
		          a = m % 10;
		         
		          storeDigits.put(counter,a);
		          
		          m= (long)(m / 10);
		          counter--;
		        }
		      
		      
		      long one=(storeDigits.get(1)+ 9)%10;

		      long two=(storeDigits.get(2) + 8)%10;
		     
		      long three=(storeDigits.get(3) +7)%10;
		      long four=(storeDigits.get(4) + 6)%10;
		      long five=(storeDigits.get(5) + 5)%10;
		      long six=(storeDigits.get(6) +4 )%10;
		      long seven=(storeDigits.get(7) + 3)%10;
		      long eight=(storeDigits.get(8) + 2)%10;

		      long nine=(storeDigits.get(9) + 1)%10;
		      long ten = storeDigits.get(10);
		      long newNumber=(one*100000000)+(two*10000000)+(three*1000000)+(four*100000)
		    		  +(five*10000)+(six*1000)+(seven*100)+(eight*10)+nine;
		      
		      long oneM= sumDigits(one*512);
		      if(oneM%9==0){
		    	  if(oneM==0)
		    		  oneM=0;
		    	  else
		    	  oneM=9;
		      }
		      else{
		    	  oneM=oneM%9;
		      }

		      long twoM= (sumDigits(two*256))%9;
		      if(twoM%9==0){
		    	  if(twoM==0)
		    		  twoM=0;
		    	  else
		    	  twoM=9;
		      }
		      else{
		    	  twoM=twoM%9;
		      }

		      long threeM = (sumDigits(three*128))%9;
		      if(threeM%9==0){
		    	  if(threeM==0)
		    		  threeM=0;
		    	  else
		    	  threeM=9;
		      }
		      else{
		    	  threeM=threeM%9;
		      }

		      long fourM = (sumDigits(four*64))%9;
		      if(fourM%9==0){
		    	  if(fourM==0)
		    		  fourM=0;
		    	  else
		    	  fourM=9;
		      }
		      else{
		    	  fourM=fourM%9;
		      }

		      long fiveM = (sumDigits(five*32))%9;
		      if(fiveM%9==0){
		    	  if(fiveM==0)
		    		  fiveM=0;
		    	  else
		    	  fiveM=9;
		      }
		      else{
		    	  fiveM=fiveM%9;
		      }

		      long sixM = (sumDigits(six*16))%9;
		      if(sixM%9==0){
		    	  if(sixM==0)
		    		  sixM=0;
		    	  else
		    		  sixM=9;
		      }
		      else{
		    	  sixM=sixM%9;
		      }

		      long sevenM = (sumDigits(seven*8))%9;
		      if(sevenM%9==0){
		    	  if(sevenM==0)
		    		  sevenM=0;
		    	  else
		    	  sevenM=9;
		      }
		      else{
		    	  sevenM=sevenM%9;
		      }

		      long eightM = (sumDigits(eight*4))%9;
		      if(eightM%9==0){
		    	  if(eightM==0)
		    		  eightM=0;
		    	  else
		    	  eightM=9;
		      }
		      else{
		    	  eightM=eightM%9;
		      }

		      long nineM = (sumDigits(nine*2))%9;
		      if(nineM%9==0){
		    	  if(nineM==0)
		    		  nineM=0;
		    	  else
		    	  nineM=9;
		      }
		      else{
		    	  nineM=nineM%9;
		      }

		      long digitNumber=(oneM*100000000)+(twoM*10000000)+(threeM*1000000)+(fourM*100000)
		    		  +(fiveM*10000)+(sixM*1000)+(sevenM*100)+(eightM*10)+nineM;
		      System.out.println("newNumber is: "+newNumber);
		      System.out.println("digitNumber is: "+digitNumber);
long digitNumberSum=sumDigits(digitNumber);
long newNumberSum=sumDigits(newNumber);


		      long digg=newNumberSum-digitNumberSum;
		      
		      if(digg == ten){
System.out.println("TAX ID Valid");
element.put(STRUCTURE.CM_TAXSTATUS,"true");

		      }
		      else{
		    	  System.out.println("TAX ID InValid");
					element.put(STRUCTURE.CM_TAXSTATUS,"false");

		      }
		      }
		     else{
		    	  System.out.println("TAX ID InValid 2");
					element.put(STRUCTURE.CM_TAXSTATUS,"false");

		      }
//		      long ten=storeDigits.get(10);
//		      long eleven=storeDigits.get(11);
//		      long oddSum=one+three+five+seven+nine;
//		      oddSum=oddSum*7;
//		      long evenSum=two+four+six+eight;
//		      long difference=oddSum-evenSum;
//		      long sum=one+two+three+four+five+six+seven+eight+nine+ten;
//		      long calculatedTenth=(difference)%10;
		       
		      
		     }
		     else{
		    	  System.out.println("TAX ID InValid 3");
					element.put(STRUCTURE.CM_TAXSTATUS,"false");

		      }
		       
		     
		     
		     

		}
		catch(Exception e){
			e.printStackTrace();
		}
   	 return element;
	}
	public static long sumDigits(long i) {
	    return i == 0 ? 0 : i % 10 + sumDigits(i / 10);
	}

}
