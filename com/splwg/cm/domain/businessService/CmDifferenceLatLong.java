package com.splwg.cm.domain.businessService;

//import java.math.BigInteger;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.service.DataElement;
import com.splwg.base.api.service.PageHeader;
import com.splwg.shared.common.ApplicationError;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

 

/**
* @author
*
** @PageMaintenance (secured = false, service = CMLATLOG,
*      body = @DataElement (contents = { @DataField (name = CM_DIST)}),              
*      actions = { "read"},
*      header = { @DataField (name = CM_LAT1)
*                    , @DataField (name = CM_LAT2)
*                    , @DataField (name = CM_LONG1)
*                    , @DataField (name = CM_LONG2)},
*      headerFields = { @DataField (name = CM_LAT1)
*                    , @DataField (name = CM_LAT2)
*                    , @DataField (name = CM_LONG1)
*                    , @DataField (name = CM_LONG2)},
*    
*      modules = {})    
*/
public class CmDifferenceLatLong extends CmDifferenceLatLong_Gen{

      public static Logger logger = LoggerFactory.getLogger(CmDifferenceLatLong.class);
     
      @Override
      protected DataElement read(PageHeader header) throws ApplicationError {

            Double latitute1 = header.get(STRUCTURE.HEADER.CM_LAT1).doubleValue();
            Double latitute2 = header.get(STRUCTURE.HEADER.CM_LAT2).doubleValue();
            Double longitute1 = header.get(STRUCTURE.HEADER.CM_LONG1).doubleValue();
            Double longitute2 = header.get(STRUCTURE.HEADER.CM_LONG2).doubleValue();

            final int R = 6371; // Radious of the earth
            Double latDistance = toRad(latitute2-latitute1);
            Double lonDistance = toRad(longitute2-longitute1);
            Double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + 
                       Math.cos(toRad(latitute1)) * Math.cos(toRad(latitute2)) * 
                       Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
            Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
            Double distance = R * c;     
            
            DataElement  dataElement = new DataElement();
            dataElement.put(STRUCTURE.CM_DIST, BigDecimal.valueOf(distance));
                   
            return dataElement;
      }
      private static Double toRad(Double value) {
          return value * Math.PI / 180;
      }
   
}

