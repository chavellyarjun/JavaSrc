package com.splwg.cm.domain.measurementCycle.batch;


import java.math.BigInteger;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.QueryIterator;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadIterationStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.StringId;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.cm.domain.customMessages.CmMessageRepository;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointD1;
import com.splwg.d1.domain.installation.servicePoint.entities.ServicePointD1_Id;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;
/**
 * @author 
 *
@BatchJob (modules = {},
 *      softParameters = { @BatchJobSoftParameter (name = measurementCycleRoute, type = string)
 *            ,@BatchJobSoftParameter (name = measurementCycle, type = string)
 *            ,@BatchJobSoftParameter (name =startSequenceNumber, type =integer)     
 *            ,@BatchJobSoftParameter (name =sequenceIncrementBy, type =integer)
 *            })
 */
public class RouteResequencingBatchProcess extends RouteResequencingBatchProcess_Gen {
	public void validateSoftParameters(boolean isNewRun) 
	{ 
		 if(isNull(getParameters().getMeasurementCycleRoute())&& notNull(getParameters().getMeasurementCycle())){
  	 	  logger.info("mcCycleRoute is not passed");
  	 	  addError(CmMessageRepository.softParamsNotPassed());
  	     }
  	     else if(isNull(getParameters().getMeasurementCycle())&& notNull(getParameters().getMeasurementCycleRoute())){
  		  logger.info("mcCycle is not passed");
  		  addError(CmMessageRepository.softParamsNotPassed());
  	     }
  	     else if(isNull(getParameters().getStartSequenceNumber()) && notNull(getParameters().getSequenceIncrementBy())){
   	      logger.info("stratSeqNo is not passed");
          addError(CmMessageRepository.softParamsNotPassed());
         }
         else if(notNull(getParameters().getStartSequenceNumber()) && isNull(getParameters().getSequenceIncrementBy())) {
   	      logger.info("SeqNoIncrementedBy is not passed");
          addError(CmMessageRepository.softParamsNotPassed());
         }
      }

	private static Logger logger = LoggerFactory.getLogger(RouteResequencingBatchProcess.class);
	
	

	public Class<RouteResequencingBatchProcessWorker> getThreadWorkerClass() {
		return RouteResequencingBatchProcessWorker.class;
	}

	public static class RouteResequencingBatchProcessWorker extends RouteResequencingBatchProcessWorker_Gen {
	    
		//working variables
		public  String mcCycle;
		public  String mcCycleRoute;
		public  int startSeqNo=1;
		public  int seqIncreBy=1;
		public BigInteger generate_Seq=new BigInteger("0");
		public String mcCycle_DB;
		public String mcCycleRoute_DB;
		public boolean flag=false;
		
		public ThreadExecutionStrategy createExecutionStrategy() {
			  // TODO Auto-generated method stub
	    			      return new ThreadIterationStrategy(this);
		}
	    
		public void initializeThreadWork(boolean initializationPreviouslySuccessful) throws ThreadAbortedException,
                                                                                                     RunAbortedException {
                    startResultRowQueryIteratorForThread(ServicePointD1_Id.class);
        }
		/**
	     * Create an iterator for a simple query that selects a single business entity.
	 	 * Use lowId and highId to confine the selection to the current thread.  These two
	 	 * arguments are calculated and supplied by the framework.
	 	 */
	     @Override
	     protected QueryIterator<SQLResultRow> getQueryIteratorForThread(StringId lowId, StringId highId) {
	    	 		 //using Optioal params
	    	if(notNull(getParameters().getMeasurementCycle()) && notNull(getParameters().getMeasurementCycleRoute())){
	    	 	mcCycle=getParameters().getMeasurementCycle().toString();
	    	    mcCycleRoute=getParameters().getMeasurementCycleRoute();
	    	 }
	    
	    	 
	    	   if( notBlank(mcCycleRoute)&& notBlank(mcCycle)){
	    		   flag=true;
	    		     logger.info("Retrieving SPID's from for  Route when SoftParams are avail");
	    		 	 //building the query to get SPID
	    		 	 StringBuilder queryString=new StringBuilder();
	    		 	 queryString.append("select D1_SP_ID FROM ");
	    		     queryString.append("D1_SP where MSRMT_CYC_CD='"+mcCycle+"' and MSRMT_CYC_RTE_CD='"+mcCycleRoute+"'");
	    		     PreparedStatement statement = createPreparedStatement(queryString.toString(),"Retrive SpID's ");
	    			 QueryIterator<SQLResultRow>queryResults = statement.iterate();
	    	    	 //statement.bindId("lowId", lowId);
	    	    	 //statement.bindId("highId", highId);
	    			 
	    	    	 return statement.iterate();
	    	     }
	    	  
	    		 
	    		 else{

	    			 logger.info("Retrieving SPID's from for  Route when SoftParams are not avail");
	    			 //HERE getting the spids of routws which Resequencing status is Approved
	    			 StringBuffer routeQuery=new StringBuffer("SELECT DISTINCT(SP.D1_SP_ID),MC.MSRMT_CYC_CD,MC.MSRMT_CYC_RTE_CD ");
	    			 routeQuery.append("FROM D1_MSRMT_CYC_RTE MC,D1_SP SP  WHERE  SP.MSRMT_CYC_RTE_CD =MC.MSRMT_CYC_RTE_CD AND ");
	    			 routeQuery.append(" SP.MSRMT_CYC_CD = MC.MSRMT_CYC_CD AND MC.BO_DATA_AREA LIKE '%"+"CMAR"+"%'");
	    			 PreparedStatement routeStmt=createPreparedStatement(routeQuery.toString(),"Retriving SPID's");
	    			 QueryIterator<SQLResultRow> routesResult = routeStmt.iterate();
	    			 SQLResultRow getroute_Cycle=routesResult.next();
	    			 mcCycle_DB=getroute_Cycle.getString("MSRMT_CYC_CD");
	    			 mcCycleRoute_DB=getroute_Cycle.getString("MSRMT_CYC_RTE_CD");
	    			 return routeStmt.iterate();
	    		 }
	    }//method
	     
	     private void addError(String string) {
			// TODO Auto-generated method stub
			
		}
		/*
	      * Create and return a ThreadWorkUnit from the QueryResultRow instance that was
	      * selected in getQueryForThreadIterator(StringId, StringId) above.  This method must be implemented if
	      * startResultRowQueryIteratorForThread(StringId) was used to initiate the query iterator.
	      * This method's job is to create a ThreadWorkUnit, which will be passed to
	      * executeWorkUnit(ThreadWorkUnit) below.
	      * The returned ThreadWorkUnit must contain a primaryId.
	      */
	     @Override
	      public ThreadWorkUnit getNextWorkUnit(QueryResultRow row) {
	    	  ThreadWorkUnit unit = new ThreadWorkUnit(row.getId("D1_SP_ID", ServicePointD1.class));
	    	  return unit;
	     }


		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			//building the query to get MC which is having UOM as KWH
			 //<here spid in the qury we need to take SP is which is coming from Query Iterator r not>

	    	ServicePointD1_Id sP_ID = (ServicePointD1_Id) unit.getPrimaryId();
	    	StringBuffer query=new StringBuffer();
			query.append("select  mc.MOST_RECENT_MSRMT_DTTM,sp.D1_SP_ID from D1_MEASR_COMP_IDENTIFIER uom,D1_MEASR_COMP mc, D1_INSTALL_EVT ie,");
			query.append("D1_DVC_CFG dc,D1_sp sp where ");
			query.append("uom.MEASR_COMP_ID=mc.MEASR_COMP_ID and ");
			query.append("sp.D1_SP_ID='"+sP_ID.getIdValue()+"' and " ); 
			query.append("ie.DEVICE_CONFIG_ID=dc.DEVICE_CONFIG_ID and "); 
			query.append("dc.DEVICE_CONFIG_ID=mc.DEVICE_CONFIG_ID ORDER BY mc.MOST_RECENT_MSRMT_DTTM ASC ");
			PreparedStatement spIdnrecDttmStatement = createPreparedStatement(query.toString(),"Retrieve recDttm");
			QueryIterator<SQLResultRow> spIdnmostRecDtt = spIdnrecDttmStatement.iterate();
			//closing statement
			
		


			//Adding SpiD as key and MostRecDttm as value into MAP
			LinkedHashMap spidsNrecDttmMap=new LinkedHashMap();
			
			while(spIdnmostRecDtt.hasNext()){
				SQLResultRow getSpidNdttm=spIdnmostRecDtt.next();
				 
				DateTime mostRecDttm= getSpidNdttm.getDateTime("MOST_RECENT_MSRMT_DTTM");
				String spIdMap=getSpidNdttm.getString("D1_SP_ID");
				spidsNrecDttmMap.put(spIdMap, mostRecDttm);
			}
			
			spIdnrecDttmStatement.close();
			//performing SP's reSequencing for which r not read
			Set lhs=spidsNrecDttmMap.keySet();
			Iterator iterate=lhs.iterator();
			while(iterate.hasNext()){
				  Object spId =iterate.next();
				  if(isNull(spidsNrecDttmMap.get(spId))){
					 //Object spId=servicePoint;
					 Object mostRecDttm=spidsNrecDttmMap.get(spId);
					 spidsNrecDttmMap.remove(spId, mostRecDttm);
					 spidsNrecDttmMap.put(spId, mostRecDttm);
				  }

					 //while(spidsNrecDttmMap.entrySet().iterator().hasNext()){
					 BigInteger softParam_StartSeqNo=getParameters().getStartSequenceNumber();
				     BigInteger softParam_seqIncreBy=getParameters().getSequenceIncrementBy();
                  
		 			 
				  if(notNull(getParameters().getStartSequenceNumber())&& notNull(getParameters().getSequenceIncrementBy())){	
					  logger.info("updating Sequnce for when seqNo Params  avail");
					  //getting the BO data
					 // adding datt to BO
						 BusinessObjectInstance BOInstance = BusinessObjectInstance.create("X1D-ServicePoint");
						 BOInstance.set("spId", spId.toString());

						  if(flag==true){
					 			 BOInstance.set("measurementCycle",mcCycle);
					 			 BOInstance.set("measurementCycleRoute",mcCycleRoute);
					 		}
							else{
								 BOInstance.set("measurementCycle",mcCycle_DB);
						 		 BOInstance.set("measurementCycleRoute",mcCycleRoute_DB);
							}
				     BOInstance=BusinessObjectDispatcher.read(BOInstance,true);
		 			 generate_Seq=generate_Seq.add(softParam_StartSeqNo).add(softParam_seqIncreBy);
		 			 BOInstance.set("measurementCycleRouteSequence", new BigDecimal(generate_Seq));
		 			 BOInstance = BusinessObjectDispatcher.update(BOInstance);
		 			 logger.info(BOInstance.getDocument().asXML().toString());
		 			}
				  else{

						 BusinessObjectInstance BOInstance = BusinessObjectInstance.create("X1D-ServicePoint");
						 BOInstance.set("spId", spId.toString());
					  
					  if(flag==true){
				 			 BOInstance.set("measurementCycle",mcCycle);
				 			 BOInstance.set("measurementCycleRoute",mcCycleRoute);
				 		}
						else{
							logger.info("when cycle and route are not there");
							 BOInstance.set("measurementCycle",mcCycle_DB);
					 		 BOInstance.set("measurementCycleRoute",mcCycleRoute_DB);
						}

						 BOInstance=BusinessObjectDispatcher.read(BOInstance,true);
					logger.info("updating Sequnce for when seqNo are not avail");					 
					BOInstance.set("measurementCycleRouteSequence", new BigDecimal(startSeqNo));
		 			BOInstance =BusinessObjectDispatcher.update(BOInstance);
		 			logger.info(BOInstance.getDocument().asXML().toString());
 			   		startSeqNo=startSeqNo+seqIncreBy;
				} 	
			 }//while
			
				return true;
		}//method
		}//innerClass
}//class
