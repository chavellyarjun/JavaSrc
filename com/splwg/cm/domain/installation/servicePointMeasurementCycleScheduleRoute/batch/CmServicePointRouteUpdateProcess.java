package com.splwg.cm.domain.installation.servicePointMeasurementCycleScheduleRoute.batch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dom4j.Element;

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
import com.splwg.base.api.businessObject.COTSInstanceNode;
import com.splwg.base.api.datatypes.StringId;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.fact.fact.Fact;
import com.splwg.base.domain.fact.fact.Fact_Id;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author BHARGAVA
 * @BatchJob (modules = {}, 
 * softParameters = {@BatchJobSoftParameter (name = activityId, required = true, type = string)})
 */

public class CmServicePointRouteUpdateProcess extends CmServicePointRouteUpdateProcess_Gen {

	public static Logger logger = LoggerFactory.getLogger(CmServicePointRouteUpdateProcess.class);

	public Class<CmServicePointRouteUpdateProcessWorker> getThreadWorkerClass() {
		return CmServicePointRouteUpdateProcessWorker.class;
	}

	public static class CmServicePointRouteUpdateProcessWorker extends CmServicePointRouteUpdateProcessWorker_Gen {
		
		private String CHAR_TYPE_CD = "CM-RTACT",
				CM_ROUTE_IMPORT_FACT = "CM-RouteImportFact",
				ROUTE_IMPORT_DATA = "routeImportData",
				NEW_ROUTE = "newRoute", 
				D1_SP_ID = "D1_SP_ID",
				X1D_SERVICE_POINT = "X1D-ServicePoint", 
				ONSP = "ONSP", 
				SERVICE_POINT = "servicePoint",
				BO_STATUS = "boStatus",
				COMPLETED = "COMPLETED",
				VALIDATED = "VALIDATED";
		
		private static Map<String, Integer> sequenceMap = new HashMap<>();
		
		public ThreadExecutionStrategy createExecutionStrategy() {
			return new ThreadIterationStrategy(this);
		}

		/**
		 * Initiate an *entity* query iterator for the business entities that
		 * are to be selected in getQueryIteratorForThread(StringId, StringId)
		 * below.
		 */

		public void initializeThreadWork(boolean initializationPreviouslySuccessful) throws ThreadAbortedException, RunAbortedException {
			setRoutesAndSequence();
			startResultRowQueryIteratorForThread(Fact_Id.class);
		}

		/**
		 * Create an iterator for a simple query that selects a single business
		 * entity. Use lowId and highId to confine the selection to the current
		 * thread. These two arguments are calculated and supplied by the
		 * framework.
		 */

		@Override
		protected QueryIterator getQueryIteratorForThread(StringId lowId, StringId highId) {
			
			String activityId = getParameters().getActivityId();
			StringBuilder queryString = new StringBuilder();
			queryString.append("SELECT F1_FACT_CHAR.FACT_ID FROM F1_FACT join F1_FACT_CHAR on F1_FACT.FACT_ID=F1_FACT_CHAR.FACT_ID WHERE "
					+ "F1_FACT_CHAR.CHAR_TYPE_CD = '" + CHAR_TYPE_CD + "' and F1_FACT_CHAR.ADHOC_CHAR_VAL = '" + activityId +  
					"' and UPPER(F1_FACT.BO_STATUS_CD)='" + VALIDATED + "' ");
			queryString.append("AND F1_FACT_CHAR.FACT_ID BETWEEN :lowId AND :highId");
			PreparedStatement statement = createPreparedStatement(queryString.toString(), "retrieve fact ids");
			statement.bindId("lowId", lowId);
			statement.bindId("highId", highId);
			logger.info("activityId=" + activityId);
			List<SQLResultRow> results = statement.list();
			logger.info(null != results ? "facts size=" + results.size() : "no facts related to activityId=" + activityId);	
			return statement.iterate();
		}

		/*
		 * Create and return a ThreadWorkUnit from the QueryResultRow instance
		 * that was selected in getQueryForThreadIterator(StringId, StringId)
		 * above. This method must be implemented if
		 * startResultRowQueryIteratorForThread(StringId) was used to initiate
		 * the query iterator. This method's job is to create a ThreadWorkUnit,
		 * which will be passed to executeWorkUnit(ThreadWorkUnit) below. The
		 * returned ThreadWorkUnit must contain a primaryId.
		 */

		@Override
		public ThreadWorkUnit getNextWorkUnit(QueryResultRow row) {
			ThreadWorkUnit unit = new ThreadWorkUnit(row.getId("FACT_ID", Fact.class));
			return unit;
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			logger.info("Inside Execute Work Unit " + unit.getPrimaryId());
			Fact_Id factId = (Fact_Id) unit.getPrimaryId();
			updateServicePointRoute(factId);
			return true;
		}
		
		private void updateServicePointRoute(Fact_Id factId){
			
			logger.info("Inside updateServicePointRoutes" + factId);
			String factIdValue = factId.getIdValue(), spId = null;
			Integer sequence = 1;
			// getting fact table row
			BusinessObjectInstance routeImportFactBOInstance = BusinessObjectInstance.create(CM_ROUTE_IMPORT_FACT);
            routeImportFactBOInstance.set("bo", CM_ROUTE_IMPORT_FACT);
            routeImportFactBOInstance.set("factId", factIdValue);
            routeImportFactBOInstance = BusinessObjectDispatcher.read(routeImportFactBOInstance, true);
            COTSInstanceNode routeImportData = routeImportFactBOInstance.getGroupFromPath(ROUTE_IMPORT_DATA);
                        	
        	// getting new route code
        	Element newRouteElement = routeImportData.getElement().element(NEW_ROUTE);
        	Element servicePointElement = routeImportData.getElement().element(SERVICE_POINT);
        	String newRouteString = newRouteElement.getStringValue(); 
        	String servicePointString = servicePointElement.getStringValue(); 
			logger.info("got newRouteString=" + newRouteString);

        	// getting SP id
			Element spIdElement = routeImportData.getElement().element("mdmServicePoint");
			if (null == spIdElement || isBlankOrNull(spIdElement.toString())){
				StringBuilder getSpIdQueryString = new StringBuilder();
    			getSpIdQueryString.append("SELECT D1_SP_ID FROM D1_SP_IDENTIFIER WHERE ID_VALUE = '" + servicePointString + "'");
    			PreparedStatement getSpIdStatement = createPreparedStatement(getSpIdQueryString.toString(), "retrieve D1_SP_ID");
    			List<SQLResultRow> result = getSpIdStatement.list();
    			for(SQLResultRow row: result)
    				spId = row.get(D1_SP_ID).toString();
			} else {
				spId = spIdElement.getStringValue();
			}
			logger.info("got spId=" + spId);
        	    			
			synchronized (sequenceMap) {
				// getting the sequence  			
				// change sequence value
				if(notNull(sequenceMap.get(ONSP + "|" + newRouteString)))
					sequence = sequenceMap.get(ONSP + "|" + newRouteString) + 1;
				sequenceMap.put(ONSP + "|" + newRouteString, sequence);
				logger.info("new sequence=" + sequence);
			}
						
			// getting SP BO and updating with new measurement cycle and route codes
			BusinessObjectInstance servicePointBOInstance = BusinessObjectInstance.create(X1D_SERVICE_POINT);
			servicePointBOInstance.set("bo", X1D_SERVICE_POINT);
			servicePointBOInstance.set("spId", spId);
			servicePointBOInstance = BusinessObjectDispatcher.read(servicePointBOInstance, true);
			if (null != servicePointBOInstance){
				servicePointBOInstance.set("measurementCycle", ONSP);
    			servicePointBOInstance.set("measurementCycleRoute", newRouteString);
    			servicePointBOInstance.set("measurementCycleRouteSequence", new BigDecimal(sequence));
    			BusinessObjectDispatcher.update(servicePointBOInstance);
    			logger.info("updated service point " + spId + " with route " + newRouteString + " and sequence " + sequence);
    			
    			// update status to completed in fact BO
    			routeImportFactBOInstance.set(BO_STATUS, COMPLETED);
    			BusinessObjectDispatcher.update(routeImportFactBOInstance);
    			logger.info("updated fact " + factIdValue + " status from " + VALIDATED + " to " + COMPLETED);
			}
            
		}
		
		private void setRoutesAndSequence(){
			StringBuilder sequenceQueryString = new StringBuilder();
			sequenceQueryString.append("select MAX(MSRMT_CYC_RTE_SEQ), MSRMT_CYC_CD, MSRMT_CYC_RTE_CD from d1_sp where MSRMT_CYC_CD='" + ONSP + "' GROUP by MSRMT_CYC_CD, MSRMT_CYC_RTE_CD order by MSRMT_CYC_CD");
			PreparedStatement sequenceQueryStatement = createPreparedStatement(sequenceQueryString.toString(), "retrieve fact ids");
			List<SQLResultRow> sequenceResults = sequenceQueryStatement.list();
			for(SQLResultRow row: sequenceResults){
				if (!isBlankOrNull(row.get("MSRMT_CYC_CD").toString()) && !isBlankOrNull(row.get("MSRMT_CYC_RTE_CD").toString())){
					String route = row.get("MSRMT_CYC_CD").toString() + "|" + row.get("MSRMT_CYC_RTE_CD").toString();
					Integer seq = isBlankOrNull(row.get("MSRMT_CYC_CD").toString()) ? 0 : Integer.valueOf(row.get("MAX(MSRMT_CYC_RTE_SEQ)").toString());
					sequenceMap.put(route, seq);
				}
			}
			logger.info("sequenceMap=" + sequenceMap);
		}
	}
}
