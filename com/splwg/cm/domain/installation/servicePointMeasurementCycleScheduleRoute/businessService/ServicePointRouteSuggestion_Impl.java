package com.splwg.cm.domain.installation.servicePointMeasurementCycleScheduleRoute.businessService;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.splwg.base.api.GenericBusinessComponent;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author BHARGAVA
 *
 * @BusinessComponent (customizationReplaceable = false, customizationCallable =
 *                    true)
 */

public class ServicePointRouteSuggestion_Impl extends GenericBusinessComponent implements ServicePointRouteSuggestion {

	private final Logger logger = LoggerFactory
			.getLogger(ServicePointRouteSuggestion_Impl.class);
	private final String MEASUREMENT_CYCLE = "ONSP";
	
	public List getRouteSuggestions(String mesurementCycle, String spId){
		logger.info("inside getRouteSuggestions");
		List<String> routes = new ArrayList<>();
		List<String> routesBasedOnBuildingNumber = new ArrayList<>();
		List<String> routesBasedOnCoordinates = new ArrayList<>();
		List<String> routesToBeSkipped = new ArrayList<>();
		List<String> routesRemaining = new ArrayList<>();
		Set<SpWithRouteLatLon> spsWithDistance = new TreeSet<>();
		Set<String> routesWithCoordinatesSet = new LinkedHashSet<>();
		String buildingNumber = null;
		
		// get the routes based on building number
		StringBuilder buildngNumberQueryStringBuilder = new StringBuilder();
		buildngNumberQueryStringBuilder.append("SELECT NUM1 FROM D1_SP WHERE D1_SP_ID='")
		.append(spId)
		.append("'");
		PreparedStatement buildngNumberQuery = createPreparedStatement(buildngNumberQueryStringBuilder.toString(), "");
		List<SQLResultRow> buildngNumbersFromDb = buildngNumberQuery.list();
		for (SQLResultRow row: buildngNumbersFromDb){
			String b = null != row.get("NUM1") ? row.get("NUM1").toString().trim() : null;
			if (!isBlankOrNull(b))
				buildingNumber = b;
		}
		logger.info("current SP buildingNumber=" + buildingNumber);
		if (null != buildingNumber){
			StringBuilder routesWithSameBuildngNumberQueryStringBuilder = new StringBuilder();
			routesWithSameBuildngNumberQueryStringBuilder.append("SELECT distinct(MSRMT_CYC_RTE_CD) FROM D1_SP WHERE ")
			.append("NUM1='")
			.append(buildingNumber)
			.append("' AND MSRMT_CYC_CD='")
			.append(MEASUREMENT_CYCLE)
			.append("' ORDER BY MSRMT_CYC_RTE_CD ASC");
			
			PreparedStatement routesWithSameBuildngNumberQuery = createPreparedStatement(routesWithSameBuildngNumberQueryStringBuilder.toString(), "");
			List<SQLResultRow> routesListWithSameBuildngNumberFromDb = routesWithSameBuildngNumberQuery.list();
			logger.info("routesListWithSameBuildngNumberFromDb.size()=" + (null != routesListWithSameBuildngNumberFromDb ? routesListWithSameBuildngNumberFromDb.size() : "null"));
			for (SQLResultRow row: routesListWithSameBuildngNumberFromDb){
				String routeCode = null != row.get("MSRMT_CYC_RTE_CD") ? row.get("MSRMT_CYC_RTE_CD").toString().trim() : null;
				if (!isBlankOrNull(routeCode))
					routesBasedOnBuildingNumber.add(routeCode);
			}
			routesToBeSkipped.addAll(routesBasedOnBuildingNumber);	
		}	
		logger.info("routesBasedOnBuildingNumber=" + routesBasedOnBuildingNumber);
		logger.info("after getting routes based on building number: routesToBeSkipped.size()=" + routesToBeSkipped.size() + ", routesToBeSkipped=" + routesToBeSkipped);
				
		// get the routes based on coordinates
		String s1 = "SELECT D1_GEO_LAT, D1_GEO_LONG FROM D1_SP WHERE D1_SP_ID = '" + spId + "'";
		PreparedStatement s1Query = createPreparedStatement(s1, "");
		List<SQLResultRow> s1Results = s1Query.list();
		Double lat = null, lon = null;
		for(SQLResultRow row: s1Results){
			if (null != row.get("D1_GEO_LAT") && null != row.get("D1_GEO_LONG") && !isBlankOrNull(row.get("D1_GEO_LAT").toString().trim())
					 && !isBlankOrNull(row.get("D1_GEO_LONG").toString().trim())){
				lat = new Double(row.get("D1_GEO_LAT").toString().trim());
				lon = new Double(row.get("D1_GEO_LONG").toString().trim());
			}
		}
		logger.info("current sp: lat=" + lat + ", lon=" + lon);
		if (null != lat && null != lon && lat != 0.0 && lon != 0.0){
			StringBuilder routesWithCoordinatesQueryStringBuilder = new StringBuilder();
			routesWithCoordinatesQueryStringBuilder.append("SELECT D1_SP_ID, MSRMT_CYC_RTE_CD, D1_GEO_LAT, D1_GEO_LONG FROM D1_SP ")
			.append("WHERE D1_GEO_LAT IS NOT NULL and D1_GEO_LONG IS NOT NULL and D1_GEO_LAT != '0' and D1_GEO_LONG != '0' ")
			.append("and msrmt_cyc_cd='ONSP' ");
			if (routesToBeSkipped.size() > 0){
				routesWithCoordinatesQueryStringBuilder.append("AND MSRMT_CYC_RTE_CD not in (")
				.append(getValuesAsStringForNotInFilter(routesToBeSkipped))
				.append(") ");
			}
			routesWithCoordinatesQueryStringBuilder.append("ORDER BY msrmt_cyc_rte_cd ASC");
			PreparedStatement routesWithCoordinatesQuery = createPreparedStatement(routesWithCoordinatesQueryStringBuilder.toString(), "");
			List<SQLResultRow> routesWithCoordinatesListFromDb = routesWithCoordinatesQuery.list();
			logger.info("routesWithCoordinatesListFromDb.size()="+routesWithCoordinatesListFromDb.size());
			for(SQLResultRow row: routesWithCoordinatesListFromDb){
				String spId1 = row.get("D1_SP_ID").toString();
				String route1 = row.get("MSRMT_CYC_RTE_CD").toString();
				Double lat1 = new Double(row.get("D1_GEO_LAT").toString());
				Double lon1 = new Double(row.get("D1_GEO_LONG").toString());
				SpWithRouteLatLon sp = new SpWithRouteLatLon(spId1, route1, lat1, lon1);
				sp.setDistance(distance(lat, lon, lat1, lon1, ""));
				spsWithDistance.add(sp);
			}
			logger.info("spsWithDistance="+spsWithDistance);
			for(SpWithRouteLatLon sp: spsWithDistance){
				routesWithCoordinatesSet.add(sp.getRoute());
			}
			routesBasedOnCoordinates.addAll(routesWithCoordinatesSet);
		}
		logger.info("routesBasedOnCoordinates.size()="+routesBasedOnCoordinates.size() + ", routesBasedOnCoordinates=" + routesBasedOnCoordinates);
		
		// get remaining routes
		StringBuilder routesRemainingQueryStringBuilder = new StringBuilder();
		routesRemainingQueryStringBuilder.append("select distinct(MSRMT_CYC_RTE_CD) from d1_msrmt_cyc_rte where msrmt_cyc_cd='")
		.append(MEASUREMENT_CYCLE)
		.append("'");
		
		// if we have routes based on coordinates, skip those routes in query 
		routesToBeSkipped.addAll(routesBasedOnCoordinates);
		logger.info("after getting routes based on coordinates: routesToBeSkipped.size()=" + routesToBeSkipped.size() + ", routesToBeSkipped=" + routesToBeSkipped);
		if(routesToBeSkipped.size() > 0){
			routesRemainingQueryStringBuilder.append(" AND MSRMT_CYC_RTE_CD not in (")
			.append(getValuesAsStringForNotInFilter(routesToBeSkipped))
			.append(")");
		}
		routesRemainingQueryStringBuilder.append(" ORDER BY msrmt_cyc_rte_cd ASC");
		PreparedStatement routesRemainingQuery = createPreparedStatement(routesRemainingQueryStringBuilder.toString(), "");
		List<SQLResultRow> remainingRouteListFromDb = routesRemainingQuery.list();
		logger.info("remainingRouteListFromDb.size()=" + (null != remainingRouteListFromDb ? remainingRouteListFromDb.size() : "null"));
		for (SQLResultRow row: remainingRouteListFromDb){
			String routeCode = null != row.get("MSRMT_CYC_RTE_CD") ? row.get("MSRMT_CYC_RTE_CD").toString().trim() : null;
			if (!isBlankOrNull(routeCode))
				routesRemaining.add(routeCode);
		}
		logger.info("routesRemaining.size()=" + routesRemaining.size() + ", routesRemaining=" + routesRemaining);
		routes.addAll(routesBasedOnBuildingNumber);
		routes.addAll(routesBasedOnCoordinates);
		routes.addAll(routesRemaining);
		logger.info("final routes.size()=" + routes.size() + ", final routes=" + routes);
		
		List<List<String>> result = new ArrayList<List<String>>();
		result.add(routes);
		result.add(getDescriptionsForRoutes(routes));
		return result;
	}

	private double distance(double lat1, double lon1, double lat2, double lon2, String unit) {
		if ((lat1 == lat2) && (lon1 == lon2)) {
			return 0;
		}
		else {
			double theta = lon1 - lon2;
			double dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2)) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta));
			dist = Math.acos(dist);
			dist = Math.toDegrees(dist);
			dist = dist * 60 * 1.1515;
			if (unit == "K") {
				dist = dist * 1.609344;
			} else if (unit == "N") {
				dist = dist * 0.8684;
			}
			// default distance unit is miles
			return (dist);
		}
	}
	
	private String getValuesAsStringForNotInFilter(List<String> routes){
		StringBuilder stringForNotInFilter = new StringBuilder();
		for(int i=0; i<routes.size(); i++){
			stringForNotInFilter.append("'")
			.append(routes.get(i))
			.append("'")
			.append(i != routes.size()-1 ? "," : "");
		}
		return stringForNotInFilter.toString();
	}
	
	private List<String> getDescriptionsForRoutes(List<String> routes){
		List<String> descriptions = new ArrayList<>();
		for (String route: routes){
			StringBuilder descriptionQueryStringBuilder = new StringBuilder();
			descriptionQueryStringBuilder.append("select DESCR100 from d1_msrmt_cyc_rte_l where MSRMT_CYC_CD='ONSP' ")
					.append("and LANGUAGE_CD='TUR' and MSRMT_CYC_RTE_CD = '" + route + "'");
			PreparedStatement descriptionQuery = createPreparedStatement(descriptionQueryStringBuilder.toString(), "");
			List<SQLResultRow> descriptionListFromDb = descriptionQuery.list();
			for (SQLResultRow row: descriptionListFromDb){
				descriptions.add(row.get("DESCR100").toString());
			}
		}
		logger.info("descriptions.size()=" + descriptions.size() + ", descriptions=" + descriptions);
		return descriptions;
	}
}
