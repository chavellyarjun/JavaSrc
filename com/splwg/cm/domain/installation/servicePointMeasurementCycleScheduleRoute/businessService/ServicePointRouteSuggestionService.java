package com.splwg.cm.domain.installation.servicePointMeasurementCycleScheduleRoute.businessService;

import java.util.ArrayList;
import java.util.List;

import com.splwg.base.api.service.DataElement;
import com.splwg.base.api.service.ItemList;
import com.splwg.base.api.service.PageHeader;
import com.splwg.shared.common.ApplicationError;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;


/**
 * @author BHARGAVA
 * 
 *         This business service is intended to provide the route suggestions for the service point based on the 
 *         building id or the service point coordinates.
 *
 * 		@PageMaintenance (secured = false, service = CMSPROUTESUGGESTION,
 *  	body = @DataElement(contents = {@ListField (name = CM_ROUTE_SUGGESTIONS)}),
 *      actions = {"read"}, 
 *      modules = {},
 *      header = {@DataField (name = CM_SP)
 *      		, @DataField (name = CM_MEASUREMENT_CYCLE)},
 *      headerFields = { @DataField (name = CM_SP)
 *      		       , @DataField (name = CM_MEASUREMENT_CYCLE)},
 *      lists = { @List (name = CM_ROUTE_SUGGESTIONS, size = 99999, includeLCopybook = false,
 *      		         body = @DataElement (contents = { @DataField (name = CM_ROUTE)
 *      				                                 , @DataField (name = CM_DESCRIPTION)}))}
 *      )
 */

public class ServicePointRouteSuggestionService extends ServicePointRouteSuggestionService_Gen {

	private final Logger logger = LoggerFactory.getLogger(ServicePointRouteSuggestionService.class);
	private String spId = "";
	List<DataElement> responseList = new ArrayList<>();
	ItemList<DataElement> responseItemList = null;
	DataElement responseDataElement = new DataElement();
		
	@Override
    protected DataElement read(PageHeader header) throws ApplicationError {
		logger.info("inside read");
		responseItemList = responseDataElement.newList(STRUCTURE.list_CM_ROUTE_SUGGESTIONS.name);
		if (!isBlankOrNull(header.get(STRUCTURE.HEADER.CM_SP))){
			ServicePointRouteSuggestion servicePointRouteSuggestion = ServicePointRouteSuggestion.Factory.newInstance();
			spId = header.get(STRUCTURE.HEADER.CM_SP);
			List result = servicePointRouteSuggestion.getRouteSuggestions("", spId);
			List<String> routes = (List) result.get(0);
			List<String> descriptions = (List) result.get(1);
			for (int i=0; i<routes.size();i++){
				DataElement element = new DataElement();
				element.put(STRUCTURE.list_CM_ROUTE_SUGGESTIONS.CM_ROUTE, routes.get(i));
				element.put(STRUCTURE.list_CM_ROUTE_SUGGESTIONS.CM_DESCRIPTION, descriptions.get(i));
				responseList.add(element);
			}
			responseItemList.setList(responseList);
			responseDataElement.addList(responseItemList);
			logger.info("responseDataElement=" + responseDataElement.toString());
			return responseDataElement;
		}	
		return null;
	}
	
}
