package com.splwg.cm.domain.admin.meterReaderRoute;

import com.splwg.base.api.service.DataElement;
import com.splwg.shared.common.ApplicationError;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author BHARGAVA
 * 
 *         This business service is intended to shuffle the reader & route
 *         mappings under the given area.
 *
 * @PageMaintenance (secured = false, service = CMSHUFFLEREADERS,
 *  body = @DataElement(contents = {@DataField (name = CM_AREA)  
 *                  , @DataField (name = CM_SHUFFLING_RESULT)}),
 *                  actions = {"change"}, 
 *                  modules = {})
 */

public class ShuffleReaderService extends ShuffleReaderService_Gen {
	
	private final Logger logger = LoggerFactory
			.getLogger(ShuffleReaderService.class);
	
	// input Fields
	private String area;

	// out Fields
	private String shufflingResult;

	@Override
	protected void change(DataElement element) throws ApplicationError {
		area = element.getString(STRUCTURE.CM_AREA);
		ShuffleReader shufflereader = ShuffleReader.Factory.newInstance();
		boolean result = shufflereader.shuffleReaders(area);
		shufflingResult = result ? "Successfully shuffled." : "Shuffling failed.";
		element.put(STRUCTURE.CM_SHUFFLING_RESULT, shufflingResult);
		this.logger.info("shufflingResult = " + shufflingResult);
		logger.info(element.toString());
		setOverrideResultForChange(element);
	}
}
