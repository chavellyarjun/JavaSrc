package com.splwg.cm.domain.admin.meterReaderRoute;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

public class ShuffleReader_Impl extends GenericBusinessComponent implements
		ShuffleReader {

	private final Logger logger = LoggerFactory
			.getLogger(ShuffleReader_Impl.class);

	public boolean shuffleReaders(String area) {
		Map<String, List<String>> initialReadersAndRoutesUnderOneArea = getReadersAndRoutes(area);
		this.logger.info("initialReadersAndRoutesUnderOneArea --> "
				+ initialReadersAndRoutesUnderOneArea);
		Map<String, List<String>> shuffledReadersAndRoutesUnderOneArea = shuffle(initialReadersAndRoutesUnderOneArea);
		this.logger.info("shuffledReadersAndRoutesUnderOneArea --> "
				+ shuffledReadersAndRoutesUnderOneArea);
		updateReadersAndRoutes(shuffledReadersAndRoutesUnderOneArea);
		Map<String, List<String>> afterUpdatingDb = getReadersAndRoutes(area);
		
		Set<String> keyset = shuffledReadersAndRoutesUnderOneArea.keySet();
    	int count = 0;
    	for (String key: keyset){
    		List<String> listFromDb = afterUpdatingDb.get(key);
    		List<String> listAfterShuffle = shuffledReadersAndRoutesUnderOneArea.get(key);
    		if (null != listFromDb && null != listAfterShuffle){
    			Collections.sort(listFromDb);
        		Collections.sort(listAfterShuffle);
        		if (listFromDb.equals(listAfterShuffle))
        			count++;
    		}
    	}
    	if (count == keyset.size()){
    		this.logger.info("Successfully shuffled and updated the table");
    		return true;
    	}
    	return false;
	}

	private Map<String, List<String>> getReadersAndRoutes(String area) {
		String readersAndRoutesQueryString = "select c.CM_READER_ID, c.msrmt_cyc_cd, c.msrmt_cyc_rte_cd from  cm_mtr_rdr_rte c "
				+ "inner join d1_msrmt_cyc_rte_char d on c.msrmt_cyc_cd=d.msrmt_cyc_cd and c.msrmt_cyc_rte_cd=d.msrmt_cyc_rte_cd "
				+ "where CHAR_TYPE_CD = :charType and upper(d.adhoc_char_val) = upper(:area)";
		PreparedStatement readersAndRoutesQuery = createPreparedStatement(
				readersAndRoutesQueryString,
				"to get the list of routes under an area");
		readersAndRoutesQuery.bindString("area", area, "setting area");
		readersAndRoutesQuery.bindString("charType", "CM-RAREA",
				"setting charType");
		List<SQLResultRow> results = readersAndRoutesQuery.list();
		Map<String, List<String>> map = new HashMap<>();
		for (SQLResultRow row : results) {
			String routeCode = row.get("MSRMT_CYC_CD").toString().trim() + "|"
					+ row.get("MSRMT_CYC_RTE_CD").toString().trim();
			String readerId = row.get("CM_READER_ID").toString().trim();
			if (null == map.get(routeCode)) {
				List<String> readerIds = new ArrayList<>();
				readerIds.add(readerId);
				map.put(routeCode, readerIds);
			} else {
				map.get(routeCode).add(readerId);
			}
		}
		return map;
	}

	private Map<String, List<String>> shuffle(
			Map<String, List<String>> readersAndRoutesUnderOneArea) {
		Set<String> keys = readersAndRoutesUnderOneArea.keySet();
		Map<String, List<String>> updatedReadersAndRoutesUnderOneArea = new LinkedHashMap<>();
		for (String key : keys) {
			updatedReadersAndRoutesUnderOneArea.put(key, new ArrayList<>());
		}
		List<String> keyList = new ArrayList<>(
				updatedReadersAndRoutesUnderOneArea.keySet());
		int j = 0;
		for (String key : keys) {
			List<String> ids = readersAndRoutesUnderOneArea.get(key);
			for (int i = 0; i < ids.size(); i++) {
				int size = updatedReadersAndRoutesUnderOneArea.size();
				String updatedKey = keyList.get(j);
				if (key.equals(updatedKey)) {
					j += 1;
					if (j == size)
						j = 0;
					updatedKey = keyList.get(j);
					updatedReadersAndRoutesUnderOneArea.get(updatedKey).add(
							ids.get(i));
					j += 1;
				} else {
					updatedReadersAndRoutesUnderOneArea.get(updatedKey).add(
							ids.get(i));
					j += 1;
				}
				if (j == size)
					j = 0;
			}
		}
		return updatedReadersAndRoutesUnderOneArea;
	}

	private void updateReadersAndRoutes(
			Map<String, List<String>> updatedReadersAndRoutesUnderOneArea) {
		Set<String> keySet = updatedReadersAndRoutesUnderOneArea.keySet();
		String deleteQueryString = "delete from cm_mtr_rdr_rte where msrmt_cyc_cd=:msrmt_cyc_cd and msrmt_cyc_rte_cd=:msrmt_cyc_rte_cd";
		String insertQueryString = "insert into cm_mtr_rdr_rte (cm_reader_id, msrmt_cyc_cd, msrmt_cyc_rte_cd, version) "
				+ "VALUES(:cm_reader_id, :msrmt_cyc_cd, :msrmt_cyc_rte_cd, :version)";

		for (String key : keySet) {
			PreparedStatement deleteQuery = createPreparedStatement(
					deleteQueryString, "delete existing route-reader mappings");
			String msrmt_cyc_cd = key.split("\\|")[0];
			String msrmt_cyc_rte_cd = key.split("\\|")[1];
			deleteQuery.bindString("msrmt_cyc_cd", msrmt_cyc_cd,
					"setting msrmt_cyc_cd");
			deleteQuery.bindString("msrmt_cyc_rte_cd", msrmt_cyc_rte_cd,
					"setting msrmt_cyc_rte_cd");
			int result = deleteQuery.executeUpdate();
			this.logger.info("deleted " + "for " + msrmt_cyc_cd + " " + msrmt_cyc_rte_cd + ", result=" + result);
		}

		for (String key : keySet) {
			List<String> ids = updatedReadersAndRoutesUnderOneArea.get(key);
			String msrmt_cyc_cd = key.split("\\|")[0], msrmt_cyc_rte_cd = key
					.split("\\|")[1], version = "1";
			for (String id : ids) {
				PreparedStatement insertQuery = createPreparedStatement(
						insertQueryString, "insert shuffled route-reader mappings");
				insertQuery.bindString("msrmt_cyc_cd", msrmt_cyc_cd,
						"setting msrmt_cyc_cd");
				insertQuery.bindString("msrmt_cyc_rte_cd", msrmt_cyc_rte_cd,
						"setting msrmt_cyc_rte_cd");
				insertQuery.bindString("version", version, "setting version");
				insertQuery.bindString("cm_reader_id", id,
						"setting cm_reader_id");
				int result = insertQuery.executeUpdate();
				this.logger.info("inserting msrmt_cyc_cd=" + msrmt_cyc_cd
						+ " msrmt_cyc_rte_cd=" + msrmt_cyc_rte_cd + " id=" + id
						+ "version=" + version + "result=" + result);
			}
		}
	}
}
