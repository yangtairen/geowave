package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.StringUtils;

public class HBaseMergingFilter extends
		FilterBase
{
	private final static Logger LOGGER = Logger.getLogger(HBaseMergingFilter.class);

	// TEST ONLY!
	static {
		LOGGER.setLevel(
				Level.DEBUG);
	}

	private String mergeData;

	public HBaseMergingFilter() {

	}

	public static HBaseMergingFilter parseFrom(
			final byte[] pbBytes )
			throws DeserializationException {
		HBaseMergingFilter mergingFilter = new HBaseMergingFilter();

		String mergeData = StringUtils.stringFromBinary(pbBytes);
		mergingFilter.setMergeData(mergeData);

		return mergingFilter;
	}
	
	@Override
	public byte[] toByteArray()
			throws IOException {
		return StringUtils.stringToBinary(mergeData);
	}
	
	/**
	 * Enable filterRowCells
	 */
	@Override
	public boolean hasFilterRow() {
		return true;
	}

	/**
	 * Handle the entire row at one time
	 */
	@Override
	public void filterRowCells(
			List<Cell> rowCells )
			throws IOException {
		if (!rowCells.isEmpty()) {			
			if (rowCells.size() > 1) {
				LOGGER.debug(">> filterRowCells merged " + rowCells.size() + " cells");
			}
		}
	}

	/**
	 * Don't do anything special here, since we're only interested in whole rows
	 */
	@Override
	public ReturnCode filterKeyValue(
			Cell v )
			throws IOException {
		return ReturnCode.INCLUDE;
	}

	public String getMergeData() {
		return mergeData;
	}

	public void setMergeData(
			String mergeData ) {
		this.mergeData = mergeData;
	}
}
