package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.StringUtils;

public class HBaseMergingFilter extends FilterBase
{
	private final static Logger LOGGER = Logger.getLogger(HBaseMergingFilter.class);
	
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
	
	@Override
	public ReturnCode filterKeyValue(
			Cell v )
			throws IOException {
		// Since this filter is just used by the merging observer
		// to get data about the scan, we pass everything
		return ReturnCode.INCLUDE_AND_NEXT_COL;
	}

	public String getMergeData() {
		return mergeData;
	}

	public void setMergeData(
			String mergeData ) {
		this.mergeData = mergeData;
	}
}
