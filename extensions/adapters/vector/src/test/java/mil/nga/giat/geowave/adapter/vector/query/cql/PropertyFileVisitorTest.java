package mil.nga.giat.geowave.adapter.vector.query.cql;

import org.geotools.data.Query;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Test;
import org.opengis.filter.Filter;

public class PropertyFileVisitorTest
{
	@Test
	public void test() throws CQLException {
		Filter filter = CQL.toFilter("a < 122 and b == '10' and c == 11 and d like '%d'");
		Query query = new Query(
				"type",
				filter);

		PropertyFilterVisitor visitor = new PropertyFilterVisitor();

		PropertyConstraintSet constraints = (PropertyConstraintSet) query.getFilter().accept(
				visitor,
				null);
	}
}
