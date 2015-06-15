package mil.nga.giat.geowave.analytic.param;


public interface ParameterEnum<T>
{
	public ParameterHelper<T> getHelper();

	public Enum<?> self();
}
