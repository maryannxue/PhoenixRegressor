package com.salesforce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.phoenix.calcite.PhoenixSchema;

import com.google.common.collect.Maps;

/**
 * Phoenix Connection
 * @author mchohan
 *
 */
class PhoenixConnection {

	private static final String PHOENIX_CONNECTION_PREFIX = "jdbc:phoenix:";
	private static final String CALCITE_CONNECTION_PREFIX = "jdbc:phoenixcalcite:";

	private static String HOST;

	private static String CONNECTION_STRING;

	public static void setConnectionString(String connectionString) {
		if (connectionString.startsWith(PHOENIX_CONNECTION_PREFIX))
			HOST = connectionString.substring(PHOENIX_CONNECTION_PREFIX.length());
		else if (connectionString.startsWith(CALCITE_CONNECTION_PREFIX))
			HOST = connectionString.substring(CALCITE_CONNECTION_PREFIX.length());
		else
			throw new RuntimeException("Wrong connectionString = " + connectionString);
		CONNECTION_STRING = connectionString;
		System.err.println(String.format("HOST = %s, CONNECTION_STRING = %s", HOST, CONNECTION_STRING));
	}

	private static Properties getConnectionProps(boolean enableMaterialization) {
		Properties props = new Properties();
		props.setProperty(
				CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
				Boolean.toString(enableMaterialization));
		props.setProperty(
				CalciteConnectionProperty.CREATE_MATERIALIZATIONS.camelName(),
				Boolean.toString(false));
		return props;
	}
	
	protected static Connection getConnection() throws SQLException {
		Properties prop = getConnectionProps(false);
		if (isCalciteConnection()) {
	        final Connection connection = DriverManager.getConnection(CALCITE_CONNECTION_PREFIX, prop);
	        final CalciteConnection calciteConnection =
	            connection.unwrap(CalciteConnection.class);
	        final String url = PHOENIX_CONNECTION_PREFIX + HOST;
	        Map<String, Object> operand = Maps.newHashMap();
	        operand.put("url", url);
			for (Map.Entry<Object, Object> entry : prop.entrySet()) {
				operand.put((String) entry.getKey(), entry.getValue());
			}
	        SchemaPlus rootSchema = calciteConnection.getRootSchema();
	        rootSchema.add("phoenix", PhoenixSchema.FACTORY.create(rootSchema, "phoenix", operand));
	        calciteConnection.setSchema("phoenix");
	        return connection;
		} else
			return DriverManager.getConnection(CONNECTION_STRING, prop);
	}

	static boolean isCalciteConnection() {
	    return CONNECTION_STRING.startsWith(CALCITE_CONNECTION_PREFIX);
	}

    static org.apache.phoenix.jdbc.PhoenixConnection getPhoenixConnection()
            throws SQLException {
        Properties prop = new Properties();
        return (org.apache.phoenix.jdbc.PhoenixConnection) DriverManager.getConnection(PHOENIX_CONNECTION_PREFIX + HOST, prop);
    }
}
