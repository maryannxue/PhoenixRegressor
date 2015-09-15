package com.salesforce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

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
	
	protected static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(CONNECTION_STRING);
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
