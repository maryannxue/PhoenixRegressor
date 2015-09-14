package com.salesforce;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import com.google.gson.Gson;
import org.apache.log4j.PropertyConfigurator;

/**
 * Main class for PHoenix Regressor
 * @author mchohan
 *
 */
public class PhoenixRegressor {
	
	private static final String RESULT_GSON_EXT = "result";
	private static PerformanceResult performanceResults = new PerformanceResult();

	public static enum TaskType {
		PUBLISH, EXECUTE_TEST, EXPLAIN_TEST, E_PUBLISH
	}

	public static TaskType curTask;
	
	/**
	 * Main static entry point to Phoenix Regressor
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		TestSettings ts = new Gson().fromJson(FileUtils.readFileToString(new File("settings.json")), TestSettings.class);

		if (args.length != 0 && args[0].toLowerCase().equals("publish")) {
			curTask = TaskType.PUBLISH;
			// only generate HTML comparative results
			ResultGenerator.publish(ts);
		} else if (args.length != 0 && args[0].toLowerCase().equals("epublish")) {
			curTask = TaskType.E_PUBLISH;
			// only generate HTML comparative results
			ResultGenerator.publish(ts);
		} else if (args.length != 0 && args[0].toLowerCase().equals("explain")) {
			curTask = TaskType.EXPLAIN_TEST;
            runPerformanceTests(ts);
        } else {
			curTask = TaskType.EXECUTE_TEST;
			// run perf suite
			runPerformanceTests(ts);
		}
	}

	public static boolean isExplainTest() {
		return curTask == TaskType.EXPLAIN_TEST;
	}
	
	private static void runPerformanceTests(TestSettings ts) throws Exception {
		PhoenixConnection.setConnectionString(ts.CONNECTION_STRING);
		
		if (System.getProperty("PERF_TEST_PATH") != null) {
			ts.TEST = System.getProperty("PERF_TEST_PATH");
		}
		
		if (System.getProperty("QUERY_ITERATIONS") != null) {
			ts.ITERATIONS = Integer.parseInt(System.getProperty("QUERY_ITERATIONS"));
		}
		
        File folder = new File(ts.TEST);
		File[] listOfFiles = folder.listFiles(); 
		
		for (File testFile: listOfFiles) {
			if (testFile.isFile()) 
				runTestGson(testFile.toString(), ts.LOAD_DATA, ts.ITERATIONS);
		}
		
		String resultFilePath = ts.RESULT + "/" + ts.LABEL + "." + RESULT_GSON_EXT;
		System.out.println("Writing results to " + resultFilePath);
		File resultFile = new File(resultFilePath);
		FileUtils.writeStringToFile(resultFile, new Gson().toJson(performanceResults));
	}

	private static void runTestGson(String testFile, boolean loadAndCreateData, int iterations) throws Exception {
		HashMap<String, Timing> results = new HashMap<String, Timing>(); 
		
		// read test description file
		System.out.println("Testing: " + testFile);
		PerformanceTest performanceTest = new Gson().fromJson(
				FileUtils.readFileToString(new File(testFile)),
				PerformanceTest.class);
		
		// create table and load data
		long upsertDataTime = -3;
		if (loadAndCreateData) {
			DataGenerator.createTable(performanceTest.TABLE, performanceTest.CREATE_DDL);
			if (!isExplainTest()) {
				DataGenerator.generateData(performanceTest.TABLE, performanceTest.ROW_COUNT, performanceTest.FIELDS);
				upsertDataTime = DataGenerator.upsertData(performanceTest.TABLE);
			} else
				upsertDataTime = 0;
		}
		
		// measure query performance
		for (PerformanceTest.Query query : performanceTest.QUERIES) {
			query.Sql = query.Sql.replace(DataGenerator.TABLE_ALIAS, performanceTest.TABLE);
			String sqlModifiedSpecial = modifySpecial(query.Sql, performanceTest);
            if (isExplainTest())
                results.put(query.Sql, new Timing(runQuery(getExplainSql(sqlModifiedSpecial), iterations), getExplainPlan(sqlModifiedSpecial)));
			else
                results.put(query.Sql, new Timing(runQuery(sqlModifiedSpecial, iterations), getExplainPlan(sqlModifiedSpecial)));
		}
		results.put(performanceTest.getLoadString(), new Timing(upsertDataTime, ""));
		performanceResults.testResults.put(performanceTest.TABLE, results);
	}
	
	private static String getExplainPlan(String query) {
		StringBuilder buf = new StringBuilder();
		try {
            String explainSql = getExplainSql(query);
	        System.out.println("getExplainPlan query= " + explainSql);
			PreparedStatement statement = PhoenixConnection.getConnection().prepareStatement(explainSql);
			ResultSet rs = statement.executeQuery();
	        
	        while (rs.next()) {
	            buf.append(rs.getString(1));
	            buf.append('\n');
	        }
	        if (buf.length() > 0) {
	            buf.setLength(buf.length()-1);
	        }

			statement.close();
		} catch (Exception e) {
		}
		System.out.println("EXPLAIN: "+ buf.toString());
		return buf.toString();
	}
	
	private static String modifySpecial(String query, PerformanceTest performanceTest) {
		String special = "?,?,?...";		
		if (query.contains(special)) {
			String rpl = "";
			for (int i=0; i<performanceTest.ROW_COUNT; i++) {
				rpl += "'" + StringUtils.leftPad(i + "", performanceTest.FIELDS[0].length, 'x') + "',";
			}
			rpl = rpl.substring(0, rpl.length()-1);
			query = query.replace(special, rpl);
		}
		return query;
	}

    private static String getExplainSql(String sql) {
        return (PhoenixConnection.isCalciteConnection() ? "EXPLAIN PLAN FOR " : "EXPLAIN ") + sql;
    }
	
	private static long runQuery(String query, int iterations) {
		try {
			List<Long> result = new ArrayList<Long>();
			
			System.out.println("runQuery query=" + query);
			if (query.toUpperCase().startsWith("SELECT")) {
				PreparedStatement statement = PhoenixConnection.getConnection().prepareStatement(query);
				for (int i=0; i<iterations; i++) {
					long start = isExplainTest() ? System.nanoTime() : System.currentTimeMillis();
					ResultSet rs = statement.executeQuery();
					while (rs.next()) {}
					rs.close();
					result.add((isExplainTest() ? System.nanoTime() : System.currentTimeMillis()) - start);
				}
				statement.close();
			} else {
				Connection conn = PhoenixConnection.getConnection();
				conn.setAutoCommit(true);
				PreparedStatement statement = conn.prepareStatement(query);
				long start = isExplainTest() ? System.nanoTime() : System.currentTimeMillis();
				statement.execute();
				result.add((isExplainTest() ? System.nanoTime() : System.currentTimeMillis()) - start);
				statement.close();
				conn.close();
			}
	
			Collections.sort(result);
			System.out.println(query + " - time:" + result);
			
			return result.get(0);
		} catch (java.sql.SQLFeatureNotSupportedException e) {
            e.printStackTrace();
			return PerfConstants.NOT_SUPPORTED;
		} catch (SQLException e) {
		    e.printStackTrace();
			return PerfConstants.SQL_EXCEPTION;
		} catch (Exception e) {
            e.printStackTrace();
            return PerfConstants.GENERIC_EXCEPTION;
		}
	}
}
