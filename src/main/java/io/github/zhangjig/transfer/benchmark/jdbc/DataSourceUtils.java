package io.github.zhangjig.transfer.benchmark.jdbc;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DataSourceUtils {

	private static final String DRUID_PROPERTIES_FOLDER = "druid/";
	
	private static volatile Map<String, DataSource> dataSources = new HashMap<>();

	
	public static Connection getPhysicalConnection(String dataSourceId) throws Exception {
		try {
			return getPhysicalDataSource(dataSourceId).getConnection();
		} catch (SQLException e) {
			throw new Exception("DataSourceId: " + dataSourceId + "\n" + e.getMessage(), e);
		}
	}
	
	public static DataSource getPhysicalDataSource(String dataSourceId) {
		DataSource dataSouce = dataSources.get(dataSourceId);
		if(dataSouce == null) {
			dataSouce = loadDataSource(dataSourceId);
		}
		return dataSouce;
	}
	


	
	synchronized private static DataSource loadDataSource(String dataSourceId) {
		if(dataSources.get(dataSourceId) != null) {
			return dataSources.get(dataSourceId);
		}
		
		Map<String, DataSource> dataSources = new HashMap<>(DataSourceUtils.dataSources);
		Properties props = loadProperties(dataSourceId);
		try {
			DruidDataSource druid = new DruidDataSource();
			DruidDataSourceFactory.config(druid, props);
			druid.init();
			
			dataSources.put(dataSourceId, druid);
			DataSourceUtils.dataSources = dataSources;
			return druid;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private static Properties loadProperties(String file) {
		String path = DRUID_PROPERTIES_FOLDER + file + ".properties";

		try (InputStream is = openInputStream(path)) {
			if(is == null) {
				throw new IllegalArgumentException("config file " + path + " can't be null");
			}
			Properties props = new Properties();
			props.load(is);
			return props;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static InputStream openInputStream(String path) {
		return Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
	}
	
	public static void main(String[] args) throws Throwable {

	}
}
