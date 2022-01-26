package io.github.zhangjig.transfer.benchmark;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

public class ConnectorServer {

	private static final Logger logger = LoggerFactory.getLogger(ConnectorServer.class);

	public static final String SERVLET_PREFIX = "servlet.";

	public static final String PROPERTIES_PREFIX = "jetty.";

	public static final String JETTY_CONNECTOR_KEY = "jetty.properties.file";

	public static final String DEFAULT_CONNECTOR_PROPERTIES = "jetty.properties";

	public static final int DEFAULT_THREADS = 200;

	private int port = 8080;
	private int selectors = 4;

	private final int soLinger = -1;
	private final int acceptors = 0;

	private int minThreads = 0;
	private int maxThreads = DEFAULT_THREADS;

	private volatile boolean started = false;

	private Server server;

	synchronized public void start() {

		if (started) {
			return;
		}
		stop();

		Properties props = loadProperties();
		initProperties(props);

		QueuedThreadPool threadPool = new QueuedThreadPool();
		threadPool.setName("jetty");
		threadPool.setDaemon(true);
		threadPool.setMinThreads(minThreads);
		threadPool.setMaxThreads(maxThreads);

		Server server = new Server(threadPool);
		ServerConnector connector = new ServerConnector(server, acceptors, selectors);
		connector.setPort(port);
		connector.setSoLingerTime(soLinger);
		connector.getByteBufferPool();
		server.addConnector(connector);

		// handler
		ServletHandler handler = new ServletHandler();

		Map<String, ServletHolder> holders = parseServletHolders(props);
		for (Entry<String, ServletHolder> entry : holders.entrySet()) {
			String path = entry.getKey();
			ServletHolder holder = entry.getValue();
			handler.addServletWithMapping(holder, path);
		}
		server.setHandler(handler);

		try {
			server.start();
			this.server = server;
			started = true;
			if (logger.isInfoEnabled()) {
				logger.info("Jetty connector(" + this.port + ") startup success!");
			}
		} catch (Exception e) {
			throw new IllegalStateException("Failed to start jetty server on " + port + ", cause: " + e.getMessage(),
					e);
		}
	}

	private void initProperties(Properties props) {
		for (String key : props.stringPropertyNames()) {
			if (!key.startsWith(PROPERTIES_PREFIX)) {
				continue;
			}
			String name = key.substring(PROPERTIES_PREFIX.length());
			String value = props.getProperty(key);
			switch (name) {
			case "port":
				this.port = Integer.parseInt(value);
				break;
			case "selectors":
				this.selectors = Integer.parseInt(value);
				break;
			case "minThreads":
				this.minThreads = Integer.parseInt(value);
				break;
			case "maxThreads":
				this.maxThreads = Integer.parseInt(value);
				break;
			default:
				break;
			}
		}
	}

	synchronized public void stop() {
		try {
			if(server != null) {
				server.stop();
				server.join();
			}
			started = false;
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
		}
	}

	private Map<String, ServletHolder> parseServletHolders(Properties props) {
		Map<String, Map<String, String>> servlets = parseProperties(props);

		Map<String, ServletHolder> holders = new HashMap<>();
		for (Entry<String, Map<String, String>> entry : servlets.entrySet()) {
			ServletHolder holder = new ServletHolder();
			String name = entry.getKey();
			Map<String, String> config = entry.getValue();

			String urlPattern = config.remove("url-pattern");
			if (urlPattern == null) {
				throw new RuntimeException(
						"can't found url-pattern for servlet " + name + ". please check galaxy.connector.properties");
			}

			if (config.containsKey("name")) {
				name = config.remove("name");
			}
			holder.setName(name);

			String servletClass = config.remove("class");
			if (servletClass != null) {
				holder.setClassName(servletClass);
			} else {
				throw new RuntimeException("can't found class for serlvet " + name
						+ ". please check class in galaxy.connector.properties");
			}

			int order = 2;
			String orderStr = config.remove("order");
			if (orderStr != null) {
				try {
					order = Integer.parseInt(orderStr);
				} catch (NumberFormatException e) {
					// ignore
				}
			}
			holder.setInitOrder(order);
			holder.setInitParameters(config);

			holders.put(urlPattern, holder);
		}

		return holders;
	}

	private Map<String, Map<String, String>> parseProperties(Properties props) {

		Map<String, Map<String, String>> servlets = new HashMap<>();

		for (String key : props.stringPropertyNames()) {
			if (!key.startsWith(SERVLET_PREFIX)) {
				continue;
			}
			int i = key.indexOf(".", SERVLET_PREFIX.length());
			if (i == -1) {
				i = key.length();
			}
			String name = key.substring(SERVLET_PREFIX.length(), i);

			Map<String, String> servlet = servlets.get(name);
			if (servlet == null) {
				servlet = new HashMap<>();
				servlets.put(name, servlet);
			}
			String property = null;
			if (i == key.length()) {
				property = "url-pattern";
			} else {
				property = key.substring(i + 1);
			}

			String value = trimToNull(props.getProperty(key));
			if (value != null) {
				servlet.put(property, value);
			}
		}
		return servlets;
	}

	public static Properties loadProperties() {
		String path = System.getProperty(JETTY_CONNECTOR_KEY);
		if (path == null || path.length() == 0) {
			path = System.getenv(JETTY_CONNECTOR_KEY);
			if (path == null || path.length() == 0) {
				path = DEFAULT_CONNECTOR_PROPERTIES;
			}
		}

		try (InputStream is = openInputStream(path)) {
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

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getSelectors() {
		return selectors;
	}

	public void setSelectors(int selectors) {
		this.selectors = selectors;
	}

	public int getMinThreads() {
		return minThreads;
	}

	public void setMinThreads(int minThreads) {
		this.minThreads = minThreads;
	}

	public int getMaxThreads() {
		return maxThreads;
	}

	public void setMaxThreads(int maxThreads) {
		this.maxThreads = maxThreads;
	}

	private static String trimToNull(String str) {
		if (str == null) {
			return null;
		}
		str = str.trim();
		return str.isEmpty() ? null : str;
	}

	public boolean isStarted() {
		return started;
	}

	public Server getServer() {
		return server;
	}
}
