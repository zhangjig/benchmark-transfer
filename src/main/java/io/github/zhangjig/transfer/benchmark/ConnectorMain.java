package io.github.zhangjig.transfer.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectorMain {
	
	private static final Logger logger = LoggerFactory.getLogger(ConnectorMain.class);
	
	public static void main(String[] args) {
		ConnectorServer server = new ConnectorServer();
		server.start();
		try {
			server.getServer().join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
