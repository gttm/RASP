package gr.gttm.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.apache.storm.jdbc.common.ConnectionProvider;

public class PhoenixConnectionProvider implements ConnectionProvider {
	private static final long serialVersionUID = 2857688176093179943L;
	private static final Logger LOG = Logger.getLogger(PhoenixConnectionProvider.class);
	private Connection connection;
	
    @Override
    public void prepare() {
    	try {
    		connection = DriverManager.getConnection("jdbc:phoenix:zookeeper");
		} catch (SQLException e) {
			LOG.error("Failed to get phoenix connection");
			e.printStackTrace();
		}
    }

    @Override
    public Connection getConnection() {
    	return this.connection;
    }

    @Override
    public void cleanup() {
    	try {
    		connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }
}
