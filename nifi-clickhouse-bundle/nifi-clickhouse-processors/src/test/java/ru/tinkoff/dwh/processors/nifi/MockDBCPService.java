package ru.tinkoff.dwh.processors.nifi;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;

import java.sql.Connection;
import java.sql.DriverManager;

public class MockDBCPService extends AbstractControllerService implements DBCPService {

	/**
	 * Simple implementation only for testing purposes
	 */

	private final String dbLocation;

	public MockDBCPService(final String dbLocation) {
		this.dbLocation = dbLocation;
	}

	@Override
	public String getIdentifier() {
		return "dbcp";
	}

	@Override
	public Connection getConnection() throws ProcessException {
		try {
			Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
			final Connection conn = DriverManager.getConnection("jdbc:derby:" + dbLocation + ";create=true");
			return conn;
		} catch (final Exception e) {
			e.printStackTrace();
			throw new ProcessException("getConnection failed: " + e);
		}
	}


}
