package ru.tinkoff.dwh.processors.nifi;

import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.ArrayListRecordReader;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.nifi.serialization.record.RecordFieldType.INT;
import static org.apache.nifi.serialization.record.RecordFieldType.STRING;
import static org.junit.Assert.*;

public class PutClickHouseTest {

	@ClassRule
	public static TemporaryFolder folder = new TemporaryFolder();

	public static final String personsTable = "PERSONS";
	private static final String createPersons = String.format("CREATE TABLE %s (id integer primary key, name varchar(100), code integer)", personsTable);
	private static TestRunner runner;
	/**
	 * Setting up Connection pooling is expensive operation.
	 * So let's do this only once and reuse MockDBCPService in each test.
	 */
	static protected DBCPService service;
	static protected ArrayListRecordReader recordReader;
	static protected RecordSchema schema;

	@BeforeClass
	public static void setupClass() throws ProcessException, SQLException {
		System.setProperty("derby.stream.error.file", "target/derby.log");
		final File tempDir = folder.getRoot();
		final File dbDir = new File(tempDir, "db");
		service = new MockDBCPService(dbDir.getAbsolutePath());
		try (final Connection conn = service.getConnection()) {
			try (final Statement stmt = conn.createStatement()) {
				stmt.executeUpdate(createPersons);
			}
		}
		schema = new SimpleRecordSchema(Arrays.asList(
				new RecordField("ID", INT.getDataType()),
				new RecordField("NAME", STRING.getDataType()),
				new RecordField("CODE", INT.getDataType())
		));
	}

	@Before
	public void initRunner() throws InitializationException, SQLException {
		runner = TestRunners.newTestRunner(PutClickHouse.class);
		recordReader = new ArrayListRecordReader(schema);
		IntStream.range(1,10)
				.mapToObj(i -> createRecord(schema, i, "NoName", i*20))
				.forEach(recordReader::addRecord);
		runner.addControllerService("reader", recordReader);
		runner.addControllerService("dbcp", service);
		runner.enableControllerService(recordReader);
		runner.enableControllerService(service);
		runner.setProperty(PutClickHouse.RECORD_READER, "reader");
		runner.setProperty(PutClickHouse.DBCP, "dbcp");
		runner.setProperty(PutClickHouse.TABLE_NAME, personsTable);
		runner.setProperty(PutClickHouse.INPUT_THRESHOLD, "3");
		runner.setProperty(PutClickHouse.MAX_BATCH_SIZE, "10");
		runner.setProperty(PutClickHouse.BATCH_AGE, "1 sec");
		recreateTable("PERSONS", createPersons);
	}

	@Test
	public void testGroupingByTable(){
		String emartTable = "EMART";
		String martiniTable = "MARTINI";
		runner.setProperty(PutClickHouse.TABLE_NAME, "${table.name}");
		Map<String,String> attributes = new HashMap<>();
		attributes.put("record.count","10");
		Map<String,String> attributesEM = new HashMap<>(attributes);
		attributesEM.put("table.name", emartTable);
		List<FlowFile> filesEMART = IntStream.range(0,10).mapToObj(i-> runner.enqueue("content", attributesEM)).collect(Collectors.toList());
		Map<String,String> attributesMART = new HashMap<>(attributes);
		attributesMART.put("table.name", martiniTable);
		List<FlowFile> filesMARTINI = IntStream.range(0,15).mapToObj(i-> runner.enqueue("content", attributesMART)).collect(Collectors.toList());
		PutClickHouse processor = new PutClickHouse();
		filesEMART.addAll(filesMARTINI);
		Map<String, List<FlowFile>> grouped = processor.groupFlowFilesBy(filesEMART , PutClickHouse.TABLE_NAME,runner.getProcessContext());
		assertEquals(2, grouped.keySet().size());
		assertEquals(10, grouped.get(emartTable).size());
		assertEquals(15, grouped.get(martiniTable).size());

		Map<String, Long> dataAge = new HashMap<>();
		dataAge.put(emartTable, System.currentTimeMillis());
		dataAge.put(martiniTable, System.currentTimeMillis());
		Predicate predicate = processor.getPredicate(3000, 1000000L, dataAge);
		Map<Boolean, Map<String, List<FlowFile>>>  readyToPut = processor.readyToPut(grouped, predicate);
		assertFalse(readyToPut.containsKey(true));

		dataAge.put(emartTable, System.currentTimeMillis()+10000L);
		predicate = processor.getPredicate(3000, 0L, dataAge);
		readyToPut = processor.readyToPut(grouped, predicate);
		assertTrue(readyToPut.get(false).containsKey(emartTable));
		assertTrue(readyToPut.get(true).containsKey(martiniTable));

	}

	@Test
	public void testReadyToPut(){
		runner.setProperty(PutClickHouse.TABLE_NAME, "${table.name}");
		Map<String,String> attributesEM = new HashMap<>();
		attributesEM.put("table.name", "EMART");
		List<FlowFile> filesEMART = IntStream.range(0,10).mapToObj(i-> runner.enqueue("content", attributesEM)).collect(Collectors.toList());
		Map<String,String> attributesMART = new HashMap<>();
		attributesMART.put("table.name", "MARTINI");
		List<FlowFile> filesMARTINI = IntStream.range(0,15).mapToObj(i-> runner.enqueue("content", attributesMART)).collect(Collectors.toList());
		PutClickHouse processor = new PutClickHouse();
		filesEMART.addAll(filesMARTINI);
		Map<String, List<FlowFile>> grouped = processor.groupFlowFilesBy(filesEMART , PutClickHouse.TABLE_NAME,runner.getProcessContext());

	}

	@Test
	public void testDirectStatements() throws ProcessException, SQLException {
		Map<String,String> attributes = new HashMap<>();
		attributes.put("record.count", "10");
		runner.enqueue(new byte[0], attributes);
		runner.run();
		runner.assertAllFlowFilesTransferred(PutClickHouse.REL_SUCCESS, 1);
		int count = 0;
		try (final Connection conn = service.getConnection()) {
			try (final Statement stmt = conn.createStatement()) {
				final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
				assertTrue(rs.next());
				count++;
				assertEquals(1, rs.getInt(1));
				assertEquals("NoName", rs.getString(2));
				assertEquals(20, rs.getInt(3));
				assertTrue(rs.next());
				count++;
				assertEquals(2, rs.getInt(1));
				assertEquals("NoName", rs.getString(2));
				assertEquals(40, rs.getInt(3));

				while (rs.next()){
					count++;
				}
				assertEquals(9, count);
			}
		}
	}

	private static void recreateTable(String tableName, String createSQL) throws ProcessException, SQLException {
		try (final Connection conn = service.getConnection()) {
			try (final Statement stmt = conn.createStatement()) {
				stmt.executeUpdate("drop table " + tableName);
				stmt.executeUpdate(createSQL);
			}
		}
	}

	@Test
	public void testSqlGenerating(){
		PutClickHouse processor = new PutClickHouse();
		String sqlInsert = processor.generateSql("EMART_MARTINI", Arrays.asList("id","callid","sequence"));
		assertEquals("INSERT INTO EMART_MARTINI (id,callid,sequence) VALUES (?,?,?)", sqlInsert);
	}

	public static Record createRecord(RecordSchema schema, int id, String name, int code){
		Map<String, Object> map = new HashMap<>();
		map.put("ID", id);
		map.put("NAME", name);
		map.put("CODE", code);
		return new MapRecord(schema, map);
	}

}