/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.tinkoff.dwh.processors.nifi;

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.type.ArrayDataType;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Tags({"ClickHouse", "Put"})
@CapabilityDescription("Processor for put records into clickhouse. " +
        "Processor get all flowfiles from queue then group them by table name and " +
        "if sum of record.count attribute > input threshold size or age of batch expired processor will execute batch " +
        "insert into table with max batch size that set in according property. Processor requires record.count attribute " +
        "for counting records and effective insert")
@ReadsAttributes({@ReadsAttribute(attribute = "records.count", description = "Necessary attribute for batch insert")})
public class PutClickHouse extends AbstractProcessor {

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor
            .Builder().name("Reader")
            .displayName("Record reader")
            .description("reader")
            .required(true)
            .identifiesControllerService(RecordReaderFactory.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor
            .Builder().name("tableName")
            .displayName("Table name")
            .description("Table name of clickhouse table (table might be distributed)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DBCP = new PropertyDescriptor
            .Builder().name("dbcp")
            .displayName("dbcp connection pool")
            .description("Connection pool for one instance of clickhouse, " +
                    "so for every clickhouse \"Shard\" must be created 1 connection pool service")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INPUT_THRESHOLD = new PropertyDescriptor
            .Builder().name("input threshold")
            .displayName("input threshold")
            .description("input threshold count of records for insert")
            .required(true)
            .defaultValue("50000")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor
            .Builder().name("max batch size")
            .displayName("max batch size")
            .description("max batch size for insert")
            .required(true)
            .defaultValue("50000")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_AGE = new PropertyDescriptor
            .Builder().name("batch age")
            .displayName("batch age")
            .description("Processor set batch age once for every table name and clear" +
                    " this state after put operation. So for example if you have ")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Inserted flowfiles")
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("relationship for retry")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("failed flowfiles")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships = Collections.synchronizedSet(new HashSet<>());
    private Connection connection;
    private int batchSize;
    private long batchAge;
    private int inputThreshold;
    private RecordReaderFactory factory;
    private ConcurrentMap<String, Long> dataAge = new ConcurrentHashMap<>();


    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = Arrays.asList(
                TABLE_NAME,
                DBCP,
                RECORD_READER,
                INPUT_THRESHOLD,
                MAX_BATCH_SIZE,
                BATCH_AGE
        );
        this.descriptors = Collections.unmodifiableList(descriptors);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_RETRY);
        relationships.add(REL_FAILURE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onSchedule(ProcessContext context) {
        final DBCPService dbcpService = context.getProperty(DBCP).asControllerService(DBCPService.class);
        batchSize = context.getProperty(MAX_BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        batchAge = context.getProperty(BATCH_AGE).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        factory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        inputThreshold = context.getProperty(INPUT_THRESHOLD).evaluateAttributeExpressions().asInteger();
        try {
            this.connection = dbcpService.getConnection();
        } catch (Exception e) {
            getLogger().error(e.getMessage());
        }
    }

    @OnStopped
    public void clearState() {
        dataAge.clear();
	    try {
		    connection.close();
	    } catch (SQLException e) {
		    throw new ProcessException(e);
	    }
    }

    public List<FlowFile> fetchFlowFiles(ProcessSession session, Integer count) {
        if (count == -1)
            return session.get(session.getQueueSize().getObjectCount());
        return session.get(count);
    }

    public Map<String, List<FlowFile>> groupFlowFilesBy(List<FlowFile> flowFiles,
                                                        PropertyDescriptor attribute,
                                                        ProcessContext context) {
        return flowFiles.stream().collect(Collectors.groupingBy(flowFile -> context.getProperty(attribute)
                        .evaluateAttributeExpressions(flowFile)
                        .getValue()));
    }

    public Map<Boolean, Map<String, List<FlowFile>>> readyToPut(Map<String, List<FlowFile>> groups,
                                                                Predicate<Map.Entry<String, List<FlowFile>>> p) {
        return groups.entrySet().stream().collect(Collectors.groupingBy(p::test,
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    public boolean isExpired(String dataname, long batchAge, Map<String, Long> map) {
        return System.currentTimeMillis() - map.get(dataname) >= batchAge;
    }

    public Predicate<Map.Entry<String, List<FlowFile>>> getPredicate(int inputThreshold, long batchAge, Map<String, Long> dataAge){
        return entry -> entry.getValue().stream()
                .mapToInt(flowFile ->Integer.parseInt(flowFile.getAttribute("record.count")))
                .sum() > inputThreshold || isExpired(entry.getKey(), batchAge, dataAge);
    }

    public Map<Boolean, List<FlowFile>> put(Connection connection,
                                            String tableName,
                                            ProcessSession session,
                                            RecordReaderFactory factory,
                                            List<FlowFile> flowFiles,
                                            int maxBatchSize) throws SQLException, IOException {
        List<String> fieldNames = new LinkedList<>();
        try (InputStream inputStream = session.read(flowFiles.get(0))) {
            RecordReader recordReader = factory.createRecordReader(flowFiles.get(0), inputStream, getLogger());
            fieldNames = recordReader.getSchema().getFieldNames();
        } catch (MalformedRecordException | SchemaNotFoundException | IOException e) {
            getLogger().error(e.getMessage());
        }

        String sql = generateSql(tableName, fieldNames);
        PreparedStatement statement = connection.prepareStatement(sql);
        List<FlowFile> insertedFlowFiles = new LinkedList<>();
        List<FlowFile> failedFlowFiles = new LinkedList<>();
        for (FlowFile flowFile : flowFiles) {
            int count = 0;
            try (InputStream inputStream1 = session.read(flowFile)) {
                RecordReader reader = factory.createRecordReader(flowFile, inputStream1, getLogger());
                Record record;
                while ((record = reader.nextRecord()) != null) {
                    addRecord(statement, record, connection);
                    statement.addBatch();
                    count++;
                    if (count % maxBatchSize == 0) {
                        statement.executeBatch();
                        statement.clearBatch();
                    }
                }
                reader.close();
                insertedFlowFiles.add(flowFile);
            } catch (IllegalArgumentException
                    | MalformedRecordException
                    | SQLNonTransientException
                    | SchemaNotFoundException e) {
                getLogger().error(e.getMessage());
                failedFlowFiles.add(flowFile);
            }
        }
        statement.executeBatch();
        statement.close();
        Map<Boolean, List<FlowFile>> groupedFiles = new HashMap<>();
        groupedFiles.put(true, insertedFlowFiles);
        groupedFiles.put(false, failedFlowFiles);
        return groupedFiles;
    }

    public void addRecord(PreparedStatement statement, Record record, Connection connection) throws SQLException {
        int position = 1;
        for (String field : record.getSchema().getFieldNames()) {
            Object value = record.getValue(field);
            if (value == null) {
                statement.setNull(position++, 0);// 0 is a mock value => internally clickhouse jdbc doesn't use this argument
                continue;
            }
            Optional<DataType> dt = record.getSchema().getDataType(field);
            if (!dt.isPresent()) {
                statement.setNull(position++, 0);
                continue;
            }
            RecordFieldType type = record.getSchema().getDataType(field).get().getFieldType();
            switch (type) {
                case STRING:
                    statement.setString(position++, (String) value);
                    break;
                case BOOLEAN:
                    statement.setBoolean(position++, (Boolean) value);
                    break;
                case INT:
                    statement.setInt(position++, (int) value);
                    break;
                case SHORT:
                    statement.setShort(position++, (short) value);
                    break;
                case BYTE:
                    statement.setByte(position++, (Byte) value);
                    break;
                case LONG:
                    statement.setLong(position++, (Long) value);
                    break;
                case BIGINT:
                    statement.setBigDecimal(position++, (BigDecimal) value);
                    break;
                case FLOAT:
                    statement.setFloat(position++, (Float) value);
                    break;
                case DOUBLE:
                    statement.setDouble(position++, (Double) value);
                    break;
                case DATE:
                    statement.setDate(position++, (java.sql.Date) value);
                    break;
                case TIME:
                    statement.setTime(position++, (Time) value);
                    break;
                case TIMESTAMP:
                    statement.setTimestamp(position++, (Timestamp) value);
                    break;
                case ARRAY:
                    final ArrayDataType arrayDataType = (ArrayDataType) type.getDataType();
                    final DataType elementType = arrayDataType.getElementType();
                    Array array = connection.createArrayOf(elementType.toString(), (Object[]) value);
                    statement.setArray(position++, array);
            }
        }
    }

    public String generateSql(String tableName, List<String> fields) {
        if(fields==null || fields.isEmpty()) throw new ProcessException("Fields list for generateSql is empty or null");
        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                tableName,
                String.join(",",fields),
                Stream.generate(() -> "?").limit(fields.size()).collect(Collectors.joining(",")));
    }

    public void penalizeFiles(ProcessSession session, List<FlowFile> flowFiles) {
        List<FlowFile> penalizedFlowFiles = flowFiles.stream().map(session::penalize).collect(Collectors.toList());
        session.transfer(penalizedFlowFiles);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        List<FlowFile> allFlowFiles = fetchFlowFiles(session, -1);
        if (allFlowFiles.isEmpty()) {
            return;
        }
        try {
            if (connection==null || !connection.isValid(5) ) {
                getLogger().error("Connection is NOT valid!!!");
                penalizeFiles(session, allFlowFiles);
                context.yield();
                connection.close();
                onSchedule(context);
                return;
            }
        } catch (SQLException | RuntimeException e) {
            getLogger().error(e.getMessage());
            session.transfer(allFlowFiles, REL_RETRY);
            return;
        }
        Map<Boolean,List<FlowFile>> hasRecordCount = allFlowFiles.stream()
                .collect(Collectors.partitioningBy(ff -> ff.getAttribute("record.count")!=null));
        if (hasRecordCount.get(false).size()>0){
            getLogger().error("Some flowfiles does't have record.count attribute");
            hasRecordCount.get(false).forEach(ff -> session.transfer(ff, REL_FAILURE));
        }
        //group files by table name
        Map<String, List<FlowFile>> filesGroupedByTableName = groupFlowFilesBy(hasRecordCount.get(true), TABLE_NAME, context);
        filesGroupedByTableName.forEach((key, __) -> dataAge.putIfAbsent(key, System.currentTimeMillis()));
        Predicate<Map.Entry<String, List<FlowFile>>> readyToPutPredicate = getPredicate(inputThreshold, batchAge, dataAge);
        //group by `ready to put`, `table name`
        Map<Boolean, Map<String, List<FlowFile>>> filesGroupedByReadyToPut = readyToPut(filesGroupedByTableName, readyToPutPredicate);
        Map<String, List<FlowFile>> filesToPut = Optional.ofNullable(filesGroupedByReadyToPut.get(true)).orElse(new HashMap<>());
        Map<String, List<FlowFile>> filesNotReadyToPut = Optional.ofNullable(filesGroupedByReadyToPut.get(false)).orElse(new HashMap<>());
        for (Map.Entry<String, List<FlowFile>> entry : filesToPut.entrySet()) {
            String key = entry.getKey();
            List<FlowFile> value = entry.getValue();
            try {
                Map<Boolean, List<FlowFile>> groupedFiles =
                        put(connection, key, session, factory, value, batchSize);
                session.transfer(groupedFiles.get(true), REL_SUCCESS);
                session.transfer(groupedFiles.get(false), REL_FAILURE);
            } catch (SQLException sqlEx) {
                switch (sqlEx.getErrorCode()) {
                    case 202:
                    case 203:
                    case 209:
                    case 210:
                    case 296:
                    case 297:
                    case 410:
                        session.transfer(value, REL_RETRY);
                        break;
                    default:
                        session.transfer(value, REL_FAILURE);
                }
            } catch (IOException e) {
                getLogger().error(e.getMessage());
                session.transfer(value, REL_FAILURE);
            }finally {
                dataAge.remove(key);
            }
        }
        if(filesNotReadyToPut.size()==allFlowFiles.size()){
            session.rollback();
            context.yield();
        }else {
            filesNotReadyToPut.values().forEach(session::transfer);
        }
    }
}
