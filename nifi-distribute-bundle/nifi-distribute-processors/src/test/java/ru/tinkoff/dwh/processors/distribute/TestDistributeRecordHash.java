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
package ru.tinkoff.dwh.processors.distribute;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Relationship;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.ArrayListRecordReader;

import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;


public class TestDistributeRecordHash {

    static ArrayListRecordReader recordReader;
    static ArrayListRecordWriter recordWriter;

    private final String firstNode  = "1 node";
    private final String secondNode = "2 node";
    private final String thirdNode  = "3 node";
    private final String fifthNode  = "5 node";

    final TestRunner testRunner = TestRunners.newTestRunner(new DistributeRecordHash());

    @Before
    public void before() throws InitializationException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.DistributeRecord", "debug");

        List<Record> records = Stream.iterate(0, i -> i*2)
                .limit(10)
                .map(i -> createRecord(i, "name_"+i, "NY", i*3))
                .collect(Collectors.toList());

        recordReader = new ArrayListRecordReader(records.get(0).getSchema());
        records.forEach(recordReader::addRecord);
        recordWriter = new ArrayListRecordWriter(records.get(0).getSchema());

        testRunner.addControllerService("reader", recordReader);
        testRunner.addControllerService("writer", recordWriter);
        testRunner.enableControllerService(recordReader);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(DistributeRecordHash.RECORD_READER, "reader");
        testRunner.setProperty(DistributeRecordHash.RECORD_WRITER, "writer");

        testRunner.setProperty(DistributeRecordHash.KEYS, "age");
        testRunner.enqueue("_");
        testRunner.setProperty(firstNode, "1");
        testRunner.setProperty(secondNode, "1");
    }

    @Test
    public void testSingleIntegerKey() {
        testRunner.run();
        testRunner.assertTransferCount(DistributeRecordHash.FAILURE,0);
        testRunner.assertTransferCount(firstNode, 1);
    }

    @Test
    public void testClickHouseHash(){
        DistributeRecordHash processor = new DistributeRecordHash();
        assertEquals(processor.hash("1-qwe-TTR", "murmurhash_32"), 2821231608L);
    }

    @Test
    public void TestKeyCreation(){
        DistributeRecordHash processor = new DistributeRecordHash();
        Record record = createRecord(23, "ilya", null, 99);
        List<String> keys = Arrays.asList("id","name","city","age");
        assertEquals("23-ilya-0-99", processor.createKey(keys, record));
    }

    @Test
    public void testComplexKeyDistribution() {
        testRunner.setProperty(DistributeRecordHash.KEYS, "id,name");
        testRunner.enqueue(new byte[0]);
        testRunner.run();
        testRunner.assertTransferCount(DistributeRecordHash.FAILURE,0);
        testRunner.assertTransferCount(firstNode, 1);
        testRunner.assertTransferCount(secondNode, 0);
    }

    @Test
    public void testNonExistentKeys(){
        DistributeRecordHash processor = new DistributeRecordHash();
        List<String> keys = Arrays.asList("id", "name", "age", "other_key");
        List<String> nonExistingKeys = processor.nonexistentKeys(keys, createSchema());
        assertEquals(1, nonExistingKeys.size());
        assertEquals("other_key", nonExistingKeys.get(0));
    }

    @Test
    public void testDistribute(){
        DistributeRecordHash processor = new DistributeRecordHash();
        List<String> keys = Arrays.asList("id");
        Relationship rel1 = new Relationship.Builder().name("1").build();
        Relationship rel2 = new Relationship.Builder().name("2").build();
        List<Relationship> weightedRelationships = Arrays.asList(rel1, rel2); // 1/1
        Relationship targetRelation = processor.distribute(keys, weightedRelationships,
                createRecord(1, "Endrew", "Minsk", 49), DistributeRecordHash.MURMURHASH_32);
        assertEquals(rel2, targetRelation);
    }

    @Test
    public void testWeightRelationship(){
        testRunner.setProperty(fifthNode, "3");
        testRunner.setProperty(thirdNode, "2");
        testRunner.run();
        Map<PropertyDescriptor, String> props = testRunner.getProcessContext().getProperties();
        DistributeRecordHash processor = (DistributeRecordHash) testRunner.getProcessor();
        List<Relationship> weightedRels = processor.weightRels(props);
        List<Relationship> expectedRels = Arrays.asList(createRelFromName(firstNode),
                createRelFromName(secondNode),
                createRelFromName(thirdNode),
                createRelFromName(thirdNode),
                createRelFromName(fifthNode),
                createRelFromName(fifthNode),
                createRelFromName(fifthNode));
        assertEquals(expectedRels, weightedRels);
    }

    @Test
    public void testExtractKeys(){
        String keys = "id,name ,age, country";
        DistributeRecordHash processor = (DistributeRecordHash) testRunner.getProcessor();
        List<String> extractedKeys = processor.extractKeys(keys,",");
        List<String> expectedKeys = Arrays.asList("id", "name", "age", "country");
        assertEquals(expectedKeys, extractedKeys);
    }

    @Test(expected = DistributeRecordHash.DistributeException.class)
    public void testDistributeExceptionWithZeroKeys(){
        DistributeRecordHash processor = (DistributeRecordHash) testRunner.getProcessor();
        processor.validateKeys(Collections.EMPTY_LIST, createSchema());
    }

    @Test(expected = DistributeRecordHash.DistributeException.class)
    public void testDistributeExceptionWithIncorrectKeys(){
        DistributeRecordHash processor = (DistributeRecordHash) testRunner.getProcessor();
        processor.validateKeys(Arrays.asList("id", "name", "country"), createSchema());
    }

    private Relationship createRelFromName(String name){
        return new Relationship.Builder().name(name).build();
    }

    private RecordSchema createSchema(){
        List<RecordField> fields = Arrays.asList(new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType()),
                new RecordField("city", RecordFieldType.STRING.getDataType()),
                new RecordField("age", RecordFieldType.INT.getDataType()));
        return new SimpleRecordSchema(fields);
    }

    private Record createRecord(int id, String name, String city, int age) {
        RecordSchema schema = createSchema();
        Map<String, Object> values = new HashMap<>();
        values.put("id", id);
        values.put("name", name);
        values.put("city", city);
        values.put("age", age);
        return new MapRecord(schema, values);
    }
}