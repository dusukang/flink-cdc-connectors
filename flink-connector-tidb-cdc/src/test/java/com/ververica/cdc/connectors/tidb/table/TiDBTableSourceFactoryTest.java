/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.tidb.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** Unit tests for TiDB table source factory. */
public class TiDBTableSourceFactoryTest {

    private static final TableSchema SCHEMA = TableSchema.builder()
            .add(TableColumn.computed("aaa", DataTypes.INT().notNull(),"aaa"))
            .add(TableColumn.computed("bbb", DataTypes.STRING().notNull(),"bbb"))
            .add(TableColumn.computed("ccc", DataTypes.DOUBLE(),"ccc"))
            .add(TableColumn.computed("ddd", DataTypes.DECIMAL(31,18).notNull(),"ddd"))
            .add(TableColumn.computed("eee", DataTypes.TIMESTAMP(3),"eee"))
            .watermark(null)
            .primaryKey("pk",new String[]{"bbb", "aaa"})
            .build();

    private static final TableSchema SCHEMA_WITH_METADATA = TableSchema.builder()
            .add(TableColumn.computed("id", DataTypes.BIGINT().notNull(),"id"))
            .add(TableColumn.computed("name", DataTypes.STRING().notNull(),"name"))
            .add(TableColumn.computed("count", DataTypes.DECIMAL(38, 18),"count"))
            .add(TableColumn.metadata("time", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),"op_ts",true))
            .add(TableColumn.metadata("database_name", DataTypes.STRING(),"database_name",true))
            .add(TableColumn.metadata("table_name", DataTypes.STRING(),"table_name",true))
            .add(TableColumn.metadata("op_ts", DataTypes.TIMESTAMP(),"op_ts",true))
            .watermark(null)
            .primaryKey("pk",new String[]{"id"})
            .build();

    private static final String MY_HOSTNAME = "tidb0:4000";
    private static final String MY_DATABASE = "inventory";
    private static final String MY_TABLE = "products";
    private static final String PD_ADDRESS = "pd0:2379";
    private static final Map<String, String> OPTIONS = new HashMap<>();

    @Test
    public void testCommonProperties() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        TiDBTableSource expectedSource =
                new TiDBTableSource(
                        SCHEMA,
                        MY_DATABASE,
                        MY_TABLE,
                        PD_ADDRESS,
                        StartupOptions.latest(),
                        OPTIONS);
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testOptionalProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("tikv.grpc.timeout_in_ms", "20000");
        properties.put("tikv.grpc.scan_timeout_in_ms", "20000");
        properties.put("tikv.batch_get_concurrency", "4");
        properties.put("tikv.batch_put_concurrency", "4");
        properties.put("tikv.batch_scan_concurrency", "4");
        properties.put("tikv.batch_delete_concurrency", "4");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        Map<String, String> options = new HashMap<>();
        options.put("tikv.grpc.timeout_in_ms", "20000");
        options.put("tikv.grpc.scan_timeout_in_ms", "20000");
        options.put("tikv.batch_get_concurrency", "4");
        options.put("tikv.batch_put_concurrency", "4");
        options.put("tikv.batch_scan_concurrency", "4");
        options.put("tikv.batch_delete_concurrency", "4");
        TiDBTableSource expectedSource =
                new TiDBTableSource(
                        SCHEMA,
                        MY_DATABASE,
                        MY_TABLE,
                        PD_ADDRESS,
                        StartupOptions.latest(),
                        options);
        assertEquals(expectedSource, actualSource);
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "tidb-cdc");
        options.put("hostname", MY_HOSTNAME);
        options.put("database-name", MY_DATABASE);
        options.put("table-name", MY_TABLE);
        options.put("pd-addresses", PD_ADDRESS);
        options.put("scan.startup.mode", "latest-offset");
        return options;
    }

    private static DynamicTableSource createTableSource(
            TableSchema schema, Map<String, String> options) {
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new CatalogTableImpl(schema,options,"catalogTableImpl"),
                new Configuration(),
                TiDBTableSourceFactoryTest.class.getClassLoader(),
                false);
    }

    private static DynamicTableSource createTableSource(Map<String, String> options) {
        return createTableSource(SCHEMA, options);
    }
}
