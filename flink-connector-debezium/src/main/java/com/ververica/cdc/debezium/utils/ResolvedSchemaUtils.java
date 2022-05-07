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

package com.ververica.cdc.debezium.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.api.constraints.UniqueConstraint;


import java.util.List;

/**
 * Utilities to {@link TableSchema}.
 */
@Internal
public class ResolvedSchemaUtils {
    private ResolvedSchemaUtils() {
    }

    /**
     * Return {@link TableSchema} which consists of all physical columns.
     */
    public static TableSchema getPhysicalSchema(TableSchema tableSchema) {

        WatermarkSpec watermarkSpec = tableSchema.getWatermarkSpecs().get(0);
        UniqueConstraint uniqueConstraint = tableSchema.getPrimaryKey().orElse(null);
        String primaryKeyName = uniqueConstraint.getName();
        List<String> columnList = uniqueConstraint.getColumns();
        String[] columnArr = columnList.toArray(new String[columnList.size()]);
        TableSchema.Builder builder = TableSchema.builder()
                .watermark(watermarkSpec)
                .primaryKey(primaryKeyName, columnArr);
        tableSchema.getTableColumns().stream()
                .filter(TableColumn::isPhysical)
                .map(item->builder.add(item));

        return builder.build();
    }
}
