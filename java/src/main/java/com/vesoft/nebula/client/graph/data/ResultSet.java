/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.data;

import com.google.common.collect.Lists;
import com.vesoft.nebula.Row;
import com.vesoft.nebula.Value;
import com.vesoft.nebula.graph.ErrorCode;
import com.vesoft.nebula.graph.ExecutionResponse;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

public class ResultSet {
    private final int code;
    private String errorMessage;
    private List<String> columnNames;
    private List<Record> records;

    public static class Record implements Iterable<ValueWrapper> {
        private final List<ValueWrapper> colValues = new ArrayList<>();
        private List<String> columnNames = new ArrayList<>();

        public Record(List<String> columnNames, Row row) {
            if (columnNames == null) {
                return;
            }

            if (row == null || row.values == null) {
                return;
            }

            for (Value value : row.values) {
                this.colValues.add(new ValueWrapper(value));
            }

            this.columnNames = columnNames;
        }

        @Override
        public Iterator<ValueWrapper> iterator() {
            return this.colValues.iterator();
        }

        @Override
        public void forEach(Consumer<? super ValueWrapper> action) {
            this.colValues.forEach(action);
        }

        @Override
        public Spliterator<ValueWrapper> spliterator() {
            return this.colValues.spliterator();
        }


        @Override
        public String toString() {
            StringBuilder rowStr = new StringBuilder();
            for (ValueWrapper v : colValues) {
                rowStr.append(v.toString()).append(',');
            }
            return "Record{row=" + rowStr
                    + ", columnNames=" + columnNames.toString()
                    + '}';
        }

        public int getType(int index) {
            return get(index).getType();
        }

        public int getType(String key) {
            return get(key).getType();
        }

        public ValueWrapper get(int index) {
            if (index >= columnNames.size()) {
                throw new IllegalArgumentException(
                        String.format("Cannot get field because the key '%d' out of range", index));
            }
            return this.colValues.get(index);
        }

        public ValueWrapper get(String key) {
            int index = columnNames.indexOf(key);
            if (index == -1) {
                throw new IllegalArgumentException(
                        "Cannot get field because the key '" + key + "' is not exist");
            }
            return this.colValues.get(index);
        }

        public int size() {
            return this.columnNames.size();
        }

        public boolean contains(String key) {
            return this.columnNames.contains(key);
        }

    }

    /**
     * Constructor
     */
    public ResultSet() {
        this(new ExecutionResponse());
    }

    public ResultSet(ExecutionResponse resp) {
        if (resp.error_msg != null) {
            errorMessage = new String(resp.error_msg).intern();
        }
        code = resp.error_code;
        if (resp.data != null) {
            this.columnNames = Lists.newArrayListWithCapacity(resp.data.column_names.size());
            this.records = Lists.newArrayListWithCapacity(resp.data.rows.size());
            for (byte[] column : resp.data.column_names) {
                this.columnNames.add(new String(column).intern());
            }
            this.records = new ArrayList<>(resp.data.rows.size());
            for (Row row : resp.data.rows) {
                this.records.add(new Record(this.columnNames, row));
            }
        }
    }

    public boolean isSucceeded() {
        return this.code == ErrorCode.SUCCEEDED;
    }

    public int getErrorCode() {
        return this.code;
    }

    public String getErrorMessage() {
        return this.errorMessage;
    }

    public List<String> getColumnNames() {
        return this.columnNames;
    }

    public List<Record> getRecords() {
        return this.records;
    }
}
