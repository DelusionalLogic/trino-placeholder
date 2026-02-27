/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.jsonplaceholder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.io.ByteSource;
import com.google.common.io.CountingInputStream;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

public class JsonPlaceholderRecordCursor
        implements RecordCursor
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final List<JsonPlaceholderColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private final Iterator<Map<String, Object>> posts;
    private final long totalBytes;

    private Map<String, Object> currentPost;

    public JsonPlaceholderRecordCursor(List<JsonPlaceholderColumnHandle> columnHandles, ByteSource byteSource)
    {
        this.columnHandles = columnHandles;

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            JsonPlaceholderColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }

        try (CountingInputStream input = new CountingInputStream(byteSource.openStream())) {
            List<Map<String, Object>> postsList = OBJECT_MAPPER.readValue(
                    input,
                    new TypeReference<List<Map<String, Object>>>() {});
            posts = postsList.iterator();
            totalBytes = input.getCount();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!posts.hasNext()) {
            return false;
        }
        currentPost = posts.next();
        return true;
    }

    private Object getFieldValue(int field)
    {
        checkState(currentPost != null, "Cursor has not been advanced yet");

        JsonPlaceholderColumnHandle columnHandle = columnHandles.get(field);
        String columnName = columnHandle.getColumnName();

        // Map column names to JSON field names (Trino uses lowercase, JSON uses camelCase)
        String jsonFieldName = columnName;
        if ("userid".equals(columnName)) {
            jsonFieldName = "userId";
        }

        return currentPost.get(jsonFieldName);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        Object value = getFieldValue(field);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(String.valueOf(value));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        Object value = getFieldValue(field);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        Object value = getFieldValue(field);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return Double.parseDouble(String.valueOf(value));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        Object value = getFieldValue(field);
        if (value == null) {
            return Slices.utf8Slice("");
        }
        return Slices.utf8Slice(String.valueOf(value));
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        Object value = getFieldValue(field);
        return value == null || (value instanceof String && Strings.isNullOrEmpty((String) value));
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close() {}
}
