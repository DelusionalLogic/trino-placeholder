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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJsonPlaceholderRecordSet
{
    private JsonPlaceholderHttpServer exampleHttpServer;
    private URI dataUri;

    @Test
    public void testGetColumnTypes()
    {
        RecordSet recordSet = new JsonPlaceholderRecordSet(new JsonPlaceholderSplit(dataUri), ImmutableList.of(
                new JsonPlaceholderColumnHandle("userid", BIGINT),
                new JsonPlaceholderColumnHandle("id", BIGINT),
                new JsonPlaceholderColumnHandle("title", createUnboundedVarcharType()),
                new JsonPlaceholderColumnHandle("body", createUnboundedVarcharType())));
        assertThat(recordSet.getColumnTypes()).isEqualTo(ImmutableList.of(BIGINT, BIGINT, createUnboundedVarcharType(), createUnboundedVarcharType()));

        recordSet = new JsonPlaceholderRecordSet(new JsonPlaceholderSplit(dataUri), ImmutableList.of(
                new JsonPlaceholderColumnHandle("id", BIGINT),
                new JsonPlaceholderColumnHandle("title", createUnboundedVarcharType())));
        assertThat(recordSet.getColumnTypes()).isEqualTo(ImmutableList.of(BIGINT, createUnboundedVarcharType()));

        recordSet = new JsonPlaceholderRecordSet(new JsonPlaceholderSplit(dataUri), ImmutableList.of());
        assertThat(recordSet.getColumnTypes()).isEqualTo(ImmutableList.of());
    }

    @Test
    public void testCursorSimple()
    {
        RecordSet recordSet = new JsonPlaceholderRecordSet(new JsonPlaceholderSplit(dataUri), ImmutableList.of(
                new JsonPlaceholderColumnHandle("userid", BIGINT),
                new JsonPlaceholderColumnHandle("id", BIGINT),
                new JsonPlaceholderColumnHandle("title", createUnboundedVarcharType())));
        RecordCursor cursor = recordSet.cursor();

        assertThat(cursor.getType(0)).isEqualTo(BIGINT);
        assertThat(cursor.getType(1)).isEqualTo(BIGINT);
        assertThat(cursor.getType(2)).isEqualTo(createUnboundedVarcharType());

        Map<Long, String> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getLong(1), cursor.getSlice(2).toStringUtf8());
            assertThat(cursor.isNull(0)).isFalse();
            assertThat(cursor.isNull(1)).isFalse();
            assertThat(cursor.isNull(2)).isFalse();
        }
        assertThat(data).isEqualTo(ImmutableMap.<Long, String>builder()
                .put(1L, "sunt aut facere repellat provident occaecati excepturi optio reprehenderit")
                .put(2L, "qui est esse")
                .put(3L, "ea molestias quasi exercitationem repellat qui ipsa sit aut")
                .put(4L, "eum et est occaecati")
                .put(5L, "nesciunt quas odio")
                .put(6L, "dolorem eum magni eos aperiam quia")
                .put(7L, "magnam facilis autem")
                .put(8L, "dolorem dolore est ipsam")
                .put(9L, "nesciunt iure omnis dolorem tempora et accusantium")
                .put(10L, "optio molestias id quia eum")
                .buildOrThrow());
    }

    @Test
    public void testCursorMixedOrder()
    {
        RecordSet recordSet = new JsonPlaceholderRecordSet(new JsonPlaceholderSplit(dataUri), ImmutableList.of(
                new JsonPlaceholderColumnHandle("title", createUnboundedVarcharType()),
                new JsonPlaceholderColumnHandle("id", BIGINT),
                new JsonPlaceholderColumnHandle("userid", BIGINT)));
        RecordCursor cursor = recordSet.cursor();

        Map<Long, String> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getLong(1), cursor.getSlice(0).toStringUtf8());
        }
        assertThat(data).isEqualTo(ImmutableMap.<Long, String>builder()
                .put(1L, "sunt aut facere repellat provident occaecati excepturi optio reprehenderit")
                .put(2L, "qui est esse")
                .put(3L, "ea molestias quasi exercitationem repellat qui ipsa sit aut")
                .put(4L, "eum et est occaecati")
                .put(5L, "nesciunt quas odio")
                .put(6L, "dolorem eum magni eos aperiam quia")
                .put(7L, "magnam facilis autem")
                .put(8L, "dolorem dolore est ipsam")
                .put(9L, "nesciunt iure omnis dolorem tempora et accusantium")
                .put(10L, "optio molestias id quia eum")
                .buildOrThrow());
    }

    @Test
    public void testCursorWithBody()
    {
        RecordSet recordSet = new JsonPlaceholderRecordSet(new JsonPlaceholderSplit(dataUri), ImmutableList.of(
                new JsonPlaceholderColumnHandle("id", BIGINT),
                new JsonPlaceholderColumnHandle("body", createUnboundedVarcharType())));
        RecordCursor cursor = recordSet.cursor();

        int count = 0;
        while (cursor.advanceNextPosition()) {
            count++;
            long id = cursor.getLong(0);
            String body = cursor.getSlice(1).toStringUtf8();

            assertThat(cursor.isNull(0)).isFalse();
            assertThat(cursor.isNull(1)).isFalse();
            assertThat(id).isGreaterThan(0);
            assertThat(body).isNotEmpty();

            // Body field contains newlines, verify it's preserved
            if (id == 1) {
                assertThat(body).contains("\n");
            }
        }
        assertThat(count).isEqualTo(10);
    }

    @Test
    public void testCursorAllColumns()
    {
        RecordSet recordSet = new JsonPlaceholderRecordSet(new JsonPlaceholderSplit(dataUri), ImmutableList.of(
                new JsonPlaceholderColumnHandle("userid", BIGINT),
                new JsonPlaceholderColumnHandle("id", BIGINT),
                new JsonPlaceholderColumnHandle("title", createUnboundedVarcharType()),
                new JsonPlaceholderColumnHandle("body", createUnboundedVarcharType())));
        RecordCursor cursor = recordSet.cursor();

        int count = 0;
        while (cursor.advanceNextPosition()) {
            count++;
            assertThat(cursor.getLong(0)).isGreaterThan(0); // userId
            assertThat(cursor.getLong(1)).isGreaterThan(0); // id
            assertThat(cursor.getSlice(2).toStringUtf8()).isNotEmpty(); // title
            assertThat(cursor.getSlice(3).toStringUtf8()).isNotEmpty(); // body
        }
        assertThat(count).isEqualTo(10);
    }

    @BeforeAll
    public void setUp()
    {
        exampleHttpServer = new JsonPlaceholderHttpServer();
        dataUri = exampleHttpServer.getUri().resolve("/posts");
    }

    @AfterAll
    public void tearDown()
    {
        if (exampleHttpServer != null) {
            exampleHttpServer.stop();
        }
    }
}
