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
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJsonPlaceholderRecordSetProvider
{
    private JsonPlaceholderHttpServer exampleHttpServer;
    private String dataUri;
    private String commentsPost1Uri;
    private String commentsPost2Uri;

    @Test
    public void testGetRecordSet()
    {
        ConnectorTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "posts", TupleDomain.all());
        JsonPlaceholderRecordSetProvider recordSetProvider = new JsonPlaceholderRecordSetProvider();
        RecordSet recordSet = recordSetProvider.getRecordSet(JsonPlaceholderTransactionHandle.INSTANCE, SESSION, new JsonPlaceholderSplit(dataUri), tableHandle, ImmutableList.of(
                new JsonPlaceholderColumnHandle("id", BIGINT),
                new JsonPlaceholderColumnHandle("title", createUnboundedVarcharType())));
        assertThat(recordSet)
                .describedAs("recordSet is null")
                .isNotNull();

        RecordCursor cursor = recordSet.cursor();
        assertThat(cursor)
                .describedAs("cursor is null")
                .isNotNull();

        Map<Long, String> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getLong(0), cursor.getSlice(1).toStringUtf8());
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

    //
    // Start http server for testing
    //

    @BeforeAll
    public void setUp()
    {
        exampleHttpServer = new JsonPlaceholderHttpServer();
        dataUri = exampleHttpServer.resolve("/jsonplaceholder-data/posts.json").toString();
        commentsPost1Uri = exampleHttpServer.resolve("/posts/1/comments").toString();
        commentsPost2Uri = exampleHttpServer.resolve("/posts/2/comments").toString();
    }

    @AfterAll
    public void tearDown()
    {
        if (exampleHttpServer != null) {
            exampleHttpServer.stop();
        }
    }

    @Test
    public void testGetRecordSetForCommentsPost1()
    {
        ConnectorTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "comments", TupleDomain.all());
        JsonPlaceholderRecordSetProvider recordSetProvider = new JsonPlaceholderRecordSetProvider();
        RecordSet recordSet = recordSetProvider.getRecordSet(
                JsonPlaceholderTransactionHandle.INSTANCE,
                SESSION,
                new JsonPlaceholderSplit(commentsPost1Uri),
                tableHandle,
                ImmutableList.of(
                        new JsonPlaceholderColumnHandle("postid", BIGINT),
                        new JsonPlaceholderColumnHandle("id", BIGINT),
                        new JsonPlaceholderColumnHandle("name", createUnboundedVarcharType()),
                        new JsonPlaceholderColumnHandle("email", createUnboundedVarcharType()),
                        new JsonPlaceholderColumnHandle("body", createUnboundedVarcharType())));

        assertThat(recordSet).isNotNull();

        RecordCursor cursor = recordSet.cursor();
        assertThat(cursor).isNotNull();

        // Verify first comment
        assertThat(cursor.advanceNextPosition()).isTrue();
        assertThat(cursor.getLong(0)).isEqualTo(1L); // postid
        assertThat(cursor.getLong(1)).isEqualTo(1L); // id
        assertThat(cursor.getSlice(2).toStringUtf8()).isEqualTo("id labore ex et quam laborum"); // name
        assertThat(cursor.getSlice(3).toStringUtf8()).isEqualTo("Eliseo@gardner.biz"); // email
        assertThat(cursor.getSlice(4).toStringUtf8()).startsWith("laudantium enim quasi est quidem magnam"); // body

        // Count remaining records and verify all have postid = 1
        int count = 1;
        while (cursor.advanceNextPosition()) {
            assertThat(cursor.getLong(0)).isEqualTo(1L); // All should have postid = 1
            count++;
        }

        assertThat(count).isEqualTo(5);
    }

    @Test
    public void testGetRecordSetForCommentsPost2()
    {
        ConnectorTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "comments", TupleDomain.all());
        JsonPlaceholderRecordSetProvider recordSetProvider = new JsonPlaceholderRecordSetProvider();
        RecordSet recordSet = recordSetProvider.getRecordSet(
                JsonPlaceholderTransactionHandle.INSTANCE,
                SESSION,
                new JsonPlaceholderSplit(commentsPost2Uri),
                tableHandle,
                ImmutableList.of(
                        new JsonPlaceholderColumnHandle("postid", BIGINT),
                        new JsonPlaceholderColumnHandle("id", BIGINT)));

        assertThat(recordSet).isNotNull();

        RecordCursor cursor = recordSet.cursor();
        Map<Long, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            long postId = cursor.getLong(0);
            long id = cursor.getLong(1);
            data.put(id, postId);
        }

        // Verify we got exactly 5 comments with ids 6-10, all with postid = 2
        assertThat(data).isEqualTo(ImmutableMap.<Long, Long>builder()
                .put(6L, 2L)
                .put(7L, 2L)
                .put(8L, 2L)
                .put(9L, 2L)
                .put(10L, 2L)
                .buildOrThrow());
    }
}
