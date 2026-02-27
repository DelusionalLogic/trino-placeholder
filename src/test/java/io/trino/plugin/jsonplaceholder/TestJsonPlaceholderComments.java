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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJsonPlaceholderComments
{
    private JsonPlaceholderHttpServer httpServer;
    private JsonPlaceholderClient client;
    private JsonPlaceholderMetadata metadata;
    private JsonPlaceholderSplitManager splitManager;
    private JsonPlaceholderRecordSetProvider recordSetProvider;

    @BeforeAll
    public void setUp()
    {
        httpServer = new JsonPlaceholderHttpServer();
        JsonPlaceholderConfig config = new JsonPlaceholderConfig()
                .setApiBaseUri(httpServer.resolve("/"));
        client = new JsonPlaceholderClient(config);
        metadata = new JsonPlaceholderMetadata(client);
        splitManager = new JsonPlaceholderSplitManager(client);
        recordSetProvider = new JsonPlaceholderRecordSetProvider();
    }

    @AfterAll
    public void tearDown()
    {
        httpServer.stop();
    }

    @Test
    public void testCommentsTableExists()
    {
        JsonPlaceholderTable table = client.getTable("default", "comments");
        assertThat(table).isNotNull();
        assertThat(table.getColumns()).hasSize(5);
        assertThat(table.getColumns().stream().map(JsonPlaceholderColumn::getName))
                .containsExactly("postid", "id", "name", "email", "body");
    }

    @Test
    public void testCommentsTableHandle()
    {
        ConnectorTableHandle handle = metadata.getTableHandle(
                SESSION,
                new io.trino.spi.connector.SchemaTableName("default", "comments"),
                Optional.empty(),
                Optional.empty());
        assertThat(handle).isNotNull();
        assertThat(handle).isInstanceOf(JsonPlaceholderTableHandle.class);
    }

    @Test
    public void testApplyFilterWithPostId()
    {
        JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "comments", TupleDomain.all());
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, tableHandle);

        JsonPlaceholderColumnHandle postIdColumn = (JsonPlaceholderColumnHandle) columns.get("postid");
        Domain domain = Domain.singleValue(BIGINT, 1L);
        TupleDomain<ColumnHandle> summary = TupleDomain.withColumnDomains(
                ImmutableMap.of(postIdColumn, domain));
        Constraint constraint = new Constraint(summary);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(SESSION, tableHandle, constraint);

        assertThat(result).isPresent();
        JsonPlaceholderTableHandle newHandle = (JsonPlaceholderTableHandle) result.get().getHandle();
        assertThat(newHandle.getConstraint().getDomains()).isPresent();
    }

    @Test
    public void testSplitManagerWithFilter()
            throws Exception
    {
        // Create table handle with postid = 1 filter
        JsonPlaceholderColumnHandle postIdColumn = new JsonPlaceholderColumnHandle("postid", BIGINT, 0);
        Domain domain = Domain.singleValue(BIGINT, 1L);
        TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(
                ImmutableMap.of(postIdColumn, domain));

        JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "comments", constraint);

        ConnectorSplitSource splitSource = splitManager.getSplits(
                JsonPlaceholderTransactionHandle.INSTANCE,
                SESSION,
                tableHandle,
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10).get().getSplits();
        assertThat(splits).hasSize(1);
        JsonPlaceholderSplit split = (JsonPlaceholderSplit) splits.get(0);
        assertThat(split.getUri()).contains("/posts/1/comments");
    }

    @Test
    public void testSplitManagerWithoutFilterThrowsException()
    {
        JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "comments", TupleDomain.all());

        assertThatThrownBy(() -> splitManager.getSplits(
                JsonPlaceholderTransactionHandle.INSTANCE,
                SESSION,
                tableHandle,
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Missing required filter")
                .hasMessageContaining("postid");
    }

    @Test
    public void testReadCommentsData()
            throws Exception
    {
        // Create table handle with postid = 1 filter
        JsonPlaceholderColumnHandle postIdColumn = new JsonPlaceholderColumnHandle("postid", BIGINT, 0);
        Domain domain = Domain.singleValue(BIGINT, 1L);
        TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(
                ImmutableMap.of(postIdColumn, domain));

        JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "comments", constraint);

        // Get splits
        ConnectorSplitSource splitSource = splitManager.getSplits(
                JsonPlaceholderTransactionHandle.INSTANCE,
                SESSION,
                tableHandle,
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10).get().getSplits();
        JsonPlaceholderSplit split = (JsonPlaceholderSplit) splits.get(0);

        // Read data
        List<JsonPlaceholderColumnHandle> columnHandles = ImmutableList.of(
                new JsonPlaceholderColumnHandle("postid", BIGINT, 0),
                new JsonPlaceholderColumnHandle("id", BIGINT, 1),
                new JsonPlaceholderColumnHandle("name", createUnboundedVarcharType(), 2),
                new JsonPlaceholderColumnHandle("email", createUnboundedVarcharType(), 3),
                new JsonPlaceholderColumnHandle("body", createUnboundedVarcharType(), 4));

        RecordSet recordSet = recordSetProvider.getRecordSet(
                JsonPlaceholderTransactionHandle.INSTANCE,
                SESSION,
                split,
                tableHandle,
                columnHandles);

        RecordCursor cursor = recordSet.cursor();

        int count = 0;
        while (cursor.advanceNextPosition()) {
            long postId = cursor.getLong(0);
            long id = cursor.getLong(1);
            String name = cursor.getSlice(2).toStringUtf8();
            String email = cursor.getSlice(3).toStringUtf8();
            String body = cursor.getSlice(4).toStringUtf8();

            assertThat(postId).isEqualTo(1L);
            assertThat(id).isGreaterThan(0);
            assertThat(name).isNotEmpty();
            assertThat(email).isNotEmpty();
            assertThat(body).isNotEmpty();

            count++;
        }

        assertThat(count).isEqualTo(5);
    }

    @Test
    public void testReadCommentsForPost2()
            throws Exception
    {
        // Create table handle with postid = 2 filter
        JsonPlaceholderColumnHandle postIdColumn = new JsonPlaceholderColumnHandle("postid", BIGINT, 0);
        Domain domain = Domain.singleValue(BIGINT, 2L);
        TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(
                ImmutableMap.of(postIdColumn, domain));

        JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "comments", constraint);

        // Get splits
        ConnectorSplitSource splitSource = splitManager.getSplits(
                JsonPlaceholderTransactionHandle.INSTANCE,
                SESSION,
                tableHandle,
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10).get().getSplits();
        JsonPlaceholderSplit split0 = (JsonPlaceholderSplit) splits.get(0);
        assertThat(split0.getUri()).contains("/posts/2/comments");

        // Read data
        List<JsonPlaceholderColumnHandle> columnHandles = ImmutableList.of(
                new JsonPlaceholderColumnHandle("postid", BIGINT, 0),
                new JsonPlaceholderColumnHandle("id", BIGINT, 1));

        RecordSet recordSet = recordSetProvider.getRecordSet(
                JsonPlaceholderTransactionHandle.INSTANCE,
                SESSION,
                split0,
                tableHandle,
                columnHandles);

        RecordCursor cursor = recordSet.cursor();

        int count = 0;
        while (cursor.advanceNextPosition()) {
            long postId = cursor.getLong(0);
            long id = cursor.getLong(1);

            assertThat(postId).isEqualTo(2L);
            assertThat(id).isBetween(6L, 10L);

            count++;
        }

        assertThat(count).isEqualTo(5);
    }
}
