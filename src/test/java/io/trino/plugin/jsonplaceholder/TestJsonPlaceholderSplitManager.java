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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJsonPlaceholderSplitManager
{
    @Test
    public void testGetSplitsForPostsTable()
            throws Exception
    {
        JsonPlaceholderHttpServer httpServer = new JsonPlaceholderHttpServer();
        JsonPlaceholderConfig config = new JsonPlaceholderConfig()
                .setApiBaseUri(httpServer.getUri());
        JsonPlaceholderClient client = new JsonPlaceholderClient(config);
        JsonPlaceholderMetadata meta = new JsonPlaceholderMetadata(client);
        JsonPlaceholderSplitManager splitManager = new JsonPlaceholderSplitManager(client, meta);

        JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "posts", TupleDomain.all());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                JsonPlaceholderTransactionHandle.INSTANCE,
                SESSION,
                tableHandle,
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue());

        List<ConnectorSplit> splits = splitSource.getNextBatch(10).get().getSplits();
        assertThat(splits).hasSize(1);
        JsonPlaceholderSplit split = (JsonPlaceholderSplit) splits.get(0);
        assertThat(split.getUri().getPath()).contains("/posts");

        httpServer.stop();
    }

    @Test
    public void testGetSplitsForCommentsWithPostIdFilter()
            throws Exception
    {
        JsonPlaceholderHttpServer httpServer = new JsonPlaceholderHttpServer();
        JsonPlaceholderConfig config = new JsonPlaceholderConfig()
                .setApiBaseUri(httpServer.getUri());
        JsonPlaceholderClient client = new JsonPlaceholderClient(config);
        JsonPlaceholderMetadata meta = new JsonPlaceholderMetadata(client);
        JsonPlaceholderSplitManager splitManager = new JsonPlaceholderSplitManager(client, meta);

        // Create table handle with postid = 1 filter
        JsonPlaceholderColumnHandle postIdColumn = new JsonPlaceholderColumnHandle("postid", BIGINT);
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
        assertThat(split.getUri().getPath()).contains("/posts/1/comments");

        httpServer.stop();
    }

    @Test
    public void testGetSplitsForCommentsWithoutFilterThrowsException()
    {
        JsonPlaceholderHttpServer httpServer = new JsonPlaceholderHttpServer();
        JsonPlaceholderConfig config = new JsonPlaceholderConfig()
                .setApiBaseUri(httpServer.getUri());
        JsonPlaceholderClient client = new JsonPlaceholderClient(config);
        JsonPlaceholderMetadata meta = new JsonPlaceholderMetadata(client);
        JsonPlaceholderSplitManager splitManager = new JsonPlaceholderSplitManager(client, meta);

        JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "comments", TupleDomain.all());

        assertThatThrownBy(() -> splitManager.getSplits(
                JsonPlaceholderTransactionHandle.INSTANCE,
                SESSION,
                tableHandle,
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue())
                .getNextBatch(10))
                .isInstanceOf(TrinoException.class);

        httpServer.stop();
    }

    @Test
    public void testGetSplitsForNonExistentTable()
    {
        JsonPlaceholderHttpServer httpServer = new JsonPlaceholderHttpServer();
        JsonPlaceholderConfig config = new JsonPlaceholderConfig()
                .setApiBaseUri(httpServer.getUri());
        JsonPlaceholderClient client = new JsonPlaceholderClient(config);
        JsonPlaceholderMetadata meta = new JsonPlaceholderMetadata(client);
        JsonPlaceholderSplitManager splitManager = new JsonPlaceholderSplitManager(client, meta);

        JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "unknown", TupleDomain.all());

        assertThatThrownBy(() -> splitManager.getSplits(
                JsonPlaceholderTransactionHandle.INSTANCE,
                SESSION,
                tableHandle,
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue())
                .getNextBatch(10))
                .isInstanceOf(TableNotFoundException.class);

        httpServer.stop();
    }
}
