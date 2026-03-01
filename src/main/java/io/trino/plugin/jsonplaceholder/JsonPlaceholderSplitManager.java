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
import com.google.inject.Inject;
import io.trino.plugin.jsonplaceholder.filter.CommentsFilterApplier;
import io.trino.plugin.jsonplaceholder.filter.FilterApplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorSplitSource.ConnectorSplitBatch;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JsonPlaceholderSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.getLogger(JsonPlaceholderSplitManager.class.getName());
    private static final ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitBatch(ImmutableList.of(), false);

    private final JsonPlaceholderClient client;
    private final JsonPlaceholderMetadata meta;

    @Inject
    public JsonPlaceholderSplitManager(JsonPlaceholderClient client, JsonPlaceholderMetadata meta)
    {
        this.client = requireNonNull(client);
        this.meta = requireNonNull(meta);
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        JsonPlaceholderTableHandle tableHandle = (JsonPlaceholderTableHandle) connectorTableHandle;
        return new DynamicFilteringSplitSource(client, meta, session, tableHandle, dynamicFilter, new CommentsFilterApplier());
    }

    private static class DynamicFilteringSplitSource
            implements ConnectorSplitSource
    {
        private static final Logger log = Logger.getLogger(DynamicFilteringSplitSource.class.getName());

        private final JsonPlaceholderClient client;
        private final JsonPlaceholderMetadata meta;
        private final ConnectorSession session;
        private final JsonPlaceholderTableHandle tableHandle;
        private final DynamicFilter filter;
        private final FilterApplier applier;

        DynamicFilteringSplitSource(
                JsonPlaceholderClient client,
                JsonPlaceholderMetadata meta,
                ConnectorSession session,
                JsonPlaceholderTableHandle tableHandle,
                DynamicFilter filter,
                FilterApplier applier)
        {
            this.client = requireNonNull(client);
            this.meta = requireNonNull(meta);
            this.session = requireNonNull(session);
            this.tableHandle = requireNonNull(tableHandle);
            this.filter = requireNonNull(filter);
            this.applier = requireNonNull(applier);
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
        {
            log.warning(format("Dyamic Filter awaitable (%s)", filter.isAwaitable()));
            if (filter.isAwaitable()) {
                return filter.isBlocked()
                    .thenApply(_ -> EMPTY_BATCH);
            }

            log.warning(format("DynamicFilter has resolved (awaitable %s) (constraint %s)", filter.isAwaitable(), filter.getCurrentPredicate()));

            JsonPlaceholderTable table = client.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());

            if (table == null) {
                throw new TableNotFoundException(tableHandle.toSchemaTableName());
            }

            List<ConnectorSplit> splits = new ArrayList<>();
            for (URI uri : table.getSources()) {
                String uriString = uri.toString();

                // Handle URI templates for comments table
                if (tableHandle.getTableName().equals("comments")) {
                    FilterApplier filterApplier = new CommentsFilterApplier();
                    TupleDomain<ColumnHandle> tableConstraint = tableHandle.getConstraint();
                    tableConstraint = tableConstraint.intersect(filter.getCurrentPredicate());

                    JsonPlaceholderColumnHandle postIdColumn = (JsonPlaceholderColumnHandle) meta.getColumnHandles(session, tableHandle).get("postid");
                    var postId = filterApplier.getFilterAll(postIdColumn, tableConstraint);

                    if (postId == null) {
                        throw new TrinoException(INVALID_ROW_FILTER, "Missing required filter: postid");
                    }

                    for (var id : postId) {
                        splits.add(new JsonPlaceholderSplit(uriString.replace("__POSTID__", id.toString())));
                    }
                }
                else {
                    splits.add(new JsonPlaceholderSplit(uriString));
                }
            }

            return CompletableFuture.completedFuture(new ConnectorSplitBatch(splits, true));
        }

        @Override
        public boolean isFinished()
        {
            if (filter.isAwaitable()) {
                return false;
            }

            return true;
        }

        @Override
        public void close()
        {
            // Nothing to do
        }
    }
}
