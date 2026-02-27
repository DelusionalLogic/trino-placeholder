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

import com.google.inject.Inject;
import io.trino.plugin.jsonplaceholder.filter.CommentsFilterApplier;
import io.trino.plugin.jsonplaceholder.filter.FilterApplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static java.util.Objects.requireNonNull;

public class JsonPlaceholderSplitManager
        implements ConnectorSplitManager
{
    private final JsonPlaceholderClient exampleClient;

    @Inject
    public JsonPlaceholderSplitManager(JsonPlaceholderClient exampleClient)
    {
        this.exampleClient = requireNonNull(exampleClient, "exampleClient is null");
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
        JsonPlaceholderTable table = exampleClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());

        // this can happen if table is removed during a query
        if (table == null) {
            throw new TableNotFoundException(tableHandle.toSchemaTableName());
        }

        List<ConnectorSplit> splits = new ArrayList<>();
        for (URI uri : table.getSources()) {
            String uriString = uri.toString();

            // Handle URI templates for comments table
            if (tableHandle.getTableName().equals("comments")) {
                uriString = resolveUriTemplate(uriString, tableHandle);
            }

            splits.add(new JsonPlaceholderSplit(uriString));
        }
        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }

    private String resolveUriTemplate(String uriTemplate, JsonPlaceholderTableHandle tableHandle)
    {
        // For comments table, extract the postid filter value
        if (uriTemplate.contains("__POSTID__")) {
            FilterApplier filterApplier = new CommentsFilterApplier();
            TupleDomain<ColumnHandle> constraint = tableHandle.getConstraint();

            // Create a temporary column handle to extract the filter
            JsonPlaceholderColumnHandle postIdColumn = new JsonPlaceholderColumnHandle("postid", io.trino.spi.type.BigintType.BIGINT, 0);
            Object postId = filterApplier.getFilter(postIdColumn, constraint);

            if (postId == null) {
                throw new TrinoException(INVALID_ROW_FILTER,
                        "Missing required filter: comments table requires a filter on postid column. " +
                                "Example: SELECT * FROM comments WHERE postid = 1");
            }

            return uriTemplate.replace("__POSTID__", postId.toString());
        }

        return uriTemplate;
    }
}
