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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static java.util.Objects.requireNonNull;

public class JsonPlaceholderTable
{
    private final String name;
    private final List<JsonPlaceholderColumn> columns;
    private final List<URI> sources;

    public JsonPlaceholderTable(
            String name,
            List<JsonPlaceholderColumn> columns,
            List<URI> sources)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = requireNonNull(name, "name is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));
    }

    public String getName()
    {
        return name;
    }

    public List<JsonPlaceholderColumn> getColumns()
    {
        return columns;
    }

    public List<URI> getSources()
    {
        return sources;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (JsonPlaceholderColumn column : this.columns) {
            columnsMetadata.add(column.asMetadata());
        }
        return columnsMetadata.build();
    }

    public ConnectorTableMetadata asMetadata(String schema)
    {
        return new ConnectorTableMetadata(new SchemaTableName(schema, name), getColumnsMetadata());
    }

    public List<ConnectorSplit> getPage(ConnectorSession session, JsonPlaceholderMetadata meta, Map<String, ColumnHandle> columns, TupleDomain<ColumnHandle> tableConstraint)
    {
        List<ConnectorSplit> splits = new ArrayList<>();
        for (var uri : getSources()) {
            String uriString = uri.toString();

            // Handle URI templates for comments table
            if (name.equals("comments")) {
                var postIdColumn = columns.get("postid");

                if (!tableConstraint.getDomains().isPresent()) {
                    throw new TrinoException(INVALID_ROW_FILTER, "Missing required filter: postid");
                }

                Domain domain = tableConstraint.getDomains().get().get(postIdColumn);
                if (domain == null) {
                    throw new TrinoException(INVALID_ROW_FILTER, "Missing required filter: postid");
                }

                for (var id : domain.getValues().tryExpandRanges(1024).get()) {
                    splits.add(new JsonPlaceholderSplit(uriString.replace("__POSTID__", id.toString())));
                }
            }
            else {
                splits.add(new JsonPlaceholderSplit(uriString));
            }
        }

        return splits;
    }
}
