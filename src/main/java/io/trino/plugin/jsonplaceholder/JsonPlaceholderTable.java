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
import io.trino.plugin.jsonplaceholder.filter.FilterType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JsonPlaceholderTable
        implements TableDef
{
    private final String name;
    private final List<JsonPlaceholderColumn> columns;
    private final URI baseUri;

    public JsonPlaceholderTable(
            String name,
            List<JsonPlaceholderColumn> columns,
            URI baseUri)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = requireNonNull(name, "name is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.baseUri = baseUri;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public List<JsonPlaceholderColumn> getColumns()
    {
        return columns;
    }

    @Override
    public List<ColumnMetadata> getColumnsMetadata()
    {
        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (JsonPlaceholderColumn column : this.columns) {
            columnsMetadata.add(column.asMetadata());
        }
        return columnsMetadata.build();
    }

    @Override
    public ConnectorTableMetadata asMetadata(String schema)
    {
        return new ConnectorTableMetadata(new SchemaTableName(schema, name), getColumnsMetadata());
    }

    @Override
    public List<ConnectorSplit> getPage(ConnectorSession session, JsonPlaceholderMetadata meta, Map<String, ColumnHandle> columns, TupleDomain<ColumnHandle> tableConstraint)
    {
        List<ConnectorSplit> splits = new ArrayList<>();

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
                splits.add(new JsonPlaceholderSplit(baseUri.resolve(format("/posts/%d/comments", id))));
            }
        }
        else {
            splits.add(new JsonPlaceholderSplit(baseUri.resolve("/posts")));
        }

        return splits;
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            JsonPlaceholderMetadata meta,
            JsonPlaceholderTableHandle tableHandle,
            Map<String, ColumnHandle> columns,
            Constraint constraint)
    {
        if (!name.equals("comments")) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isAll() || summary.getDomains().isEmpty()) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> currentConstraint = tableHandle.getConstraint();
        Map<String, FilterType> supportedFilters = ImmutableMap.of("postid", FilterType.EQUAL);

        boolean found = false;
        for (Map.Entry<String, FilterType> entry : supportedFilters.entrySet()) {
            String columnName = entry.getKey();

            ColumnHandle columnHandle = columns.get(columnName);
            if (columnHandle == null) {
                continue;
            }

            if (!summary.getDomains().isPresent()) {
                continue;
            }

            Domain domain = summary.getDomains().get().get(columnHandle);
            if (domain == null) {
                continue;
            }

            // Create constraint for this column
            TupleDomain<ColumnHandle> newConstraint = TupleDomain.withColumnDomains(
                    Map.of(columnHandle, domain));

            if (currentConstraint.getDomains().isPresent() &&
                    currentConstraint.getDomains().get().containsKey(columnHandle)) {
                throw new AssertionError("Constraint was already applied");
            }

            currentConstraint = currentConstraint.intersect(newConstraint);

            found = true;

            // Remove the applied constraints
            summary = summary.filter((ch, d) -> !ch.equals(columnHandle));
        }

        if (!found) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                    tableHandle.withConstraint(currentConstraint),
                    summary,
                    constraint.getExpression(),
                    false));
    }
}
