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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.jsonplaceholder.filter.CommentsFilterApplier;
import io.trino.plugin.jsonplaceholder.filter.FilterApplier;
import io.trino.plugin.jsonplaceholder.filter.FilterType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JsonPlaceholderMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.getLogger(FilterApplier.class.getName());

    private final JsonPlaceholderClient exampleClient;
    private final Map<String, FilterApplier> filterAppliers;

    @Inject
    public JsonPlaceholderMetadata(JsonPlaceholderClient exampleClient)
    {
        this.exampleClient = requireNonNull(exampleClient, "exampleClient is null");
        this.filterAppliers = ImmutableMap.of("comments", new CommentsFilterApplier());
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
        return ImmutableList.copyOf(exampleClient.getSchemaNames());
    }

    @Override
    public JsonPlaceholderTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        JsonPlaceholderTable table = exampleClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new JsonPlaceholderTableHandle(tableName.getSchemaName(), tableName.getTableName(), TupleDomain.all());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return getTableMetadata(((JsonPlaceholderTableHandle) table).toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        Set<String> schemaNames = optionalSchemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(exampleClient.getSchemaNames()));

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : exampleClient.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        JsonPlaceholderTableHandle exampleTableHandle = (JsonPlaceholderTableHandle) tableHandle;

        JsonPlaceholderTable table = exampleClient.getTable(exampleTableHandle.getSchemaName(), exampleTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(exampleTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            columnHandles.put(column.getName(), new JsonPlaceholderColumnHandle(column.getName(), column.getType()));
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        return listTables(session, prefix).stream()
                .map(tableName -> {
                    ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
                    // table can disappear during listing operation
                    if (tableMetadata != null) {
                        return TableColumnsMetadata.forTable(tableName, tableMetadata.getColumns());
                    }
                    return TableColumnsMetadata.forRedirectedTable(tableName);
                })
                .iterator();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }

        JsonPlaceholderTable table = exampleClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((JsonPlaceholderColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint constraint)
    {
        JsonPlaceholderTableHandle table = (JsonPlaceholderTableHandle) handle;

        if (!table.getTableName().equals("comments")) {
            return Optional.empty();
        }

        Map<String, ColumnHandle> columns = getColumnHandles(session, handle);

        log.warning(format("applyFilter %s", constraint));
        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isAll() || summary.getDomains().isEmpty()) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> currentConstraint = table.getConstraint();
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

            // Check if this constraint is already applied
            if (currentConstraint.getDomains().isPresent() &&
                    currentConstraint.getDomains().get().containsKey(columnHandle)) {
                Domain currentDomain = currentConstraint.getDomains().get().get(columnHandle);
                if (currentDomain.equals(domain)) {
                    throw new AssertionError("Constraint was already applied");
                }
            }

            currentConstraint = currentConstraint.intersect(newConstraint);

            found = true;

            // Remove from remaining constraints
            summary = summary.filter((ch, d) -> !ch.equals(columnHandle));
        }

        if (!found) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                    new JsonPlaceholderTableHandle(
                        table.getSchemaName(),
                        table.getTableName(),
                        currentConstraint),
                    summary,
                    constraint.getExpression(),
                    false));
    }
}
