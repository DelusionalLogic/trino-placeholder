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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class PostsTableDef
        implements TableDef
{
    private static final String TABLE_NAME = "posts";
    private static final List<JsonPlaceholderColumn> COLUMNS = ImmutableList.of(
            new JsonPlaceholderColumn("userid", BIGINT),
            new JsonPlaceholderColumn("id", BIGINT),
            new JsonPlaceholderColumn("title", createUnboundedVarcharType()),
            new JsonPlaceholderColumn("body", createUnboundedVarcharType()));

    private final URI baseUri;

    public PostsTableDef(URI baseUri)
    {
        this.baseUri = requireNonNull(baseUri, "baseUri is null");
    }

    @Override
    public String getName()
    {
        return TABLE_NAME;
    }

    @Override
    public List<JsonPlaceholderColumn> getColumns()
    {
        return COLUMNS;
    }

    @Override
    public List<ColumnMetadata> getColumnsMetadata()
    {
        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (JsonPlaceholderColumn column : COLUMNS) {
            columnsMetadata.add(column.asMetadata());
        }
        return columnsMetadata.build();
    }

    @Override
    public ConnectorTableMetadata asMetadata(String schema)
    {
        return new ConnectorTableMetadata(new SchemaTableName(schema, TABLE_NAME), getColumnsMetadata());
    }

    @Override
    public List<ConnectorSplit> getPage(ConnectorSession session, JsonPlaceholderMetadata meta, Map<String, ColumnHandle> columns, TupleDomain<ColumnHandle> tableConstraint)
    {
        List<ConnectorSplit> splits = new ArrayList<>();
        splits.add(new JsonPlaceholderSplit(baseUri.resolve("/posts")));
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
        return Optional.empty();
    }
}
