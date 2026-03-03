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
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public abstract class AbstractJsonPlaceholderTable
        implements TableDef
{
    protected final String name;
    protected final List<JsonPlaceholderColumn> columns;
    protected final URI baseUri;

    public AbstractJsonPlaceholderTable(
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
}
