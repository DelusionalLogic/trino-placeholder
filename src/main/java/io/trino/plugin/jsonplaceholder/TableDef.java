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

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface TableDef
{
    String getName();

    List<JsonPlaceholderColumn> getColumns();

    List<ColumnMetadata> getColumnsMetadata();

    ConnectorTableMetadata asMetadata(String schema);

    List<ConnectorSplit> getPage(ConnectorSession session, JsonPlaceholderMetadata meta, Map<String, ColumnHandle> columns, TupleDomain<ColumnHandle> tableConstraint);

    Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            JsonPlaceholderMetadata meta,
            JsonPlaceholderTableHandle tableHandle,
            Map<String, ColumnHandle> columns,
            Constraint constraint);
}
