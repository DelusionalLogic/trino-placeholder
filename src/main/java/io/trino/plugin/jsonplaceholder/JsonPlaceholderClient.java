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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.net.URI;
import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class JsonPlaceholderClient
{
    private static final String SCHEMA_NAME = "default";
    private static final String TABLE_NAME = "posts";

    private final JsonPlaceholderTable postsTable;

    @Inject
    public JsonPlaceholderClient(JsonPlaceholderConfig config)
    {
        requireNonNull(config, "config is null");
        URI apiBaseUri = config.getApiBaseUri();
        requireNonNull(apiBaseUri, "apiBaseUri is null");

        // Hardcode the posts table structure
        this.postsTable = new JsonPlaceholderTable(
                TABLE_NAME,
                ImmutableList.of(
                        new JsonPlaceholderColumn("userid", BIGINT),
                        new JsonPlaceholderColumn("id", BIGINT),
                        new JsonPlaceholderColumn("title", createUnboundedVarcharType()),
                        new JsonPlaceholderColumn("body", createUnboundedVarcharType())),
                ImmutableList.of(apiBaseUri.resolve("/posts")));
    }

    public Set<String> getSchemaNames()
    {
        return ImmutableSet.of(SCHEMA_NAME);
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        if (!SCHEMA_NAME.equals(schema)) {
            return ImmutableSet.of();
        }
        return ImmutableSet.of(TABLE_NAME);
    }

    public JsonPlaceholderTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        if (SCHEMA_NAME.equals(schema) && TABLE_NAME.equals(tableName)) {
            return postsTable;
        }
        return null;
    }
}
