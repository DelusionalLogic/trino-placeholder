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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.spi.connector.SchemaTableName;

import java.net.URI;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class JsonPlaceholderClient
{
    static final String SCHEMA_NAME = "default";
    private static final String POSTS_TABLE_NAME = "posts";
    private static final String COMMENTS_TABLE_NAME = "comments";

    private final TableDef postsTable;
    private final TableDef commentsTable;

    @Inject
    public JsonPlaceholderClient(JsonPlaceholderConfig config)
    {
        requireNonNull(config, "config is null");
        URI apiBaseUri = config.getApiBaseUri();
        requireNonNull(apiBaseUri, "apiBaseUri is null");

        this.postsTable = new PostsTableDef(apiBaseUri);
        this.commentsTable = new CommentsTableDef(apiBaseUri);
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
        return ImmutableSet.of(POSTS_TABLE_NAME, COMMENTS_TABLE_NAME);
    }

    public TableDef getTable(JsonPlaceholderTableHandle handle)
    {
        return getTable(handle.getSchemaName(), handle.getTableName());
    }

    public TableDef getTable(SchemaTableName name)
    {
        return getTable(name.getSchemaName(), name.getTableName());
    }

    public TableDef getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        if (!SCHEMA_NAME.equals(schema)) {
            return null;
        }
        if (POSTS_TABLE_NAME.equals(tableName)) {
            return postsTable;
        }
        if (COMMENTS_TABLE_NAME.equals(tableName)) {
            return commentsTable;
        }
        return null;
    }
}
