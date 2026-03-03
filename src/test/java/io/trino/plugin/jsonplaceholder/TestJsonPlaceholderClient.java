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
import org.junit.jupiter.api.Test;

import java.net.URI;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJsonPlaceholderClient
{
    @Test
    public void testMetadata()
            throws Exception
    {
        URI apiBaseUri = URI.create("https://api.example.local");
        JsonPlaceholderClient client = new JsonPlaceholderClient(new JsonPlaceholderConfig().setApiBaseUri(apiBaseUri));

        // Test schema discovery
        assertThat(client.getSchemaNames()).isEqualTo(ImmutableSet.of("default"));

        // Test table discovery in default schema
        assertThat(client.getTableNames("default")).isEqualTo(ImmutableSet.of("posts", "comments"));

        // Test table discovery in unknown schema
        assertThat(client.getTableNames("unknown")).isEqualTo(ImmutableSet.of());

        // Test posts table
        TableDef table = client.getTable("default", "posts");
        assertThat(table)
                .describedAs("table is null")
                .isNotNull();
        assertThat(table.getName()).isEqualTo("posts");
        assertThat(table.getColumns()).isEqualTo(ImmutableList.of(
                new JsonPlaceholderColumn("userid", BIGINT),
                new JsonPlaceholderColumn("id", BIGINT),
                new JsonPlaceholderColumn("title", createUnboundedVarcharType()),
                new JsonPlaceholderColumn("body", createUnboundedVarcharType())));

        // Test getting unknown table returns null
        assertThat(client.getTable("default", "unknown")).isNull();
        assertThat(client.getTable("unknown", "posts")).isNull();
    }

    @Test
    public void testCommentsTableMetadata()
            throws Exception
    {
        URI apiBaseUri = URI.create("https://api.example.local");
        JsonPlaceholderClient client = new JsonPlaceholderClient(new JsonPlaceholderConfig().setApiBaseUri(apiBaseUri));

        // Test comments table
        TableDef table = client.getTable("default", "comments");
        assertThat(table).isNotNull();
        assertThat(table.getColumns()).hasSize(5);
        assertThat(table.getColumns().stream().map(JsonPlaceholderColumn::getName))
                .containsExactly("postid", "id", "name", "email", "body");
    }
}
