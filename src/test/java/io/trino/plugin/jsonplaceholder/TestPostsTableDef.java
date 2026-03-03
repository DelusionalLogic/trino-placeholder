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
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPostsTableDef
{
    private static final URI BASE_URI = URI.create("https://api.example.local");
    private final PostsTableDef postsTable = new PostsTableDef(
            "posts",
            ImmutableList.of(
                    new JsonPlaceholderColumn("userid", BIGINT),
                    new JsonPlaceholderColumn("id", BIGINT),
                    new JsonPlaceholderColumn("title", createUnboundedVarcharType()),
                    new JsonPlaceholderColumn("body", createUnboundedVarcharType())),
            BASE_URI);

    @Test
    public void testGetName()
    {
        assertThat(postsTable.getName()).isEqualTo("posts");
    }

    @Test
    public void testGetColumns()
    {
        assertThat(postsTable.getColumns()).isEqualTo(ImmutableList.of(
                new JsonPlaceholderColumn("userid", BIGINT),
                new JsonPlaceholderColumn("id", BIGINT),
                new JsonPlaceholderColumn("title", createUnboundedVarcharType()),
                new JsonPlaceholderColumn("body", createUnboundedVarcharType())));
    }

    @Test
    public void testGetColumnsMetadata()
    {
        assertThat(postsTable.getColumnsMetadata()).isEqualTo(ImmutableList.of(
                new ColumnMetadata("userid", BIGINT),
                new ColumnMetadata("id", BIGINT),
                new ColumnMetadata("title", createUnboundedVarcharType()),
                new ColumnMetadata("body", createUnboundedVarcharType())));
    }

    @Test
    public void testAsMetadata()
    {
        ConnectorTableMetadata metadata = postsTable.asMetadata("default");
        assertThat(metadata.getTable()).isEqualTo(new SchemaTableName("default", "posts"));
        assertThat(metadata.getColumns()).isEqualTo(postsTable.getColumnsMetadata());
    }

    @Test
    public void testGetPage()
    {
        JsonPlaceholderMetadata metadata = new JsonPlaceholderMetadata(
                new JsonPlaceholderClient(new JsonPlaceholderConfig().setApiBaseUri(BASE_URI)));

        List<ConnectorSplit> splits = postsTable.getPage(
                SESSION,
                metadata,
                ImmutableMap.of(),
                TupleDomain.all());

        assertThat(splits).hasSize(1);
        JsonPlaceholderSplit split = (JsonPlaceholderSplit) splits.get(0);
        assertThat(split.getUri()).isEqualTo(BASE_URI.resolve("/posts"));
    }

    @Test
    public void testApplyFilterReturnsEmpty()
    {
        JsonPlaceholderMetadata metadata = new JsonPlaceholderMetadata(
                new JsonPlaceholderClient(new JsonPlaceholderConfig().setApiBaseUri(BASE_URI)));
        JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "posts", TupleDomain.all());

        Optional<ConstraintApplicationResult<io.trino.spi.connector.ConnectorTableHandle>> result =
                postsTable.applyFilter(
                        SESSION,
                        metadata,
                        tableHandle,
                        ImmutableMap.of(),
                        new Constraint(TupleDomain.all()));

        assertThat(result).isEmpty();
    }
}
