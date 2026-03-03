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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCommentsTableDef
{
    private static final URI BASE_URI = URI.create("https://api.example.local");
    private final CommentsTableDef commentsTable = new CommentsTableDef(BASE_URI);

    @Test
    public void testGetName()
    {
        assertThat(commentsTable.getName()).isEqualTo("comments");
    }

    @Test
    public void testGetColumns()
    {
        assertThat(commentsTable.getColumns()).isEqualTo(ImmutableList.of(
                new JsonPlaceholderColumn("postid", BIGINT),
                new JsonPlaceholderColumn("id", BIGINT),
                new JsonPlaceholderColumn("name", createUnboundedVarcharType()),
                new JsonPlaceholderColumn("email", createUnboundedVarcharType()),
                new JsonPlaceholderColumn("body", createUnboundedVarcharType())));
    }

    @Test
    public void testGetColumnsMetadata()
    {
        assertThat(commentsTable.getColumnsMetadata()).isEqualTo(ImmutableList.of(
                new ColumnMetadata("postid", BIGINT),
                new ColumnMetadata("id", BIGINT),
                new ColumnMetadata("name", createUnboundedVarcharType()),
                new ColumnMetadata("email", createUnboundedVarcharType()),
                new ColumnMetadata("body", createUnboundedVarcharType())));
    }

    @Test
    public void testAsMetadata()
    {
        ConnectorTableMetadata metadata = commentsTable.asMetadata("default");
        assertThat(metadata.getTable()).isEqualTo(new SchemaTableName("default", "comments"));
        assertThat(metadata.getColumns()).isEqualTo(commentsTable.getColumnsMetadata());
    }

    @Test
    public void testGetPageWithConstraint()
    {
        JsonPlaceholderMetadata metadata = new JsonPlaceholderMetadata(
                new JsonPlaceholderClient(new JsonPlaceholderConfig().setApiBaseUri(BASE_URI)));

        JsonPlaceholderColumnHandle postidColumn = new JsonPlaceholderColumnHandle("postid", BIGINT);
        Domain domain = Domain.singleValue(BIGINT, 1L);
        TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(
                ImmutableMap.of(postidColumn, domain));

        List<ConnectorSplit> splits = commentsTable.getPage(
                SESSION,
                metadata,
                ImmutableMap.of("postid", postidColumn),
                constraint);

        assertThat(splits).hasSize(1);
        JsonPlaceholderSplit split = (JsonPlaceholderSplit) splits.get(0);
        assertThat(split.getUri()).isEqualTo(BASE_URI.resolve("/posts/1/comments"));
    }

    @Test
    public void testGetPageWithMultipleValues()
    {
        JsonPlaceholderMetadata metadata = new JsonPlaceholderMetadata(
                new JsonPlaceholderClient(new JsonPlaceholderConfig().setApiBaseUri(BASE_URI)));

        JsonPlaceholderColumnHandle postidColumn = new JsonPlaceholderColumnHandle("postid", BIGINT);
        Domain domain = Domain.create(
                ValueSet.of(BIGINT, 1L, 2L, 3L),
                false);
        TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(
                ImmutableMap.of(postidColumn, domain));

        List<ConnectorSplit> splits = commentsTable.getPage(
                SESSION,
                metadata,
                ImmutableMap.of("postid", postidColumn),
                constraint);

        assertThat(splits).hasSize(3);
        assertThat(splits.stream().map(s -> ((JsonPlaceholderSplit) s).getUri()))
                .containsExactlyInAnyOrder(
                        BASE_URI.resolve("/posts/1/comments"),
                        BASE_URI.resolve("/posts/2/comments"),
                        BASE_URI.resolve("/posts/3/comments"));
    }

    @Test
    public void testGetPageWithoutConstraintThrows()
    {
        JsonPlaceholderMetadata metadata = new JsonPlaceholderMetadata(
                new JsonPlaceholderClient(new JsonPlaceholderConfig().setApiBaseUri(BASE_URI)));

        assertThatThrownBy(() -> commentsTable.getPage(
                SESSION,
                metadata,
                ImmutableMap.of(),
                TupleDomain.all()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Missing required filter: postid");
    }

    @Test
    public void testApplyFilterWithPostidConstraint()
    {
        JsonPlaceholderMetadata metadata = new JsonPlaceholderMetadata(
                new JsonPlaceholderClient(new JsonPlaceholderConfig().setApiBaseUri(BASE_URI)));
        JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "comments", TupleDomain.all());

        JsonPlaceholderColumnHandle postidColumn = new JsonPlaceholderColumnHandle("postid", BIGINT);
        Domain domain = Domain.singleValue(BIGINT, 1L);
        TupleDomain<ColumnHandle> summary = TupleDomain.withColumnDomains(
                ImmutableMap.of(postidColumn, domain));

        Map<String, ColumnHandle> columns = ImmutableMap.of("postid", postidColumn);
        Optional<ConstraintApplicationResult<io.trino.spi.connector.ConnectorTableHandle>> result =
                commentsTable.applyFilter(
                        SESSION,
                        metadata,
                        tableHandle,
                        columns,
                        new Constraint(summary));

        assertThat(result).isPresent();
        JsonPlaceholderTableHandle newHandle = (JsonPlaceholderTableHandle) result.get().getHandle();
        assertThat(newHandle.getConstraint().getDomains()).isPresent();
        assertThat(newHandle.getConstraint().getDomains().get()).containsKey(postidColumn);
    }

    @Test
    public void testApplyFilterWithoutPostidReturnsEmpty()
    {
        JsonPlaceholderMetadata metadata = new JsonPlaceholderMetadata(
                new JsonPlaceholderClient(new JsonPlaceholderConfig().setApiBaseUri(BASE_URI)));
        JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "comments", TupleDomain.all());

        Optional<ConstraintApplicationResult<io.trino.spi.connector.ConnectorTableHandle>> result =
                commentsTable.applyFilter(
                        SESSION,
                        metadata,
                        tableHandle,
                        ImmutableMap.of(),
                        new Constraint(TupleDomain.all()));

        assertThat(result).isEmpty();
    }

    @Test
    public void testApplyFilterWithNonPostidColumnReturnsEmpty()
    {
        JsonPlaceholderMetadata metadata = new JsonPlaceholderMetadata(
                new JsonPlaceholderClient(new JsonPlaceholderConfig().setApiBaseUri(BASE_URI)));
        JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "comments", TupleDomain.all());

        JsonPlaceholderColumnHandle idColumn = new JsonPlaceholderColumnHandle("id", BIGINT);
        Domain domain = Domain.singleValue(BIGINT, 1L);
        TupleDomain<ColumnHandle> summary = TupleDomain.withColumnDomains(
                ImmutableMap.of(idColumn, domain));

        Map<String, ColumnHandle> columns = ImmutableMap.of("id", idColumn);
        Optional<ConstraintApplicationResult<io.trino.spi.connector.ConnectorTableHandle>> result =
                commentsTable.applyFilter(
                        SESSION,
                        metadata,
                        tableHandle,
                        columns,
                        new Constraint(summary));

        assertThat(result).isEmpty();
    }
}
