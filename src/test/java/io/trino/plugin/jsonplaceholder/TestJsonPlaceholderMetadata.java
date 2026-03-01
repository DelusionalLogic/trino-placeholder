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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestJsonPlaceholderMetadata
{
    private static final JsonPlaceholderTableHandle POSTS_TABLE_HANDLE = new JsonPlaceholderTableHandle("default", "posts", TupleDomain.all());
    private static final JsonPlaceholderTableHandle COMMENTS_TABLE_HANDLE = new JsonPlaceholderTableHandle("default", "comments", TupleDomain.all());
    private JsonPlaceholderMetadata metadata;

    @BeforeEach
    public void setUp()
            throws Exception
    {
        URI apiBaseUri = URI.create("https://jsonplaceholder.typicode.com");
        JsonPlaceholderClient client = new JsonPlaceholderClient(new JsonPlaceholderConfig().setApiBaseUri(apiBaseUri));
        metadata = new JsonPlaceholderMetadata(client);
    }

    @Test
    public void testListSchemaNames()
    {
        assertThat(metadata.listSchemaNames(SESSION)).containsExactlyElementsOf(ImmutableSet.of("default"));
    }

    @Test
    public void testGetTableHandle()
    {
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("default", "posts"), Optional.empty(), Optional.empty())).isEqualTo(POSTS_TABLE_HANDLE);
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("default", "unknown"), Optional.empty(), Optional.empty())).isNull();
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "posts"), Optional.empty(), Optional.empty())).isNull();
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown"), Optional.empty(), Optional.empty())).isNull();
    }

    @Test
    public void testGetColumnHandles()
    {
        // known table
        assertThat(metadata.getColumnHandles(SESSION, POSTS_TABLE_HANDLE)).isEqualTo(ImmutableMap.of(
                "userid", new JsonPlaceholderColumnHandle("userid", BIGINT),
                "id", new JsonPlaceholderColumnHandle("id", BIGINT),
                "title", new JsonPlaceholderColumnHandle("title", createUnboundedVarcharType()),
                "body", new JsonPlaceholderColumnHandle("body", createUnboundedVarcharType())));

        // unknown table
        assertThatThrownBy(() -> metadata.getColumnHandles(SESSION, new JsonPlaceholderTableHandle("unknown", "unknown", TupleDomain.all())))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessage("Table 'unknown.unknown' not found");
        assertThatThrownBy(() -> metadata.getColumnHandles(SESSION, new JsonPlaceholderTableHandle("default", "unknown", TupleDomain.all())))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessage("Table 'default.unknown' not found");
    }

    @Test
    public void getTableMetadata()
    {
        // known table
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, POSTS_TABLE_HANDLE);
        assertThat(tableMetadata.getTable()).isEqualTo(new SchemaTableName("default", "posts"));
        assertThat(tableMetadata.getColumns()).isEqualTo(ImmutableList.of(
                new ColumnMetadata("userid", BIGINT),
                new ColumnMetadata("id", BIGINT),
                new ColumnMetadata("title", createUnboundedVarcharType()),
                new ColumnMetadata("body", createUnboundedVarcharType())));

        // unknown tables should produce null
        assertThat(metadata.getTableMetadata(SESSION, new JsonPlaceholderTableHandle("unknown", "unknown", TupleDomain.all()))).isNull();
        assertThat(metadata.getTableMetadata(SESSION, new JsonPlaceholderTableHandle("default", "unknown", TupleDomain.all()))).isNull();
        assertThat(metadata.getTableMetadata(SESSION, new JsonPlaceholderTableHandle("unknown", "posts", TupleDomain.all()))).isNull();
    }

    @Test
    public void testListTables()
    {
        // all schemas
        assertThat(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.empty()))).isEqualTo(ImmutableSet.of(
                new SchemaTableName("default", "posts"),
                new SchemaTableName("default", "comments")));

        // specific schema
        assertThat(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("default")))).isEqualTo(ImmutableSet.of(
                new SchemaTableName("default", "posts"),
                new SchemaTableName("default", "comments")));

        // unknown schema
        assertThat(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("unknown")))).isEqualTo(ImmutableSet.of());
    }

    @Test
    public void getColumnMetadata()
    {
        assertThat(metadata.getColumnMetadata(SESSION, POSTS_TABLE_HANDLE, new JsonPlaceholderColumnHandle("userid", BIGINT))).isEqualTo(new ColumnMetadata("userid", BIGINT));
        assertThat(metadata.getColumnMetadata(SESSION, POSTS_TABLE_HANDLE, new JsonPlaceholderColumnHandle("title", createUnboundedVarcharType()))).isEqualTo(new ColumnMetadata("title", createUnboundedVarcharType()));

        // example connector assumes that the table handle and column handle are
        // properly formed, so it will return a metadata object for any
        // JsonPlaceholderTableHandle and JsonPlaceholderColumnHandle passed in.  This is because
        // it is not possible for the Trino Metadata system to create the handles
        // directly.
    }

    @Test
    public void testCreateTable()
    {
        assertThatThrownBy(() -> metadata.createTable(
                SESSION,
                new ConnectorTableMetadata(
                        new SchemaTableName("default", "foo"),
                        ImmutableList.of(new ColumnMetadata("text", createUnboundedVarcharType()))),
                SaveMode.FAIL))
                .isInstanceOf(TrinoException.class)
                .hasMessage("This connector does not support creating tables");
    }

    @Test
    public void testDropTableTable()
    {
        assertThatThrownBy(() -> metadata.dropTable(SESSION, POSTS_TABLE_HANDLE))
                .isInstanceOf(TrinoException.class);
    }

    @Test
    public void testGetTableHandleForComments()
    {
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("default", "comments"), Optional.empty(), Optional.empty())).isEqualTo(COMMENTS_TABLE_HANDLE);
    }

    @Test
    public void testGetColumnHandlesForComments()
    {
        assertThat(metadata.getColumnHandles(SESSION, COMMENTS_TABLE_HANDLE)).isEqualTo(ImmutableMap.of(
                "postid", new JsonPlaceholderColumnHandle("postid", BIGINT),
                "id", new JsonPlaceholderColumnHandle("id", BIGINT),
                "name", new JsonPlaceholderColumnHandle("name", createUnboundedVarcharType()),
                "email", new JsonPlaceholderColumnHandle("email", createUnboundedVarcharType()),
                "body", new JsonPlaceholderColumnHandle("body", createUnboundedVarcharType())));
    }

    @Test
    public void testApplyFilterForCommentsTable()
    {
        JsonPlaceholderColumnHandle postIdColumn = new JsonPlaceholderColumnHandle("postid", BIGINT);
        Domain domain = Domain.singleValue(BIGINT, 1L);
        TupleDomain<io.trino.spi.connector.ColumnHandle> summary = TupleDomain.withColumnDomains(
                ImmutableMap.of(postIdColumn, domain));
        io.trino.spi.connector.Constraint constraint = new io.trino.spi.connector.Constraint(summary);

        Optional<io.trino.spi.connector.ConstraintApplicationResult<io.trino.spi.connector.ConnectorTableHandle>> result =
                metadata.applyFilter(SESSION, COMMENTS_TABLE_HANDLE, constraint);

        assertThat(result).isPresent();
        JsonPlaceholderTableHandle newHandle = (JsonPlaceholderTableHandle) result.get().getHandle();
        assertThat(newHandle.getConstraint().getDomains()).isPresent();
        assertThat(newHandle.getConstraint().getDomains().get()).containsKey(postIdColumn);
    }

    @Test
    public void testApplyFilterWithPostId()
    {
        JsonPlaceholderColumnHandle postIdColumn = new JsonPlaceholderColumnHandle("postid", BIGINT);

        JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "comments", TupleDomain.all());

        // Create constraint with postid = 1
        Domain domain = Domain.singleValue(BIGINT, 1L);
        TupleDomain<ColumnHandle> summary = TupleDomain.withColumnDomains(
                ImmutableMap.of(postIdColumn, domain));
        Constraint constraint = new Constraint(summary);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(SESSION, tableHandle, constraint);

        assertThat(result).isPresent();
        JsonPlaceholderTableHandle newHandle = (JsonPlaceholderTableHandle) result.get().getHandle();
        assertThat(newHandle.getConstraint().getDomains()).isPresent();
        assertThat(newHandle.getConstraint().getDomains().get()).containsKey(postIdColumn);
    }

    @Test
    public void testApplyFilterWithoutPostId()
    {
        JsonPlaceholderColumnHandle postIdColumn = new JsonPlaceholderColumnHandle("postid", BIGINT);

        JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "comments", TupleDomain.all());

        // Create constraint without any filters
        Constraint constraint = new Constraint(TupleDomain.all());

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(SESSION, tableHandle, constraint);

        assertThat(result).isEmpty();
    }

    @Test
    public void testApplyFilterMultipleDistinctSingularRanges()
    {
        JsonPlaceholderColumnHandle postIdColumn = new JsonPlaceholderColumnHandle("postid", BIGINT);

        JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "comments", TupleDomain.all());

        Domain domain = Domain.create(
                ValueSet.ofRanges(
                        io.trino.spi.predicate.Range.equal(BIGINT, 1L),
                        io.trino.spi.predicate.Range.equal(BIGINT, 3L)),
                false);
        TupleDomain<ColumnHandle> summary = TupleDomain.withColumnDomains(
                ImmutableMap.of(postIdColumn, domain));
        Constraint constraint = new Constraint(summary);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(SESSION, tableHandle, constraint);

        assertThat(result).isPresent();
        JsonPlaceholderTableHandle newHandle = (JsonPlaceholderTableHandle) result.get().getHandle();
        assertThat(newHandle.getConstraint().getDomains()).isPresent();
        assertThat(newHandle.getConstraint().getDomains().get()).containsKey(postIdColumn);
    }

    @Test
    public void testApplyFilterSingleBounded()
    {
        JsonPlaceholderColumnHandle postIdColumn = new JsonPlaceholderColumnHandle("postid", BIGINT);

        JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "comments", TupleDomain.all());

        // Trino prefers this form of the expression over disjoin values
        Domain domain = Domain.create(
                ValueSet.ofRanges(
                        io.trino.spi.predicate.Range.range(BIGINT, 1L, true, 2L, true)),
                false);
        TupleDomain<ColumnHandle> summary = TupleDomain.withColumnDomains(
                ImmutableMap.of(postIdColumn, domain));
        Constraint constraint = new Constraint(summary);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(SESSION, tableHandle, constraint);

        assertThat(result).isPresent();
        JsonPlaceholderTableHandle newHandle = (JsonPlaceholderTableHandle) result.get().getHandle();
        assertThat(newHandle.getConstraint().getDomains()).isPresent();
        assertThat(newHandle.getConstraint().getDomains().get()).containsKey(postIdColumn);
    }
}
