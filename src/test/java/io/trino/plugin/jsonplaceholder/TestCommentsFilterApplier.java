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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jsonplaceholder.filter.CommentsFilterApplier;
import io.trino.plugin.jsonplaceholder.filter.FilterApplier;
import io.trino.plugin.jsonplaceholder.filter.FilterType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCommentsFilterApplier
{
    private final FilterApplier filterApplier = new CommentsFilterApplier();

    @Test
    public void testGetSupportedFilters()
    {
        Map<String, FilterType> supportedFilters = filterApplier.getSupportedFilters();
        assertThat(supportedFilters).containsEntry("postid", FilterType.EQUAL);
        assertThat(supportedFilters).hasSize(1);
    }

    @Test
    public void testApplyFilterWithPostId()
    {
        JsonPlaceholderColumnHandle postIdColumn = new JsonPlaceholderColumnHandle("postid", BIGINT);
        Map<String, ColumnHandle> columns = ImmutableMap.of("postid", postIdColumn);

        JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "comments", TupleDomain.all());

        // Create constraint with postid = 1
        Domain domain = Domain.singleValue(BIGINT, 1L);
        TupleDomain<ColumnHandle> summary = TupleDomain.withColumnDomains(
                ImmutableMap.of(postIdColumn, domain));
        Constraint constraint = new Constraint(summary);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                filterApplier.applyFilter(tableHandle, columns, constraint);

        assertThat(result).isPresent();
        JsonPlaceholderTableHandle newHandle = (JsonPlaceholderTableHandle) result.get().getHandle();
        assertThat(newHandle.getConstraint().getDomains()).isPresent();
        assertThat(newHandle.getConstraint().getDomains().get()).containsKey(postIdColumn);

        // Verify the filter value can be extracted
        Object filterValue = filterApplier.getFilter(postIdColumn, newHandle.getConstraint());
        assertThat(filterValue).isEqualTo(1L);
    }

    @Test
    public void testApplyFilterWithoutPostId()
    {
        JsonPlaceholderColumnHandle postIdColumn = new JsonPlaceholderColumnHandle("postid", BIGINT);
        Map<String, ColumnHandle> columns = ImmutableMap.of("postid", postIdColumn);

        JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("default", "comments", TupleDomain.all());

        // Create constraint without any filters
        Constraint constraint = new Constraint(TupleDomain.all());

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                filterApplier.applyFilter(tableHandle, columns, constraint);

        assertThat(result).isEmpty();
    }

    @Test
    public void testGetFilterReturnsNull()
    {
        JsonPlaceholderColumnHandle postIdColumn = new JsonPlaceholderColumnHandle("postid", BIGINT);
        TupleDomain<ColumnHandle> emptyConstraint = TupleDomain.all();

        Object filterValue = filterApplier.getFilter(postIdColumn, emptyConstraint);
        assertThat(filterValue).isNull();
    }

    @Test
    public void testApplyFilterMultipleDistinctSingularRanges()
    {
        JsonPlaceholderColumnHandle postIdColumn = new JsonPlaceholderColumnHandle("postid", BIGINT);
        Map<String, ColumnHandle> columns = ImmutableMap.of("postid", postIdColumn);

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
                filterApplier.applyFilter(tableHandle, columns, constraint);

        assertThat(result).isPresent();
        JsonPlaceholderTableHandle newHandle = (JsonPlaceholderTableHandle) result.get().getHandle();
        assertThat(newHandle.getConstraint().getDomains()).isPresent();
        assertThat(newHandle.getConstraint().getDomains().get()).containsKey(postIdColumn);

        Object filterValue = filterApplier.getFilterAll(postIdColumn, newHandle.getConstraint());
        assertThat(filterValue).isEqualTo(List.of(1L, 3L));
    }

    @Test
    public void testApplyFilterSingleBounded()
    {
        JsonPlaceholderColumnHandle postIdColumn = new JsonPlaceholderColumnHandle("postid", BIGINT);
        Map<String, ColumnHandle> columns = ImmutableMap.of("postid", postIdColumn);

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
                filterApplier.applyFilter(tableHandle, columns, constraint);

        assertThat(result).isPresent();
        JsonPlaceholderTableHandle newHandle = (JsonPlaceholderTableHandle) result.get().getHandle();
        assertThat(newHandle.getConstraint().getDomains()).isPresent();
        assertThat(newHandle.getConstraint().getDomains().get()).containsKey(postIdColumn);

        Object filterValue = filterApplier.getFilterAll(postIdColumn, newHandle.getConstraint());
        assertThat(filterValue).isEqualTo(List.of(1L, 2L));
    }
}
