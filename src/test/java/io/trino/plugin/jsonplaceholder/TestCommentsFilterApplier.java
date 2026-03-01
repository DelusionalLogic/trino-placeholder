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

import io.trino.plugin.jsonplaceholder.filter.CommentsFilterApplier;
import io.trino.plugin.jsonplaceholder.filter.FilterApplier;
import io.trino.plugin.jsonplaceholder.filter.FilterType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.Map;

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
    public void testGetFilterReturnsNull()
    {
        JsonPlaceholderColumnHandle postIdColumn = new JsonPlaceholderColumnHandle("postid", BIGINT);
        TupleDomain<ColumnHandle> emptyConstraint = TupleDomain.all();

        Object filterValue = filterApplier.getFilter(postIdColumn, emptyConstraint);
        assertThat(filterValue).isNull();
    }
}
