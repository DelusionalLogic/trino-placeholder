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

package io.trino.plugin.jsonplaceholder.filter;

import io.trino.plugin.jsonplaceholder.JsonPlaceholderColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.Collection;
import java.util.Map;
import java.util.logging.Logger;

import static java.util.Objects.requireNonNull;

public interface FilterApplier
{
    Logger log = Logger.getLogger(FilterApplier.class.getName());

    Map<String, FilterType> getSupportedFilters();

    default Collection<Object> getFilterAll(JsonPlaceholderColumnHandle column, TupleDomain<ColumnHandle> constraint)
    {
        requireNonNull(column, "column is null");
        requireNonNull(constraint, "constraint is null");

        if (!constraint.getDomains().isPresent()) {
            return null;
        }

        Domain domain = constraint.getDomains().get().get(column);
        if (domain == null) {
            return null;
        }

        return domain.getValues().tryExpandRanges(1024).get();
    }

    default Object getFilter(JsonPlaceholderColumnHandle column, TupleDomain<ColumnHandle> constraint)
    {
        var values = getFilterAll(column, constraint);
        if (values == null) {
            return null;
        }

        if (values.size() != 1) {
            throw new AssertionError("Not a single value");
        }

        return values.stream().findFirst().get();
    }
}
