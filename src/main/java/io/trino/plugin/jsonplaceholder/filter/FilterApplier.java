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
import io.trino.plugin.jsonplaceholder.JsonPlaceholderTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public interface FilterApplier
{
    Logger log = Logger.getLogger(FilterApplier.class.getName());

    Map<String, FilterType> getSupportedFilters();

    default Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            JsonPlaceholderTableHandle table,
            Map<String, ColumnHandle> columns,
            Constraint constraint)
    {
        log.warning(format("applyFilter %s", constraint));
        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isAll() || summary.getDomains().isEmpty()) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> currentConstraint = table.getConstraint();
        Map<String, FilterType> supportedFilters = getSupportedFilters();

        boolean found = false;
        for (Map.Entry<String, FilterType> entry : supportedFilters.entrySet()) {
            String columnName = entry.getKey();

            ColumnHandle columnHandle = columns.get(columnName);
            if (columnHandle == null) {
                continue;
            }

            if (!summary.getDomains().isPresent()) {
                continue;
            }

            Domain domain = summary.getDomains().get().get(columnHandle);
            if (domain == null) {
                continue;
            }

            // Create constraint for this column
            TupleDomain<ColumnHandle> newConstraint = TupleDomain.withColumnDomains(
                    Map.of(columnHandle, domain));

            // Check if this constraint is already applied
            if (currentConstraint.getDomains().isPresent() &&
                    currentConstraint.getDomains().get().containsKey(columnHandle)) {
                Domain currentDomain = currentConstraint.getDomains().get().get(columnHandle);
                if (currentDomain.equals(domain)) {
                    throw new AssertionError("Constraint was already applied");
                }
            }

            currentConstraint = currentConstraint.intersect(newConstraint);

            found = true;

            // Remove from remaining constraints
            summary = summary.filter((ch, d) -> !ch.equals(columnHandle));
        }

        if (!found) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                    new JsonPlaceholderTableHandle(
                        table.getSchemaName(),
                        table.getTableName(),
                        currentConstraint),
                    summary,
                    constraint.getExpression(),
                    false));
    }

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
