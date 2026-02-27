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

import io.airlift.slice.Slice;
import io.trino.plugin.jsonplaceholder.JsonPlaceholderColumnHandle;
import io.trino.plugin.jsonplaceholder.JsonPlaceholderTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface FilterApplier
{
    Map<String, FilterType> getSupportedFilters();

    default Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            JsonPlaceholderTableHandle table,
            Map<String, ColumnHandle> columns,
            Constraint constraint)
    {
        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isAll() || summary.getDomains().isEmpty()) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> currentConstraint = table.getConstraint();
        Map<String, FilterType> supportedFilters = getSupportedFilters();

        boolean found = false;
        for (Map.Entry<String, FilterType> entry : supportedFilters.entrySet()) {
            String columnName = entry.getKey();
            FilterType filterType = entry.getValue();

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

            // Only support EQUAL filter with single discrete values
            if (filterType == FilterType.EQUAL) {
                if (!domain.isSingleValue()) {
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
                        continue;
                    }
                }

                // Merge with existing constraints
                if (currentConstraint.isAll()) {
                    currentConstraint = newConstraint;
                }
                else {
                    currentConstraint = currentConstraint.intersect(newConstraint);
                }

                found = true;

                // Remove from remaining constraints
                summary = summary.filter((ch, d) -> !ch.equals(columnHandle));
            }
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

    default Object getFilter(JsonPlaceholderColumnHandle column, TupleDomain<ColumnHandle> constraint)
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

        if (!domain.isSingleValue()) {
            return null;
        }

        Object value = domain.getSingleValue();
        if (value instanceof Slice) {
            return ((Slice) value).toStringUtf8();
        }

        return value;
    }
}
