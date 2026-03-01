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

import io.airlift.testing.EquivalenceTester;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.jsonplaceholder.MetadataUtil.COLUMN_CODEC;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJsonPlaceholderColumnHandle
{
    private final JsonPlaceholderColumnHandle columnHandle = new JsonPlaceholderColumnHandle("columnName", createUnboundedVarcharType());

    @Test
    public void testJsonRoundTrip()
    {
        String json = COLUMN_CODEC.toJson(columnHandle);
        JsonPlaceholderColumnHandle copy = COLUMN_CODEC.fromJson(json);
        assertThat(copy).isEqualTo(columnHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new JsonPlaceholderColumnHandle("columnName", createUnboundedVarcharType()),
                        new JsonPlaceholderColumnHandle("columnName", BIGINT),
                        new JsonPlaceholderColumnHandle("columnName", createUnboundedVarcharType()))
                .addEquivalentGroup(
                        new JsonPlaceholderColumnHandle("columnNameX", createUnboundedVarcharType()),
                        new JsonPlaceholderColumnHandle("columnNameX", BIGINT),
                        new JsonPlaceholderColumnHandle("columnNameX", createUnboundedVarcharType()))
                .check();
    }
}
