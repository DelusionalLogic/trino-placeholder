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

import io.airlift.json.JsonCodec;
import io.airlift.testing.EquivalenceTester;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJsonPlaceholderTableHandle
{
    private final JsonPlaceholderTableHandle tableHandle = new JsonPlaceholderTableHandle("schemaName", "tableName", TupleDomain.all());

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<JsonPlaceholderTableHandle> codec = jsonCodec(JsonPlaceholderTableHandle.class);
        String json = codec.toJson(tableHandle);
        JsonPlaceholderTableHandle copy = codec.fromJson(json);
        assertThat(copy).isEqualTo(tableHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(new JsonPlaceholderTableHandle("schema", "table", TupleDomain.all()), new JsonPlaceholderTableHandle("schema", "table", TupleDomain.all()))
                .addEquivalentGroup(new JsonPlaceholderTableHandle("schemaX", "table", TupleDomain.all()), new JsonPlaceholderTableHandle("schemaX", "table", TupleDomain.all()))
                .addEquivalentGroup(new JsonPlaceholderTableHandle("schema", "tableX", TupleDomain.all()), new JsonPlaceholderTableHandle("schema", "tableX", TupleDomain.all()))
                .check();
    }
}
