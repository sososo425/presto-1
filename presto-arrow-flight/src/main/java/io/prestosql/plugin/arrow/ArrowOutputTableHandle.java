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
package io.prestosql.plugin.arrow;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ArrowOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final ArrowTableHandle tableHandle;
    private final List<ArrowColumnHandle> columnHandles;

    @JsonCreator
    public ArrowOutputTableHandle(
            @JsonProperty("tableHandle") ArrowTableHandle tableHandle,
            @JsonProperty("columnHandles") List<ArrowColumnHandle> columnHandles)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
    }

    @JsonProperty
    public ArrowTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public List<ArrowColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }
}
