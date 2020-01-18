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
package io.prestosql.plugin.arrow.writer;

import io.prestosql.plugin.arrow.ArrowColumnHandle;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;

public class ColumnWriters
{
    private ColumnWriters()
    {
    }

    public static ColumnWriter getColumnWriter(ArrowColumnHandle arrowColumnHandle, BufferAllocator bufferAllocator)
    {
        if (arrowColumnHandle.getColumnType().equals(BIGINT)) {
            return new LongColumnWriter(
                    arrowColumnHandle.getColumnType(),
                    new BigIntVector(arrowColumnHandle.getColumnName(), bufferAllocator));
        }
        if (arrowColumnHandle.getColumnType().equals(INTEGER)) {
            return new IntegerColumnWriter(
                    arrowColumnHandle.getColumnType(),
                    new IntVector(arrowColumnHandle.getColumnName(), bufferAllocator));
        }
        throw new UnsupportedOperationException("Not yet supported for type " + arrowColumnHandle.getColumnType());
    }
}
