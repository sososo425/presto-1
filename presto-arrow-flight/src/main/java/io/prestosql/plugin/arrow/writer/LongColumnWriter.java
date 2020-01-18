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

import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;

import static java.util.Objects.requireNonNull;

public class LongColumnWriter
        implements ColumnWriter
{
    private final Type type;
    private final BigIntVector bigIntVector;

    public LongColumnWriter(Type type, BigIntVector bigIntVector)
    {
        this.type = requireNonNull(type, "type is null");
        this.bigIntVector = requireNonNull(bigIntVector, "bigIntVector is null");
    }

    @Override
    public void writeBlock(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                bigIntVector.setNull(i);
            }
            else {
                bigIntVector.setSafe(i, type.getLong(block, i));
            }
        }
    }

    @Override
    public FieldVector getValueVector()
    {
        return bigIntVector;
    }
}
