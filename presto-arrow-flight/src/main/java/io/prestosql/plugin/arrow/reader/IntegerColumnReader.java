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
package io.prestosql.plugin.arrow.reader;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import org.apache.arrow.vector.IntVector;

import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;

public class IntegerColumnReader
        implements ColumnReader
{
    private final IntVector intVector;
    private int offset;

    public IntegerColumnReader(IntVector intVector)
    {
        this.intVector = requireNonNull(intVector, "intVector is null");
    }

    @Override
    public Block readBlock(int batchSize)
    {
        BlockBuilder blockBuilder = INTEGER.createBlockBuilder(null, batchSize);
        for (int i = 0; i < batchSize; i++) {
            if (intVector.isNull(offset + i)) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeInt(intVector.get(offset + i));
            }
        }
        offset += batchSize;
        return blockBuilder.build();
    }
}
