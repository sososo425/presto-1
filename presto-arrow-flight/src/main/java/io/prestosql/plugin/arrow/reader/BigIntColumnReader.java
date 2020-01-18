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
import org.apache.arrow.vector.BigIntVector;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class BigIntColumnReader
        implements ColumnReader
{
    private final BigIntVector bigIntVector;
    private int offset;

    public BigIntColumnReader(BigIntVector bigIntVector)
    {
        this.bigIntVector = requireNonNull(bigIntVector, "bigIntVector is null");
    }

    @Override
    public Block readBlock(int batchSize)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, batchSize);
        for (int i = 0; i < batchSize; i++) {
            if (bigIntVector.isNull(offset + i)) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeLong(bigIntVector.get(offset + i));
            }
        }
        offset += batchSize;
        return blockBuilder.build();
    }
}
