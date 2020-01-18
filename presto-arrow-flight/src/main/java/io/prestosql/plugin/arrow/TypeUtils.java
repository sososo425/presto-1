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

import io.prestosql.spi.type.Type;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeVisitor;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.Duration;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Interval;
import org.apache.arrow.vector.types.pojo.ArrowType.List;
import org.apache.arrow.vector.types.pojo.ArrowType.Map;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class TypeUtils
{
    private TypeUtils()
    {}

    public static Type fromArrowType(ArrowType arrowType)
    {
        return arrowType.accept(new TypeVisitor());
    }

    public static ArrowType toArrowType(Type type)
    {
        if (type.equals(BIGINT)) {
            return new ArrowType.Int(64, true);
        }
        if (type.equals(INTEGER)) {
            return new ArrowType.Int(32, true);
        }
        return null;
    }

    private static class TypeVisitor
            implements ArrowTypeVisitor<Type>
    {
        @Override
        public Type visit(Null type)
        {
            throw new UnsupportedOperationException("Null type is not supported");
        }

        @Override
        public Type visit(Struct type)
        {
            throw new UnsupportedOperationException("Struct type is not supported");
        }

        @Override
        public Type visit(List type)
        {
            throw new UnsupportedOperationException("List type is not supported");
        }

        @Override
        public Type visit(FixedSizeList type)
        {
            throw new UnsupportedOperationException("FixedSizeList type is not supported");
        }

        @Override
        public Type visit(Union type)
        {
            throw new UnsupportedOperationException("Union type is not supported");
        }

        @Override
        public Type visit(Map type)
        {
            throw new UnsupportedOperationException("Map type is not supported");
        }

        @Override
        public Type visit(Int type)
        {
            if (type.getBitWidth() == 64) {
                return BIGINT;
            }
            if (type.getBitWidth() == 32) {
                return INTEGER;
            }
            if (type.getBitWidth() == 16) {
                return SMALLINT;
            }
            if (type.getBitWidth() == 8) {
                return TINYINT;
            }
            throw new UnsupportedOperationException(format("Int type of bit width %d is not supported", type.getBitWidth()));
        }

        @Override
        public Type visit(FloatingPoint type)
        {
            if (type.getPrecision() == FloatingPointPrecision.DOUBLE) {
                return DOUBLE;
            }
            if (type.getPrecision() == FloatingPointPrecision.HALF) {
                return REAL;
            }
            throw new UnsupportedOperationException(format("FloatingPoint type of precision %s is not supported", type.getPrecision()));
        }

        @Override
        public Type visit(Utf8 type)
        {
            return VARCHAR;
        }

        @Override
        public Type visit(Binary type)
        {
            return VARBINARY;
        }

        @Override
        public Type visit(FixedSizeBinary type)
        {
            throw new UnsupportedOperationException("FixedSizeBinary type is not supported");
        }

        @Override
        public Type visit(Bool type)
        {
            return BOOLEAN;
        }

        @Override
        public Type visit(Decimal type)
        {
            return createDecimalType(type.getPrecision(), type.getScale());
        }

        @Override
        public Type visit(Date type)
        {
            return DATE;
        }

        @Override
        public Type visit(Time type)
        {
            return TIME;
        }

        @Override
        public Type visit(Timestamp type)
        {
            return TIMESTAMP;
        }

        @Override
        public Type visit(Interval type)
        {
            throw new UnsupportedOperationException("Interval type is not supported");
        }

        @Override
        public Type visit(Duration type)
        {
            throw new UnsupportedOperationException("Duration type is not supported");
        }
    }
}
