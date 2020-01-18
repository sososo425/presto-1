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

import io.prestosql.plugin.arrow.writer.ColumnWriter;
import io.prestosql.plugin.arrow.writer.ColumnWriters;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import javax.inject.Inject;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class ArrowPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final BufferAllocator allocator;
    private final FlightClientSupplier flightClientSupplier;

    @Inject
    public ArrowPageSinkProvider(FlightClientSupplier flightClientSupplier, BufferAllocator allocator)
    {
        this.flightClientSupplier = requireNonNull(flightClientSupplier, "flightClientSupplier is null");
        this.allocator = requireNonNull(allocator, "allocatior is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        ArrowOutputTableHandle arrowOutputTableHandle = (ArrowOutputTableHandle) outputTableHandle;

        List<ColumnWriter> columnWriters = arrowOutputTableHandle.getColumnHandles().stream().map(x -> ColumnWriters.getColumnWriter(x, allocator)).collect(Collectors.toList());

        VectorSchemaRoot root = new VectorSchemaRoot(columnWriters.stream().map(ColumnWriter::getValueVector).collect(Collectors.toList()));
        FlightClient flightClient = flightClientSupplier.getClient(session);
        return new ArrowPageSink(flightClient.startPut(
                FlightDescriptor.path(((ArrowOutputTableHandle) outputTableHandle).getTableHandle().getTableName()),
                root, new FlightClient.PutListener()
                {
                    @Override
                    public void getResult()
                    {
                    }

                    @Override
                    public void onNext(PutResult val)
                    {
                    }

                    @Override
                    public void onError(Throwable t)
                    {
                    }

                    @Override
                    public void onCompleted()
                    {
                    }
                }),
                columnWriters,
                root,
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try {
                            flightClient.close();
                            System.out.println("Free size " + allocator.getAllocatedMemory());
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }, 0);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        return null;
    }
}
