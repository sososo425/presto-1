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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.prestosql.plugin.arrow.writer.ColumnWriter;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorPageSink;
import org.apache.arrow.flight.FlightClient.ClientStreamListener;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class ArrowPageSink
        implements ConnectorPageSink
{
    private final long pageSizeLimit = new DataSize(16, DataSize.Unit.MEGABYTE).toBytes();
    private final VectorSchemaRoot vectorSchemaRoot;
    private final ClientStreamListener streamListener;
    private final List<ColumnWriter> columnWriters;
    private long sizeInBytes;
    private final Runnable onFinish;

    public ArrowPageSink(ClientStreamListener streamListener, List<ColumnWriter> columnWriters, VectorSchemaRoot vectorSchemaRoot, Runnable onFinish, long pageSizeLimit)
    {
        this.vectorSchemaRoot = requireNonNull(vectorSchemaRoot, "vectorSchemaRoot is null");
        this.streamListener = requireNonNull(streamListener, "streamListener is null");
        this.columnWriters = ImmutableList.copyOf(requireNonNull(columnWriters, "columnWriter is null"));
        this.onFinish = requireNonNull(onFinish, "onFinish is null");
        this.vectorSchemaRoot.allocateNew();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        vectorSchemaRoot.setRowCount(vectorSchemaRoot.getRowCount() + page.getPositionCount());
        for (int i = 0; i < page.getChannelCount(); i++) {
            columnWriters.get(i).writeBlock(page.getBlock(i));
        }
        sizeInBytes = sizeInBytes + page.getSizeInBytes();
        if (sizeInBytes > pageSizeLimit) {
            sizeInBytes = 0;
            streamListener.putNext();
            vectorSchemaRoot.clear();
            vectorSchemaRoot.allocateNew();
        }

        return CompletableFuture.completedFuture(ImmutableList.of());
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        streamListener.putNext();
        streamListener.completed();
        vectorSchemaRoot.clear();
        vectorSchemaRoot.close();
        onFinish.run();
        return CompletableFuture.completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
    }
}
