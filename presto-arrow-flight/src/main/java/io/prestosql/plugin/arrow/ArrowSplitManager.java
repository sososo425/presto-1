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
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;

import javax.inject.Inject;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.arrow.ArrowErrorCode.ARROW_ERROR;
import static java.util.Objects.requireNonNull;

public class ArrowSplitManager
        implements ConnectorSplitManager
{
    private final FlightClientSupplier flightClientSupplier;

    @Inject
    public ArrowSplitManager(FlightClientSupplier flightClientSupplier)
    {
        this.flightClientSupplier = requireNonNull(flightClientSupplier, "flightClientSupplier is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        try (FlightClient client = flightClientSupplier.getClient(session)) {
            ArrowTableHandle arrowTableHandle = (ArrowTableHandle) table;
            FlightInfo flightInfo = client.getInfo(FlightDescriptor.path(arrowTableHandle.getTableName()));
            ImmutableList.Builder<ArrowSplit> splitBuilder = ImmutableList.builder();
            for (FlightEndpoint flightEndpoint : flightInfo.getEndpoints()) {
                splitBuilder.add(new ArrowSplit(flightEndpoint.getLocations().stream().map(Location::getUri).collect(toImmutableList()), flightEndpoint.getTicket().getBytes()));
            }
            return new FixedSplitSource(splitBuilder.build());
        }
        catch (Exception e) {
            throw new PrestoException(ARROW_ERROR, "Unable to fetch the details");
        }
    }
}
