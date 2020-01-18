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

import io.prestosql.plugin.arrow.location.LocationProvider;
import io.prestosql.spi.connector.ConnectorSession;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;

import javax.inject.Inject;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class FlightClientSupplier
{
    private final LocationProvider locationProvider;
    private final BufferAllocator bufferAllocator;

    @Inject
    public FlightClientSupplier(LocationProvider locationProvider, BufferAllocator bufferAllocator)
    {
        this.locationProvider = requireNonNull(locationProvider, "locationProvider is null");
        this.bufferAllocator = requireNonNull(bufferAllocator, "bufferAllocator is null");
    }

    public FlightClient getClient(ConnectorSession session)
    {
        return FlightClient.builder(bufferAllocator, locationProvider.getLocation(session))
                .build();
    }

    public FlightClient getClient(URI uri)
    {
        return FlightClient.builder(bufferAllocator, Location.forGrpcDomainSocket(uri.toString()))
                .build();
    }
}
