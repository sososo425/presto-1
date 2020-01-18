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
package io.prestosql.plugin.arrow.location;

import io.prestosql.plugin.arrow.ArrowFlightConfig;
import io.prestosql.spi.connector.ConnectorSession;
import org.apache.arrow.flight.Location;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class InsecureLocationProvider
        implements LocationProvider
{
    private final String flightServerUrl;
    private final int flightServerPort;

    @Inject
    public InsecureLocationProvider(ArrowFlightConfig arrowFlightConfig)
    {
        this.flightServerUrl = requireNonNull(arrowFlightConfig, "arrowFlightConfig is null").getFlightServerUrl();
        this.flightServerPort = requireNonNull(arrowFlightConfig, "arrowFlightConfig is null").getFlightServerPort();
    }

    @Override
    public Location getLocation(ConnectorSession connectorSession)
    {
        return Location.forGrpcInsecure(flightServerUrl, flightServerPort);
    }
}
