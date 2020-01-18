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

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.prestosql.plugin.arrow.location.LocationProviderType;

import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.plugin.arrow.location.LocationProviderType.INSECURE;

public class ArrowFlightConfig
{
    private String flightServerUrl;
    private int flightServerPort;
    private LocationProviderType locationProviderType = INSECURE;
    private DataSize maxBufferSize = new DataSize(16, MEGABYTE);

    @Config("flight-server-url")
    public ArrowFlightConfig setFlightServerUrl(String flightServerUrl)
    {
        this.flightServerUrl = flightServerUrl;
        return this;
    }

    @NotNull
    public String getFlightServerUrl()
    {
        return flightServerUrl;
    }

    @Config("flight-server-port")
    public ArrowFlightConfig setFlightServerPort(int flightServerPort)
    {
        this.flightServerPort = flightServerPort;
        return this;
    }

    public int getFlightServerPort()
    {
        return flightServerPort;
    }

    @Config("location-provider-type")
    public ArrowFlightConfig setLocationProviderType(LocationProviderType locationProviderType)
    {
        this.locationProviderType = locationProviderType;
        return this;
    }

    @NotNull
    public LocationProviderType getLocationProviderType()
    {
        return locationProviderType;
    }

    @Config("max-buffer-size")
    public ArrowFlightConfig setMaxBufferSize(DataSize maxBufferSize)
    {
        this.maxBufferSize = maxBufferSize;
        return this;
    }

    @NotNull
    public DataSize getMaxBufferSize()
    {
        return maxBufferSize;
    }
}
