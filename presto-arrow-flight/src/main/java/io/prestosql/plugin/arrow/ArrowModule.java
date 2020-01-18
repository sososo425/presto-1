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

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class ArrowModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(ArrowFlightConfig.class);
        binder.bind(FlightClientSupplier.class).in(SINGLETON);
        binder.bind(ArrowMetadata.class).in(SINGLETON);
        binder.bind(ArrowConnector.class).in(SINGLETON);
        binder.bind(ArrowSplitManager.class).in(SINGLETON);
        binder.bind(ArrowPageSourceProvider.class).in(SINGLETON);
        binder.bind(ArrowPageSinkProvider.class).in(SINGLETON);
    }

    @Inject
    @Singleton
    @Provides
    public BufferAllocator getBufferAllocator(ArrowFlightConfig arrowFlightConfig)
    {
        return new RootAllocator(arrowFlightConfig.getMaxBufferSize().toBytes());
    }
}
