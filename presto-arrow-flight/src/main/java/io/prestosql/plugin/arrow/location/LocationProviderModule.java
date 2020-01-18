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

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.arrow.ArrowFlightConfig;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.plugin.arrow.location.LocationProviderType.INSECURE;

public class LocationProviderModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(ArrowFlightConfig.class);
        bindLocationProviderModule(
                INSECURE,
                internalBinder -> internalBinder.bind(LocationProvider.class).to(InsecureLocationProvider.class).in(SINGLETON));
    }

    private void bindLocationProviderModule(LocationProviderType name, Module module)
    {
        install(installModuleIf(
                ArrowFlightConfig.class,
                config -> name.equals(config.getLocationProviderType()),
                module));
    }
}
