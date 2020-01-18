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
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.statistics.ComputedStatistics;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.plugin.arrow.ArrowErrorCode.ARROW_ERROR;
import static io.prestosql.plugin.arrow.TypeUtils.fromArrowType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.apache.arrow.flight.Criteria.ALL;

public class ArrowMetadata
        implements ConnectorMetadata
{
    private static final Logger LOGGER = Logger.get(ArrowMetadata.class);

    private final FlightClientSupplier flightClientSupplier;

    @Inject
    public ArrowMetadata(FlightClientSupplier flightClientSupplier)
    {
        this.flightClientSupplier = requireNonNull(flightClientSupplier, "flightClientSupplier is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of("default");
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<SchemaTableName> tableNameBuilder = ImmutableList.builder();
        if (schemaName.orElse("default").equals("default")) {
            for (FlightInfo flightInfo : flightClientSupplier.getClient(session).listFlights(ALL)) {
                if (flightInfo.getDescriptor().getPath().size() == 1) {
                    tableNameBuilder.add(new SchemaTableName("default", getOnlyElement(flightInfo.getDescriptor().getPath())));
                }
            }
        }
        return tableNameBuilder.build();
    }

    @Nullable
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        try (FlightClient client = flightClientSupplier.getClient(session)) {
            return new ArrowTableHandle(client.getInfo(FlightDescriptor.path(tableName.getTableName())).getDescriptor().getPath().get(0));
        }
        catch (Exception e) {
            LOGGER.error("Unable to fetch table details", e);
            return null;
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table)
    {
        ArrowTableHandle tableHandle = (ArrowTableHandle) table;
        return getColumnHandles(session, tableHandle.getTableName()).stream()
                .collect(toMap(ArrowColumnHandle::getColumnName, column -> column));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ArrowTableHandle tableHandle = (ArrowTableHandle) table;
        return new ConnectorTableMetadata(
                new SchemaTableName("default", tableHandle.getTableName()),
                getColumnHandles(session, tableHandle.getTableName()).stream()
                        .map(ArrowColumnHandle::getColumnMetadata)
                        .collect(toImmutableList()));
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        return new ArrowOutputTableHandle(new ArrowTableHandle(tableMetadata.getTable().getTableName()), tableMetadata.getColumns().stream().map(me -> new ArrowColumnHandle(me.getName(), me.getType())).collect(Collectors.toList()));
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((ArrowColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ArrowTableHandle table = (ArrowTableHandle) tableHandle;
        List<ArrowColumnHandle> columnHandles = getColumnHandles(session, ((ArrowTableHandle) tableHandle).getTableName());
        return new ArrowInsertTableHandle(table, columnHandles);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    private List<ArrowColumnHandle> getColumnHandles(ConnectorSession session, String tableName)
    {
        try (FlightClient client = flightClientSupplier.getClient(session)) {
            FlightInfo flightInfo = client.getInfo(FlightDescriptor.path(tableName));
            return flightInfo.getSchema().getFields().stream()
                    .map(field -> new ArrowColumnHandle(field.getName(), fromArrowType(field.getType())))
                    .collect(toImmutableList());
        }
        catch (Exception e) {
            throw new PrestoException(ARROW_ERROR, "Unable to fetch column details", e);
        }
    }
}
