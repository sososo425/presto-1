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

import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class ArrowConnector
        implements Connector
{
    private final ArrowMetadata arrowMetadata;
    private final ArrowSplitManager arrowSplitManager;
    private final ArrowPageSourceProvider arrowPageSourceProvider;
    private final ArrowPageSinkProvider arrowPageSinkProvider;

    @Inject
    public ArrowConnector(ArrowMetadata arrowMetadata, ArrowSplitManager arrowSplitManager, ArrowPageSourceProvider arrowPageSourceProvider, ArrowPageSinkProvider arrowPageSinkProvider)
    {
        this.arrowMetadata = requireNonNull(arrowMetadata, "arrowMetadata is null");
        this.arrowSplitManager = requireNonNull(arrowSplitManager, "arrowSplitManager is null");
        this.arrowPageSourceProvider = requireNonNull(arrowPageSourceProvider, "arrowPageSourceProvider is null");
        this.arrowPageSinkProvider = requireNonNull(arrowPageSinkProvider, "arrowPageSinkProvider us null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return ArrowTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return arrowMetadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return arrowSplitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return arrowPageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return arrowPageSinkProvider;
    }
}
