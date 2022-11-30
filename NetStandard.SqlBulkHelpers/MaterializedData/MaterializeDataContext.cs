using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SqlBulkHelpers.MaterializedData
{
    public struct MaterializeDataContext : IAsyncDisposable
    {
        private readonly Func<MaterializationTableInfo[], Task> _finishMaterializationProcessFunc;

        public MaterializationTableInfo[] Tables { get; }

        public MaterializeDataContext(MaterializationTableInfo[] materializationTables, Func<MaterializationTableInfo[], Task> finishMaterializationProcessFunc)
        {
            _isDisposed = false;
            _finishMaterializationProcessFunc = finishMaterializationProcessFunc.AssertArgumentIsNotNull(nameof(finishMaterializationProcessFunc));
            Tables = materializationTables.AssertArgumentIsNotNull(nameof(materializationTables));
        }

        private bool _isDisposed;
        public async ValueTask DisposeAsync()
        {
            if (!_isDisposed)
            {
                await _finishMaterializationProcessFunc.Invoke(Tables);
                _isDisposed = true;
            }
        }
    }
}
