using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SqlBulkHelpers.MaterializedData
{
    public interface IMaterializeDataContextCompletionSource : IMaterializeDataContext
    {
        Task FinishMaterializeDataProcessAsync(SqlTransaction sqlTransaction);
    }
}