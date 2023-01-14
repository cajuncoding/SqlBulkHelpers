using System.Threading.Tasks;

namespace SqlBulkHelpers.MaterializedData
{
    public interface IMaterializeDataContextCompletionSource : IMaterializeDataContext
    {
        Task FinishMaterializeDataProcessAsync();
    }
}