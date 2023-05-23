using System;

namespace SqlBulkHelpers.Interfaces
{
    public interface ISqlBulkHelperIdentitySetter
    {
        void SetIdentityId(int id);
    }
    
    public interface ISqlBulkHelperBigIntIdentitySetter
    {
        void SetIdentityId(long id);
    }
}
