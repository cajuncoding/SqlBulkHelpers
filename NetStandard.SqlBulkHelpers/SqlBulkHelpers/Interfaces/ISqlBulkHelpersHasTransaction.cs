using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlBulkHelpers.SqlBulkHelpers.Interfaces
{
    public interface ISqlBulkHelpersHasTransaction
    {
        SqlTransaction GetTransaction();
    }
}
