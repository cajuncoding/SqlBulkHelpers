﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkHelpersSample.ConsoleApp
{
    public static class SqlBulkHelpersSampleApp
    {
        public const string TestTableName = "[dbo].[SqlBulkHelpersTestElements]";
        public const string TestChildTableName = "[dbo].[SqlBulkHelpersTestElements_Child_NoIdentity]";
        public const int SqlTimeoutSeconds = 120;
    }
}
