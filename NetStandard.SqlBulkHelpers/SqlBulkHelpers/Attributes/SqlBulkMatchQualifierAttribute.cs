using System;
using System.Collections.Generic;
using System.Text;

namespace SqlBulkHelpers.SqlBulkHelpers.Attributes
{
    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class SqlBulkMatchQualifierAttribute : Attribute
    {
        public SqlBulkMatchQualifierAttribute()
        {
        }
    }
}
