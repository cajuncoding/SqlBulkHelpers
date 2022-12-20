using System;

namespace SqlBulkHelpers
{
    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class SqlBulkMatchQualifierAttribute : Attribute
    {
        public SqlBulkMatchQualifierAttribute()
        {
        }
    }
}
