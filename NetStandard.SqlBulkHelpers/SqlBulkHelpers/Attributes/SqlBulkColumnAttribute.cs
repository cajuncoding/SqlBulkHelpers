using System;
using System.Collections.Generic;
using System.Text;

namespace SqlBulkHelpers.SqlBulkHelpers.Attributes
{
    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class SqlBulkColumnAttribute : Attribute
    {
        public string Name { get; set; }
        public SqlBulkColumnAttribute(string mappedDbColumnName)
        {
            this.Name = mappedDbColumnName;
        }
    }
}
