using System;
using System.Collections.Generic;
using System.Text;

namespace SqlBulkHelpers
{
    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class SqlBulkIgnoreAttribute : Attribute
    {
    }
}
