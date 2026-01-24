using System;

namespace SqlBulkHelpers.Interfaces
{
    /// <summary>
    /// Provies an interface to intercept and manage the handling of any given property being written to the database by SqlBulkHelpers library.
    /// Simply implement this interface on any custom Attribute you want/create and it will be detected and used to map values from the Property
    ///     to the TConverted target type for storing into the database whe SQL Bulk Inserting or Updating.
    /// </summary>
    public interface ISqlBulkHelpersPropertyConverter
    {
        object ConvertPropValue(object propValue);
    }
}
