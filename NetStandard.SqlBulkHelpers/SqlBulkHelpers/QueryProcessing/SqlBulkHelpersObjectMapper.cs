using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Collections.Concurrent;
using System.Reflection;

namespace SqlBulkHelpers
{
    public class SqlBulkHelpersObjectMapper
    {

        public DataTable ConvertEntitiesToDataTable<T>(IEnumerable<T> entityList, SqlBulkHelpersColumnDefinition identityColumnDefinition)
        {
            //Get the name of hte Identity Column
            //NOTE: BBernard - We take in the strongly typed class (immutable) to ensure that we have validated Parameter vs raw string!
            //var identityColumnName = identityColumnDefinition.ColumnName;

            //NOTE: We Map all Properties to an anonymous type with Index, Name, and base PropInfo here for easier logic below,
            //          and we ALWAYS convert to a List<> so that we always preserve the critical order of the PropertyInfo items!
            //          to simplify all following code.
            //NOTE: The helper class provides internal Lazy type caching for better performance once a type has been loaded.
            var propertyDefs = SqlBulkHelpersObjectReflectionFactory.GetPropertyDefinitions<T>(identityColumnDefinition);

            DataTable dataTable = new DataTable();
            dataTable.Columns.AddRange(propertyDefs.Select(pi => new DataColumn
            {
                ColumnName = pi.Name,
                DataType = Nullable.GetUnderlyingType(pi.PropInfo.PropertyType) ?? pi.PropInfo.PropertyType,
                //We Always allow Null to make below logic easier, and it's the Responsibility of the Model to ensure values are Not Null vs Nullable.
                AllowDBNull = true //Nullable.GetUnderlyingType(pi.PropertyType) == null ? false : true
            }).ToArray());

            //BBernard - We ALWAYS Add the internal RowNumber reference so that we can exactly correlate Identity values from the Server
            //              back to original records that we passed!
            //NOTE: THIS IS CRITICAL because SqlBulkCopy and Sql Server OUTPUT clause do not preserve Order; e.g. it may change based
            //      on execution plan (indexes/no indexes, etc.).
            dataTable.Columns.Add(new DataColumn()
            {
                ColumnName = SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME,
                DataType = typeof(int),
                AllowDBNull = true
            });

            int rowCounter = 1;
            int identityIdFakeCounter = -1;
            foreach (T entity in entityList)
            {
                var rowValues = propertyDefs.Select(p => {
                    var value = p.PropInfo.GetValue(entity);

                    //Handle special cases to ensure that Identity values are mapped to unique invalid values.
                    if (p.IsIdentityProperty && (int)value <= 0)
                    {
                        //Create a Unique but Invalid Fake Identity Id (e.g. negative number)!
                        value = identityIdFakeCounter--;
                    }

                    return value;

                }).ToArray();

                //Add the Values (must be critically in the same order as the PropertyInfos List) as a new Row!
                var newRow = dataTable.Rows.Add(rowValues);

                //Always set the unique Row Number identifier
                newRow[SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME] = rowCounter++;
            }

            return dataTable;
        }
    }
}
