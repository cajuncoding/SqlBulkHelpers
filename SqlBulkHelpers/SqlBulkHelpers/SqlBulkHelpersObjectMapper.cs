using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Reflection;

namespace SqlBulkHelpers
{
    public class SqlBulkHelpersObjectMapper
    {
        //TODO: BBernard - If beneficial (Need to Add Timers) we can improve the Reflection Performance here with Caching of PropertyInfo results, 
        //          use of Delegates for FASTer access to model members, etc. (IF practical and/or needed).
        public DataTable ConvertEntitiesToDatatable<T>(List<T> entityList, SqlBulkHelpersColumnDefinition identityColumnDefinition)
        {
            //Get the name of hte Identity Column
            //NOTE: BBernard - We take in the strongly typed class (immutable) to ensure that we have validated Parameter vs raw string!
            var identityColumnName = identityColumnDefinition.ColumnName;

            //TODO: BBERNARD - Optimilze this with internal Type level caching and possbily mapping these to Delegates for faster execution!
            //NOTE: We Map all Properties to an anonymous type with Index, Name, and base PropInfo here for easier logic below,
            //          and we ALWAYS convert to a List<> so that we always preserve the critical order of the PropertyInfo items!
            //          to simplify all following code.
            var propertyInfos = typeof(T).GetProperties().Select((pi) => new {
                Name = pi.Name,
                //Early determination if a Property is an Identity Property for Fast processing later.
                IsIdentityProperty = pi.Name.Equals(identityColumnName, StringComparison.OrdinalIgnoreCase),
                PropInfo = pi
            }).ToList();

            DataTable dataTable = new DataTable();
            dataTable.Columns.AddRange(propertyInfos.Select(pi => new DataColumn
            {
                ColumnName = pi.Name,
                DataType = Nullable.GetUnderlyingType(pi.PropInfo.PropertyType) ?? pi.PropInfo.PropertyType,
                //We Always allow Null to make below logic easier, and it's the Responsibility of the Model to ensure values are Not Null vs Nullable.
                AllowDBNull = true //Nullable.GetUnderlyingType(pi.PropertyType) == null ? false : true
            }).ToArray());

            //BBernaard - We ALWAYS Add the internal RowNumber reference so that we can exactly correllate Identity values 
            //  from the Server back to original records that we passed!
            dataTable.Columns.Add(new DataColumn()
            {
                ColumnName = SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME,
                DataType = typeof(int),
                AllowDBNull = true
            });

            int rowCounter = 1;
            int idenityIdFakeCounter = -1;
            foreach (T entity in entityList)
            {
                var rowValues = propertyInfos.Select(p => {
                    var value = p.PropInfo.GetValue(entity);

                    //Handle special cases to ensure that Identity values are mapped to unique invalid values.
                    if (p.IsIdentityProperty && (int)value <= 0)
                    {
                        //Create a Unique but Invalid Fake Identity Id (e.g. negative number)!
                        value = idenityIdFakeCounter--;
                    }

                    return value;

                }).ToArray();

                //Add the Values (must be critically in the same order as the PropertyInfos List) as a new Row!
                var newRow = dataTable.Rows.Add(rowValues);

                //Alwasy set the unique Row Number identifier
                newRow[SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME] = rowCounter++;
            }

            return dataTable;
        }
    }
}
