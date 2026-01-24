using System;
using SqlBulkHelpers.CustomExtensions;
using SqlBulkHelpers.Interfaces;

namespace SqlBulkHelpers
{
    public class SqlBulkLambdaPropertyConverter : ISqlBulkHelpersPropertyConverter
    {
        public Func<object, object> ConverterFunc { get; protected set; }

        public SqlBulkLambdaPropertyConverter(Func<object, object> coverterFunc)
        {
            ConverterFunc = coverterFunc.AssertArgumentIsNotNull(nameof(ConverterFunc));
        }

        public object ConvertPropValue(object propValue) => ConverterFunc.Invoke(propValue);
    }
}
