using SqlBulkHelpers.Interfaces;
using System;
using System.Text.Json;

namespace SqlBulkHelpers
{
    /// <summary>
    /// Dynamically convert any property that is annotated with this attribute to JSON String before storing in the Database.
    /// Provides and example and helpful utility using hte ISqlBulkHelperPropertyConverter<TProp, TConverted> interface!
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class SqlBulkConvertToJsonAttribute : Attribute, ISqlBulkHelpersPropertyTransformer
    {
        object ISqlBulkHelpersPropertyTransformer.TransformPropValue(object propValue)
            => JsonSerializer.Serialize(
                propValue, 
                SqlBulkHelpersConfig.DefaultConfig.SqlBulkJsonConverterSerializerOptions ?? new JsonSerializerOptions()
            );
    }
}
