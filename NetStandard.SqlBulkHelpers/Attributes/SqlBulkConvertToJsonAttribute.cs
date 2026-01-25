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
        /// <summary>
        /// You can globally set this in your application root or bootstrapping logic to ensure the desired Serialization options
        ///     are used for all implementations of the Json Conversion!
        /// </summary>
        public static JsonSerializerOptions SqlBulkJsonConverterSerializerOptions { get; set; } = new JsonSerializerOptions();

        public static void SetJsonSerializerOptions(JsonSerializerOptions options)
            => SqlBulkJsonConverterSerializerOptions = options;

        object ISqlBulkHelpersPropertyTransformer.TransformPropValue(object propValue)
            => JsonSerializer.Serialize(propValue, SqlBulkJsonConverterSerializerOptions);
    }
}
