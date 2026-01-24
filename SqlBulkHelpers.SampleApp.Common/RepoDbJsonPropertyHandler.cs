using RepoDb.Interfaces;
using RepoDb.Options;
using System.Text.Json;

namespace SqlBulkHelpers.Tests
{
    public class RepoDbJsonPropertyHandler : IPropertyHandler<string, TestElement>
    {
        public TestElement Get(string input, PropertyHandlerGetOptions options)
            => JsonSerializer.Deserialize<TestElement>(input, SqlBulkConvertToJsonAttribute.SqlBulkJsonConverterSerializerOptions);

        public string Set(TestElement input, PropertyHandlerSetOptions options)
            => JsonSerializer.Serialize(input, SqlBulkConvertToJsonAttribute.SqlBulkJsonConverterSerializerOptions);
    }
}
