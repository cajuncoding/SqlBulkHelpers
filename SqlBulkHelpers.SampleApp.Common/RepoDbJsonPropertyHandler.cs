using RepoDb.Interfaces;
using RepoDb.Options;
using System.Text.Json;

namespace SqlBulkHelpers.Tests
{
    public class RepoDbJsonPropertyHandler : IPropertyHandler<string, TestElement>
    {
        public TestElement Get(string input, PropertyHandlerGetOptions options)
            => JsonSerializer.Deserialize<TestElement>(input, SqlBulkHelpersConfig.DefaultConfig.SqlBulkJsonConverterSerializerOptions);

        public string Set(TestElement input, PropertyHandlerSetOptions options)
            => JsonSerializer.Serialize(input, SqlBulkHelpersConfig.DefaultConfig.SqlBulkJsonConverterSerializerOptions);
    }
}
