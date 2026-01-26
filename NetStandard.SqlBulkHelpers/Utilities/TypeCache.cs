using SqlBulkHelpers.Interfaces;
using System;

namespace SqlBulkHelpers
{
    internal static class TypeCache
    {
        public static readonly Type Type = typeof(Type);

        public static readonly Type Short = typeof(short);
        public static readonly Type Int = typeof(int);
        public static readonly Type Long = typeof(long);
        public static readonly Type Byte = typeof(byte);

        public static readonly Type SqlBulkHelperIdentitySetter = typeof(ISqlBulkHelperIdentitySetter);
        public static readonly Type SqlBulkHelperBigIntIdentitySetter = typeof(ISqlBulkHelperBigIntIdentitySetter);
    }
}
