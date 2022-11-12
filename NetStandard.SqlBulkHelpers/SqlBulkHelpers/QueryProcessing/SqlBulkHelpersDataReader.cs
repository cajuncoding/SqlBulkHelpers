﻿using System;
using System.Collections.Generic;
using System.Data;
using FastMember;

namespace SqlBulkHelpers.SqlBulkHelpers.QueryProcessing
{
    internal sealed class SqlBulkHelpersDataReader<T> : IDataReader, IDisposable
    {
        private readonly SqlBulkHelpersProcessingDefinition _processingDefinition;
        private readonly TypeAccessor _fastTypeAccessor = TypeAccessor.Create(typeof(T));
        private readonly int _rowNumberColumnOrdinal;

        private IEnumerator<T> _dataEnumerator;
        private Dictionary<string, int> _processingDefinitionOrdinalDictionary;
        private int _entityCounter = 0;

        public SqlBulkHelpersDataReader(IEnumerable<T> entityData, SqlBulkHelpersProcessingDefinition processingDefinition)
        {
            // ReSharper disable once PossibleMultipleEnumeration
            _dataEnumerator = entityData.AssertArgumentIsNotNull(nameof(entityData)).GetEnumerator();
            _processingDefinition = processingDefinition.AssertArgumentIsNotNull(nameof(processingDefinition));
            _rowNumberColumnOrdinal = _processingDefinition.PropertyDefinitions.Length;
        }

        #region IDataReader Members (Minimal methods to be Implemented as required by SqlBulkCopy)

        public int Depth => 1;
        public bool IsClosed => _dataEnumerator == null;

        public bool Read()
        {
            if (IsClosed)
                throw new ObjectDisposedException(GetType().Name);

            _entityCounter++;
            return _dataEnumerator.MoveNext();
        }

        public int GetOrdinal(string name)
        {
            //Lazy Load the Ordinal reverse lookup dictionary (ONLY if needed)
            if (_processingDefinitionOrdinalDictionary == null)
            {
                //Populate our Property Ordinal reverse lookup dictionary...
                int i = 0;
                _processingDefinitionOrdinalDictionary = new Dictionary<string, int>();
                foreach (var propDef in _processingDefinition.PropertyDefinitions)
                    _processingDefinitionOrdinalDictionary[propDef.PropertyName] = i++;
            }

            if (SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME.Equals(name))
                return _rowNumberColumnOrdinal;
            else if (_processingDefinitionOrdinalDictionary.TryGetValue(name, out var ordinalIndex))
                return ordinalIndex;

            throw new ArgumentOutOfRangeException($"Property name [{name}] could not be found.");
        }

        public object GetValue(int i)
        {
            if (_dataEnumerator == null)
                throw new ObjectDisposedException(GetType().Name);

            //Handle RowNumber Pseudo Column...
            if (i == _rowNumberColumnOrdinal)
                return _entityCounter;

            var propDef = _processingDefinition.PropertyDefinitions[i];
            var propertyValue = _fastTypeAccessor[_dataEnumerator.Current, propDef.PropertyName];
            
            //Handle special edge cases to ensure that invalid identity values are mapped to unique invalid values.
            if (propDef.IsIdentityProperty && (int)propertyValue <= 0)
            {
                //Create a Unique but Invalid Fake Identity Id (e.g. negative number)!
                propertyValue = _entityCounter * -1;
            }

            return propertyValue;
        }

        public int FieldCount => _processingDefinition.PropertyDefinitions.Length;

        public void Close() => Dispose();
        public bool NextResult() => false;

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (!disposing) return;
            _dataEnumerator?.Dispose();
            _dataEnumerator = null;
        }

        #endregion

        #region Not Implemented Members

        public int RecordsAffected => -1;

        protected TValue ThrowNotImplementedException<TValue>() => throw new NotImplementedException();

        public DataTable GetSchemaTable() => ThrowNotImplementedException<DataTable>();

        public bool GetBoolean(int i) => ThrowNotImplementedException<bool>();
        public byte GetByte(int i) => ThrowNotImplementedException<byte>();
        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => ThrowNotImplementedException<long>();
        public char GetChar(int i) => ThrowNotImplementedException<char>();
        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => ThrowNotImplementedException<long>();
        public IDataReader GetData(int i) => ThrowNotImplementedException<IDataReader>();
        public string GetDataTypeName(int i) => ThrowNotImplementedException<string>();
        public DateTime GetDateTime(int i) => ThrowNotImplementedException<DateTime>();
        public decimal GetDecimal(int i) => ThrowNotImplementedException<decimal>();
        public double GetDouble(int i) => ThrowNotImplementedException<double>();
        public Type GetFieldType(int i) => ThrowNotImplementedException<Type>();
        public float GetFloat(int i) => ThrowNotImplementedException<float>();
        public Guid GetGuid(int i) => ThrowNotImplementedException<Guid>();
        public short GetInt16(int i) => ThrowNotImplementedException<short>();
        public int GetInt32(int i) => ThrowNotImplementedException<int>();
        public long GetInt64(int i) => ThrowNotImplementedException<long>();
        public string GetName(int i) => ThrowNotImplementedException<string>();
        public string GetString(int i) => ThrowNotImplementedException<string>();
        public int GetValues(object[] values) => ThrowNotImplementedException<int>();
        public bool IsDBNull(int i) => ThrowNotImplementedException<bool>();
        public object this[string name] => ThrowNotImplementedException<object>();
        public object this[int i] => ThrowNotImplementedException<object>();

        #endregion
    }}