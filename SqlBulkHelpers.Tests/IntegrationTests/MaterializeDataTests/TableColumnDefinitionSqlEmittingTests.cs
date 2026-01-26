using System;

namespace SqlBulkHelpers.Tests.IntegrationTests
{
    [TestClass]
    public class TableColumnDefinitionSqlEmittingTests
    {
        // ─────────────────────────────────────────────────────────────────────────────
        // Helper: Create a TableColumnDefinition quickly for tests
        // Adjust the constructor argument order here if your ctor differs.
        // ─────────────────────────────────────────────────────────────────────────────
        private static TableColumnDefinition NewCol(
            string columnName,
            string dataType,
            bool isNullable = true,
            bool isIdentity = false,
            long? identitySeed = null,
            long? identityIncrement = null,
            bool isComputed = false,
            string computedDefinition = null,
            bool isPersisted = false,
            bool isRowGuid = false,
            string collation = null,
            int? charLen = null,
            int? numericPrecision = null,
            int? numericScale = null,
            int? dtPrecision = null,
            int? binaryLen = null
        )
        {
            // NOTE: This assumes your constructor includes BinaryMaxLength as the LAST parameter.
            // If your constructor is different, update parameter order here.
            return new TableColumnDefinition(
                sourceTableSchema: "dbo",
                sourceTableName: "T",
                ordinalPosition: 1,
                columnName: columnName,
                dataType: dataType,
                isNullableColumn: isNullable,
                isIdentityColumn: isIdentity,
                identitySeedValue: identitySeed,
                identityIncrementValue: identityIncrement,
                isComputedColumn: isComputed,
                computedColumnDefinition: computedDefinition,
                isPersistedColumn: isPersisted,
                isRowGuidColumn: isRowGuid,
                characterCollationName: collation,
                characterMaxLength: charLen,
                numericPrecision: numericPrecision,
                numericPrecisionRadix: null,
                numericScale: numericScale,
                dateTimePrecision: dtPrecision,
                binaryMaxLength: binaryLen
            );
        }

        // ─────────────────────────────────────────────────────────────────────────────
        // NVARCHAR / NCHAR (character length)
        // ─────────────────────────────────────────────────────────────────────────────

        [TestMethod]
        public void NVarChar_WithLength_RendersCharacters()
        {
            var col = NewCol("C", "nvarchar", isNullable: true, charLen: 50, binaryLen: 100);
            Assert.AreEqual("[C] nvarchar(50) NULL", col.TableScriptColumnSql);
        }

        [TestMethod]
        public void NVarChar_Max_RendersMax()
        {
            var col = NewCol("C", "nvarchar", isNullable: false, charLen: null, binaryLen: null);
            Assert.AreEqual("[C] nvarchar(MAX) NOT NULL", col.TableScriptColumnSql);
        }

        [TestMethod]
        public void NChar_Max_RendersMax()
        {
            var col = NewCol("C", "nchar", isNullable: true, charLen: null, binaryLen: null);
            Assert.AreEqual("[C] nchar(MAX) NULL", col.TableScriptColumnSql);
        }

        // ─────────────────────────────────────────────────────────────────────────────
        // VARCHAR / CHAR (byte length)
        // ─────────────────────────────────────────────────────────────────────────────

        [TestMethod]
        public void VarChar_WithLength_RendersBytes()
        {
            var col = NewCol("C", "varchar", isNullable: true, charLen: 20, binaryLen: 20);
            Assert.AreEqual("[C] varchar(20) NULL", col.TableScriptColumnSql);
        }

        [TestMethod]
        public void VarChar_Max_RendersMax()
        {
            var col = NewCol("C", "varchar", isNullable: false, charLen: null, binaryLen: null);
            Assert.AreEqual("[C] varchar(MAX) NOT NULL", col.TableScriptColumnSql);
        }

        // ─────────────────────────────────────────────────────────────────────────────
        // BINARY / VARBINARY (byte length)
        // ─────────────────────────────────────────────────────────────────────────────

        [TestMethod]
        public void VarBinary_WithLength_RendersBytes()
        {
            var col = NewCol("C", "varbinary", isNullable: true, charLen: 0, binaryLen: 8000);
            Assert.AreEqual("[C] varbinary(8000) NULL", col.TableScriptColumnSql);
        }

        [TestMethod]
        public void VarBinary_Max_RendersMax()
        {
            var col = NewCol("C", "varbinary", isNullable: false, charLen: null, binaryLen: null);
            Assert.AreEqual("[C] varbinary(MAX) NOT NULL", col.TableScriptColumnSql);
        }

        [TestMethod]
        public void Binary_WithLength_RendersBytes()
        {
            var col = NewCol("C", "binary", isNullable: true, charLen: 0, binaryLen: 8);
            Assert.AreEqual("[C] binary(8) NULL", col.TableScriptColumnSql);
        }

        [TestMethod]
        public void NegativeBinaryLength_TreatedAsMax()
        {
            var col = NewCol("C", "varbinary", isNullable: true, charLen: null, binaryLen: -1);
            Assert.AreEqual("[C] varbinary(MAX) NULL", col.TableScriptColumnSql);
        }

        // ─────────────────────────────────────────────────────────────────────────────
        // DECIMAL / NUMERIC
        // ─────────────────────────────────────────────────────────────────────────────

        [TestMethod]
        public void Decimal_WithPrecisionScale_RendersCorrectly()
        {
            var col = NewCol("C", "decimal", isNullable: false, numericPrecision: 18, numericScale: 4);
            Assert.AreEqual("[C] decimal(18,4) NOT NULL", col.TableScriptColumnSql);
        }

        [TestMethod]
        public void Numeric_WithDefaults_RendersDefaultPrecision()
        {
            var col = NewCol("C", "numeric", isNullable: true, numericPrecision: null, numericScale: null);
            Assert.AreEqual("[C] numeric(18,0) NULL", col.TableScriptColumnSql);
        }

        // ─────────────────────────────────────────────────────────────────────────────
        // DATETIME2 / TIME / DATETIMEOFFSET (precision clamp)
        // ─────────────────────────────────────────────────────────────────────────────

        [TestMethod]
        public void DateTime2_DefaultPrecision_Is7()
        {
            var col = NewCol("C", "datetime2", isNullable: true, dtPrecision: null);
            Assert.AreEqual("[C] datetime2(7) NULL", col.TableScriptColumnSql);
        }

        [TestMethod]
        public void DateTime2_PrecisionAbove7_ClampedTo7()
        {
            var col = NewCol("C", "datetime2", isNullable: false, dtPrecision: 9);
            Assert.AreEqual("[C] datetime2(7) NOT NULL", col.TableScriptColumnSql);
        }

        [TestMethod]
        public void DateTimeOffset_Precision3_RendersCorrectly()
        {
            var col = NewCol("C", "datetimeoffset", isNullable: true, dtPrecision: 3);
            Assert.AreEqual("[C] datetimeoffset(3) NULL", col.TableScriptColumnSql);
        }

        [TestMethod]
        public void Time_Precision0_RendersCorrectly()
        {
            var col = NewCol("C", "time", isNullable: true, dtPrecision: 0);
            Assert.AreEqual("[C] time(0) NULL", col.TableScriptColumnSql);
        }

        // ─────────────────────────────────────────────────────────────────────────────
        // IDENTITY
        // ─────────────────────────────────────────────────────────────────────────────

        [TestMethod]
        public void Int_Identity_DefaultsTo_1_1_WhenNull()
        {
            var col = NewCol("Id", "int", isNullable: false, isIdentity: true, identitySeed: null, identityIncrement: null);
            Assert.AreEqual("[Id] int IDENTITY(1,1) NOT NULL", col.TableScriptColumnSql);
        }

        [TestMethod]
        public void BigInt_Identity_CustomSeedIncrement()
        {
            var col = NewCol("Id", "bigint", isNullable: false, isIdentity: true, identitySeed: 100, identityIncrement: 5);
            Assert.AreEqual("[Id] bigint IDENTITY(100,5) NOT NULL", col.TableScriptColumnSql);
        }

        // ─────────────────────────────────────────────────────────────────────────────
        // ROWGUIDCOL
        // ─────────────────────────────────────────────────────────────────────────────

        [TestMethod]
        public void UniqueIdentifier_RowGuidCol_RendersFlag()
        {
            var col = NewCol("RowGuid", "uniqueidentifier", isNullable: false, isRowGuid: true);
            Assert.AreEqual("[RowGuid] uniqueidentifier ROWGUIDCOL NOT NULL", col.TableScriptColumnSql);
        }

        // ─────────────────────────────────────────────────────────────────────────────
        // COLLATION (only meaningful for char/nchar/varchar/nvarchar)
        // ─────────────────────────────────────────────────────────────────────────────

        [TestMethod]
        public void NVarChar_WithCollation_RendersCollation()
        {
            var col = NewCol("Name", "nvarchar", isNullable: false, charLen: 100,
                             collation: "Latin1_General_100_CI_AS_SC_UTF8");
            Assert.AreEqual("[Name] nvarchar(100) COLLATE [Latin1_General_100_CI_AS_SC_UTF8] NOT NULL", col.TableScriptColumnSql);
        }

        // ─────────────────────────────────────────────────────────────────────────────
        // COMPUTED (Persisted and Non-Persisted)
        // ─────────────────────────────────────────────────────────────────────────────

        [TestMethod]
        public void Computed_NonPersisted_RendersExpression()
        {
            var col = NewCol("ComputedValue", "nvarchar", isComputed: true, computedDefinition: "CONCAT([A],N'::',[B])", isPersisted: false);
            // Exact spacing may vary based on your Build logic; check core parts exist.
            StringAssert.StartsWith(col.TableScriptColumnSql, "[ComputedValue] AS (CONCAT([A],N'::',[B]))");
            Assert.DoesNotContain(col.TableScriptColumnSql, "PERSISTED");
        }

        [TestMethod]
        public void Computed_Persisted_RendersExpressionAndPersisted()
        {
            var col = NewCol("ComputedValue", "nvarchar", isComputed: true, computedDefinition: "([A]+[B])", isPersisted: true);
            StringAssert.StartsWith(col.TableScriptColumnSql, "[ComputedValue] AS (([A]+[B]))");
            StringAssert.Contains(col.TableScriptColumnSql, "PERSISTED");
        }

        // ─────────────────────────────────────────────────────────────────────────────
        // Types without parameters
        // ─────────────────────────────────────────────────────────────────────────────

        [TestMethod]
        public void Int_NoParams_RendersTypeOnly()
        {
            var col = NewCol("C", "int", isNullable: true);
            Assert.AreEqual("[C] int NULL", col.TableScriptColumnSql);
        }

        [TestMethod]
        public void UniqueIdentifier_NoParams_RendersTypeOnly()
        {
            var col = NewCol("C", "uniqueidentifier", isNullable: true);
            Assert.AreEqual("[C] uniqueidentifier NULL", col.TableScriptColumnSql);
        }

        [TestMethod]
        public void Bit_NoParams_RendersTypeOnly()
        {
            var col = NewCol("C", "bit", isNullable: false);
            Assert.AreEqual("[C] bit NOT NULL", col.TableScriptColumnSql);
        }

        // ─────────────────────────────────────────────────────────────────────────────
        // Null/invalid datatype fallback
        // ─────────────────────────────────────────────────────────────────────────────

        [TestMethod]
        public void NullDataType_FallsBackToSqlVariant()
        {
            var col = NewCol("C", dataType: null, isNullable: true);
            Assert.AreEqual("[C] SQL_VARIANT NULL", col.TableScriptColumnSql);
        }
    }
}

