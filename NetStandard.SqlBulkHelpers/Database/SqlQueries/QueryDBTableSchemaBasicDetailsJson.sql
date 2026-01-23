	--NOTE: For Temp Table support all references to INFORMATION_SCHEMA must be replaced with tempdb.INFORMATION_SCHEMA
	--			and DB_NAME() must be changed to 'tempdb', otherwise we dynamically resolve the true Temp Table Name in the Cte...
	WITH TablesCte AS (
		SELECT TOP (1)
			TableSchema = t.[TABLE_SCHEMA], 
			TableName = t.[TABLE_NAME],
			TableCatalog = t.[TABLE_CATALOG],
			ObjectId = OBJECT_ID(CONCAT('[', t.TABLE_CATALOG, '].[', t.TABLE_SCHEMA, '].[', t.TABLE_NAME, ']'))
		FROM INFORMATION_SCHEMA.TABLES t
        WHERE 
            t.TABLE_SCHEMA = @TableSchema
			AND t.TABLE_CATALOG = DB_NAME()
			AND t.TABLE_NAME = CASE
				WHEN @IsTempTable = 0 THEN @TableName
				ELSE (SELECT TOP (1) t.[name] FROM tempdb.sys.objects t WHERE t.[object_id] = OBJECT_ID(CONCAT(N'tempdb.[', @TableSchema, '].[', @TableName, ']'))) COLLATE DATABASE_DEFAULT
			END
	)
	SELECT
		t.TableSchema, 
		t.TableName,
		[SchemaDetailLevel] = 'BasicDetails',
		[TableColumns] = (
			SELECT 
				SourceTableSchema = t.TableSchema,
				SourceTableName = t.TableName,
				OrdinalPosition = ORDINAL_POSITION,
				ColumnName = COLUMN_NAME,
				DataType = DATA_TYPE,
				IsNullableColumn = CAST(COALESCE(sc.is_nullable, 0) AS BIT),
				IsIdentityColumn = CAST(COALESCE(sc.is_identity, 0) AS BIT),
				IdentitySeedValue = ic.seed_value,
				IdentityIncrementValue = ic.increment_value,
				IsComputedColumn = CAST(COALESCE(sc.is_computed, 0) AS BIT),
				ComputedColumnDefinition = cc.[definition],
				IsPersistedColumn = CAST(COALESCE(cc.is_persisted, 0) AS BIT),
				IsRowGuidColumn = CAST(COALESCE(sc.is_rowguidcol, 0) AS BIT),
				-- Only return a value when the column's collation differs from the DB default.
				CharacterCollationName = CASE
					WHEN sc.collation_name IS NULL THEN NULL  -- non-character columns (or some computed columns)
					WHEN sc.collation_name = dbinfo.db_collation THEN NULL
					ELSE sc.collation_name
				END,
				CharacterMaxLength = CHARACTER_MAXIMUM_LENGTH,
				BinaryMaxLength = CHARACTER_OCTET_LENGTH,
				NumericPrecision = NUMERIC_PRECISION,
				NumericPrecisionRadix = NUMERIC_PRECISION_RADIX,
				NumericScale = NUMERIC_SCALE,
				DateTimePrecision = DATETIME_PRECISION
			FROM INFORMATION_SCHEMA.COLUMNS c
				OUTER APPLY (
					SELECT db_collation = CAST(DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS sysname)
				) AS dbinfo
				--NOTE: The only way to retrieve Computed Column Definition and whether is IS PERSISTED is by joining to the Sql Server sys catalog tables!
				LEFT JOIN sys.columns sc ON (sc.[object_id] = t.ObjectId AND sc.[name] = c.COLUMN_NAME)
				LEFT JOIN sys.computed_columns cc ON (cc.[object_id] = sc.[object_id] AND cc.column_id = sc.column_id)
				LEFT JOIN sys.identity_columns ic ON (ic.[object_id] = sc.[object_id] AND ic.column_id = sc.column_id)
			WHERE 
				c.TABLE_CATALOG = t.TableCatalog
				AND c.TABLE_SCHEMA = t.TableSchema 
				AND c.TABLE_NAME = t.TableName
			ORDER BY c.ORDINAL_POSITION
			FOR JSON PATH
		),
		[PrimaryKeyConstraint] = JSON_QUERY((
            SELECT TOP (1)
				SourceTableSchema = t.TableSchema,
				SourceTableName = t.TableName,
	            ConstraintName = c.CONSTRAINT_NAME,
	            ConstraintType = 'PrimaryKey',
	            [KeyColumns] = (
		            SELECT 
						OrdinalPosition = col.ORDINAL_POSITION,
						ColumnName = col.COLUMN_NAME
		            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE col
		            WHERE 
						col.TABLE_SCHEMA = c.TABLE_SCHEMA
						AND col.TABLE_NAME = c.TABLE_NAME 
						AND col.CONSTRAINT_SCHEMA = c.CONSTRAINT_SCHEMA
                        AND col.CONSTRAINT_NAME = c.CONSTRAINT_NAME
		            ORDER BY col.ORDINAL_POSITION
		            FOR JSON PATH
	            )
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
			WHERE
				c.TABLE_CATALOG = t.TableCatalog
				AND c.TABLE_SCHEMA = t.TableSchema 
				AND c.TABLE_NAME = t.TableName 
				AND c.CONSTRAINT_TYPE = 'PRIMARY KEY'
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ))
	FROM TablesCte t
	ORDER BY t.TableName
	FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
