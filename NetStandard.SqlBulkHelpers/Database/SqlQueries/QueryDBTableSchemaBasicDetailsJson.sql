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
				ELSE (SELECT TOP (1) t.[name] FROM tempdb.sys.objects t WHERE t.[object_id] = OBJECT_ID(CONCAT(N'tempdb.[', @TableSchema, '].[', @TableName, ']')))
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
				IsIdentityColumn = CAST(COLUMNPROPERTY(t.ObjectId, COLUMN_NAME, 'IsIdentity') AS bit),
				CharacterMaxLength = CHARACTER_MAXIMUM_LENGTH,
				NumericPrecision = NUMERIC_PRECISION,
				NumericPrecisionRadix = NUMERIC_PRECISION_RADIX,
				NumericScale = NUMERIC_SCALE,
				DateTimePrecision = DATETIME_PRECISION
			FROM INFORMATION_SCHEMA.COLUMNS c
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