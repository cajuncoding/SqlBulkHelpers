	WITH TablesCte AS (
		SELECT TOP (1)
			TableSchema = t.[TABLE_SCHEMA], 
			TableName = t.[TABLE_NAME],
			TableCatalog = t.[TABLE_CATALOG],
			ObjectId = OBJECT_ID('['+t.TABLE_SCHEMA+'].['+t.TABLE_NAME+']')
		FROM INFORMATION_SCHEMA.TABLES t
        WHERE 
            t.TABLE_SCHEMA = @TableSchema
            AND t.TABLE_NAME = @TableName
			AND t.TABLE_CATALOG = DB_NAME()
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