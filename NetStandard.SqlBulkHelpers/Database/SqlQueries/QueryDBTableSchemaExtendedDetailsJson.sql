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
		[SchemaDetailLevel] = 'ExtendedDetails',
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
        )),
		[ForeignKeyConstraints] = (
			-- DISTINCT is REQUIRED to Pull Reference Table up to Top Level of the Constraint!
			SELECT DISTINCT 
				SourceTableSchema = t.TableSchema,
				SourceTableName = t.TableName,
	            ConstraintName = c.CONSTRAINT_NAME,
	            ConstraintType = 'ForeignKey',
                ReferenceTableSchema = rcol.TABLE_SCHEMA,
                ReferenceTableName = rcol.TABLE_NAME,
                ReferentialMatchOption = rc.MATCH_OPTION,
                ReferentialUpdateRuleClause = rc.UPDATE_RULE,
                ReferentialDeleteRuleClause = rc.DELETE_RULE,
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
	            ),
                [ReferenceColumns] = (
		            SELECT 
						OrdinalPosition = col.ORDINAL_POSITION,
						ColumnName = col.COLUMN_NAME
		            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE col
                    WHERE 
	                    --FKeys MUST reference to the Unique Constraints or PKey Unique Constraints...
                        col.CONSTRAINT_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA
	                    AND col.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
		            ORDER BY col.ORDINAL_POSITION
		            FOR JSON PATH
                )
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
	            INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc ON (rc.CONSTRAINT_SCHEMA = c.CONSTRAINT_SCHEMA AND rc.CONSTRAINT_NAME = c.CONSTRAINT_NAME)
	            --FKeys MUST reference to the Unique Constraints or PKey Unique Constraints...
	            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE rcol ON (rcol.CONSTRAINT_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA AND rcol.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME)                            
			WHERE
				c.TABLE_CATALOG = t.TableCatalog
				AND c.TABLE_SCHEMA = t.TableSchema 
                AND c.TABLE_NAME = t.TableName
				AND c.CONSTRAINT_TYPE = 'FOREIGN KEY'
            FOR JSON PATH
        ),
		[ReferencingForeignKeyConstraints] = (
			SELECT DISTINCT
                SourceTableSchema = c.TABLE_SCHEMA,
                SourceTableName = c.TABLE_NAME,
	            ConstraintName = c.CONSTRAINT_NAME,
	            ConstraintType = 'ForeignKey'
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
	            INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc ON (rc.CONSTRAINT_SCHEMA = c.CONSTRAINT_SCHEMA AND rc.CONSTRAINT_NAME = c.CONSTRAINT_NAME)
	            ----FKeys MUST reference to the Unique Constraints or PKey Unique Constraints...
	            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE rcol ON (rcol.CONSTRAINT_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA AND rcol.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME)                            
			WHERE
				--Find all FKeys that reference the current Table...
				rcol.TABLE_CATALOG = t.TableCatalog
				AND rcol.TABLE_SCHEMA = t.TableSchema
                AND rcol.TABLE_NAME = t.TableName
				AND c.CONSTRAINT_TYPE = 'FOREIGN KEY'
			FOR JSON PATH
		),
        [ColumnDefaultConstraints] = (
			SELECT
				SourceTableSchema = t.TableSchema,
				SourceTableName = t.TableName,
				ConstraintName = dc.[name],
				[ColumnName] = col.[name],
				[Definition] = dc.[definition]
			FROM sys.default_constraints dc
				INNER JOIN sys.columns AS col ON (col.default_object_id = dc.[object_id])
			WHERE DC.[parent_object_id] = t.ObjectId
			FOR JSON PATH
		),
        [ColumnCheckConstraints] = (
            SELECT 
				SourceTableSchema = t.TableSchema,
				SourceTableName = t.TableName,
	            ConstraintName = c.CONSTRAINT_NAME, 
				[CheckClause] = (
					SELECT TOP (1) cc.CHECK_CLAUSE 
					FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc 
					WHERE 
						cc.CONSTRAINT_CATALOG = c.CONSTRAINT_CATALOG
						AND cc.CONSTRAINT_SCHEMA = c.CONSTRAINT_SCHEMA 
						AND cc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
				)
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
			WHERE 
				c.TABLE_CATALOG = t.TableCatalog
				AND c.TABLE_SCHEMA = t.TableSchema 
				AND c.TABLE_NAME = t.TableName
				AND c.CONSTRAINT_TYPE = 'CHECK'
            FOR JSON PATH
		),
        --NOTE: TableIndexes (in SQL Server) include Unique Constraints.
        [TableIndexes] = (
            SELECT 
				SourceTableSchema = t.TableSchema,
				SourceTableName = t.TableName,
	            IndexId = i.index_id, 
	            IndexName= i.[name],
	            IsUnique = i.is_unique,
	            IsUniqueConstraint = i.is_unique_constraint,
	            FilterDefinition = i.filter_definition,
	            [KeyColumns] = (
		            SELECT 
						OrdinalPosition = ROW_NUMBER() OVER(ORDER BY ic.index_column_id ASC),
			            ColumnName = c.[name], 
			            IsDescending = ic.is_descending_key
		            FROM sys.index_columns ic
			            INNER JOIN sys.columns c ON (c.[object_id] = ic.[object_id] and c.column_id = ic.column_id)
		            WHERE 
						ic.index_id = i.index_id 
						AND ic.[object_id] = i.[object_id] 
						AND key_ordinal > 0 -- KeyOrdinal > 0 are Key Columns
		            ORDER BY ic.index_column_id
		            FOR JSON PATH
	            ),
	            [IncludeColumns] = (
		            SELECT
						OrdinalPosition = ROW_NUMBER() OVER(ORDER BY ic.index_column_id ASC),
			            ColumnName = c.[name]
		            FROM sys.index_columns ic
			            INNER JOIN sys.columns c ON (c.[object_id] = ic.[object_id] and c.column_id = ic.column_id)
		            WHERE 
						ic.index_id = i.index_id 
						AND ic.[object_id] = i.[object_id] 
						AND key_ordinal = 0 -- KeyOrdinal == 0 are Include Columns
		            ORDER BY ic.index_column_id
		            FOR JSON PATH
	            )
            FROM sys.indexes i
	        WHERE 
				[type] = 2 -- Type 2 are NONCLUSTERED Table Indexes
				AND [object_id] = t.ObjectId
            FOR JSON PATH
        ),
		[FullTextIndex] = JSON_QUERY((
			SELECT TOP (1)
				SourceTableSchema = t.TableSchema,
				SourceTableName = t.TableName,
				FullTextCatalogName = cat.[name], 
				UniqueIndexName = i.[name],
				ChangeTrackingStateDescription = fti.[change_tracking_state_desc],
				StopListName = sl.[name],
				PropertyListName = pl.[name],
				IndexedColumns = (
					SELECT 
						OrdinalPosition = ROW_NUMBER() OVER(ORDER BY ic.column_id ASC),
						ColumnName = c.[name],
						LanguageId = ic.[language_id],
						StatisticalSemanticsEnabled = ic.[statistical_semantics],
						TypeColumnName = typec.[name]
					FROM 
						sys.fulltext_index_columns ic
			            INNER JOIN sys.columns c ON (c.[object_id] = ic.[object_id] and c.column_id = ic.column_id)
						LEFT JOIN sys.columns typec ON (typec.[object_id] = ic.[object_id] and typec.column_id = ic.type_column_id)
					WHERE c.[object_id] = fti.[object_id]
					FOR JSON PATH
				)
			FROM
				sys.fulltext_indexes fti
				INNER JOIN sys.fulltext_catalogs cat ON (fti.fulltext_catalog_id = cat.fulltext_catalog_id)
				INNER JOIN sys.indexes i ON (fti.unique_index_id = i.index_id AND fti.[object_id] = i.[object_id])
				LEFT JOIN sys.fulltext_stoplists sl ON (sl.stoplist_id = fti.stoplist_id)
				LEFT JOIN sys.registered_search_property_lists pl ON (pl.property_list_id = fti.property_list_id)
			WHERE fti.[object_id] = t.ObjectId
			FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
		))
	FROM TablesCte t
	ORDER BY t.TableName
	FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
