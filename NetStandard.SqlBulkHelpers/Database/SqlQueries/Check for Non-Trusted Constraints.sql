WITH FKeyCte AS (
	SELECT 
		FKeyConstraintName = QUOTENAME(fk.name),
		ObjectId = fk.[object_id],
		SourceTable = CONCAT(QUOTENAME(OBJECT_SCHEMA_NAME(fk.parent_object_id)), '.', QUOTENAME(OBJECT_NAME(fk.parent_object_id))),
			--'[' + object_schema_name(fk.parent_object_id) + '].[' + object_name(fk.parent_object_id) + ']',
		ReferenceTable = CONCAT(QUOTENAME(OBJECT_SCHEMA_NAME(fk.referenced_object_id)), '.', QUOTENAME(OBJECT_NAME(fk.referenced_object_id))),
		IsDisabled = is_disabled,
		IsNotTrusted = is_not_trusted
	FROM sys.foreign_keys fk
)
SELECT 
	fk.FKeyConstraintName,
	fk.SourceTable,
	fk.ReferenceTable,
    ReferenceColumns = STRING_AGG(c.Name, ', '),
	fk.IsDisabled, 
	fk.IsNotTrusted,
	DisableScript = CONCAT('ALTER TABLE ', fk.SourceTable, ' NOCHECK CONSTRAINT ', fk.FKeyConstraintName, ';'),
    EnableScript = CONCAT('ALTER TABLE ', fk.SourceTable, ' WITH CHECK CHECK CONSTRAINT ', fk.FKeyConstraintName, ';')
FROM FKeyCte fk
	JOIN sys.foreign_key_columns fkc ON (fkc.constraint_object_id = fk.ObjectId)
	JOIN sys.columns c ON (c.[object_id] = fkc.parent_object_id AND c.column_id = fkc.parent_column_id)
GROUP BY fk.FKeyConstraintName, fk.SourceTable, fk.ReferenceTable, fk.IsDisabled, fk.IsNotTrusted
