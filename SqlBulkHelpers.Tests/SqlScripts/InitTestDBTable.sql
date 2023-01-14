--DROP TABLE [dbo].[SqlBulkHelpersTestElements];
CREATE TABLE [dbo].[SqlBulkHelpersTestElements](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Key] [nvarchar](max) NULL,
	[Value] [nvarchar](max) NULL,
	CONSTRAINT [PK_SqlBulkHelpersTestElements] PRIMARY KEY CLUSTERED ([Id] ASC) 
		WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) 
ON [PRIMARY] 
TEXTIMAGE_ON [PRIMARY];
GO

--DROP TABLE [dbo].[SqlBulkHelpersTestElements_Child_NoIdentity];
CREATE TABLE [dbo].[SqlBulkHelpersTestElements_Child_NoIdentity](
	[ChildKey] [nvarchar](250) NOT NULL,
	[ParentId] [int] NOT NULL,
	[ChildValue] [nvarchar](max) NULL,
	CONSTRAINT [PK_SqlBulkHelpersTestElements_Child] PRIMARY KEY CLUSTERED ([ChildKey] ASC, [ParentId] ASC)
		WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
)
ON [PRIMARY] 
TEXTIMAGE_ON [PRIMARY];

ALTER TABLE [dbo].[SqlBulkHelpersTestElements_Child_NoIdentity] ADD FOREIGN KEY (ParentId) REFERENCES [dbo].[SqlBulkHelpersTestElements](Id);
GO

--DROP TABLE [dbo].[SqlBulkHelpersTestElements_WithFullTextIndex];
CREATE TABLE [dbo].[SqlBulkHelpersTestElements_WithFullTextIndex](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Key] [nvarchar](max) NULL,
	[Value] [nvarchar](max) NULL,
	CONSTRAINT [PK_SqlBulkHelpersTestElements_WithFullTextIndex] PRIMARY KEY CLUSTERED ([Id] ASC)
		WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

CREATE FULLTEXT CATALOG SearchCatalog AS DEFAULT;
CREATE FULLTEXT INDEX ON [dbo].[SqlBulkHelpersTestElements_WithFullTextIndex]([Key],[Value])   
   KEY INDEX [PK_SqlBulkHelpersTestElements_WithFullTextIndex] ON [SearchCatalog]
   WITH STOPLIST = SYSTEM;  
GO
