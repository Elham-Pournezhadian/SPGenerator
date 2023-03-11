
IF OBJECT_ID('dbo.GenerateDynamicProc','P') IS NOT NULL
    DROP PROCEDURE dbo.GenerateDynamicProc
GO



CREATE PROCEDURE dbo.GenerateDynamicProc @DatabaseName nvarchar(200)=NULL,
                                         @SchemaName nvarchar(200) = NULL,
                                         @TableName nvarchar(200) = NULL
                                                         
AS
BEGIN

DECLARE
	------ system default columns for log or proc info --------
	@CreationUserMatch nvarchar(500) = 'syscolumns.name LIKE ''%CreationUser%'' OR syscolumns.name LIKE ''%CreationBy%''',
	@CreationDateMatch nvarchar(500) = 'syscolumns.name LIKE ''%CreationDate%'' OR syscolumns.name LIKE ''%CreatedDate%''',
	@ModificationUserMatch nvarchar(500) = 'syscolumns.name LIKE ''%ModificationUser%'' OR syscolumns.name LIKE ''%ModifiedUser%''',
	@ModificationDateMatch nvarchar(500) = 'syscolumns.name LIKE ''%ModificationDate%'' OR syscolumns.name LIKE ''%ModifiedDate%''',

	@ManageTransaction bit=1,	--- XACT_ABORT ON
	@NoCount BIT = 1,			--- NOCOUNT	ON
	@DeadLockPrLow BIT = 1,		--- DEADLOCK_PRIORITY LOW
	@IsoSnapshot BIT = 1,		--- SET TRANSACTION ISOLATION LEVEL SNAPSHOT
	@GenerateDebugScriptForList bit = 1,
	@UnCommentExecForDebug BIT = 0 -- Sample Part

---======================  QUERY CODE ELEMENTS ======================----
    Declare @space			NVARCHAR(50) = REPLICATE(' ', 4) ;
	DECLARE @strBeginTran	NVARCHAR(100) = ' ^ ' + @space + 'BEGIN TRAN '
	DECLARE @strEndTran		NVARCHAR(100) = ' ^ ' + @space + 'COMMIT TRAN '
	DECLARE @strBeginTry	NVARCHAR(100) = ' ^ ' + @space + 'BEGIN TRY '
	DECLARE @strEndTry		NVARCHAR(100) = ' ^ ' + @space + 'END TRY '
	DECLARE @strEndCatch	nvarchar(100) = ' ^ ' + @space + 'END CATCH '
    DECLARE @strBegin		NVARCHAR(1000)= ' AS ' + ' ^ ' --+ 'BEGIN'
    DECLARE @strEnd			NVARCHAR(1000)=''
    DECLARE @strBeginpart	NVARCHAR(1000)=  @space + 'BEGIN '
    DECLARE @strEndpart		NVARCHAR(1000)=  @space + 'END '
	DECLARE @spaceForTrans	NVARCHAR(10)=''

    SET @strEnd = @strEnd + ' ^ ' + 'GO'
    SET @strEnd = @strEnd + ' ^ ' + ''

	SET @strEndTry = @strEndTry + ' ^ ' + @space + 'BEGIN CATCH '

    IF @NoCount=1
      Set @strBegin = @strBegin + ' ^ ' + @space + 'SET NOCOUNT ON '
	
	IF @DeadLockPrLow=1
	  SET @strBegin = @strBegin + ' ^ ' + @space + 'SET DEADLOCK_PRIORITY LOW; '   
    
	IF @ManageTransaction = 1 
    BEGIN
        Set @strBegin = @strBegin + ' ^ ' + @space + 'SET XACT_ABORT ON -- if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.'
      Set @strBegin = @strBegin + ' ^ ' + ''
      SET @spaceForTrans= @space;
    END
	
	
    IF @UnCommentExecForDebug = 0 Set @strEnd = @strEnd + ' ^ ' + '/*'
----------------------------------------------------------------------------


DECLARE @StatementList TABLE(id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,FullTableName nvarchar(1000),StatementType nvarchar(100),Statement nvarchar(max))  
DECLARE @FirstParameters nvarchar(400)='',@FirstParametersForExec nvarchar(400)=''


IF LEN(LTRIM(TRIM(@DatabaseName)))=0 OR @DatabaseName IS NULL
SET @DatabaseName=DB_NAME()

IF LEN(LTRIM(TRIM(@SchemaName)))=0
SET @SchemaName=NULL

IF LEN(LTRIM(TRIM(@TableName)))=0
SET @TableName=NULL

IF NOT(LEN(@CreationUserMatch)>0)  
   SET @CreationUserMatch = 'syscolumns.name = ''BIDON12345678917071979'''

IF NOT(LEN(@CreationDateMatch)>0)  
   SET @CreationDateMatch = 'syscolumns.name = ''BIDON12345678917071979'''

IF NOT(LEN(@ModificationUserMatch)>0)  
   SET @ModificationUserMatch = 'syscolumns.name = ''BIDON12345678917071979'''

IF NOT(LEN(@ModificationDateMatch)>0)  
   SET @ModificationDateMatch = 'syscolumns.name = ''BIDON12345678917071979'''




DECLARE @strSpText nVarchar(max) ='USE [' + @DatabaseName + ']'

IF @DatabaseName != DB_NAME()
	INSERT INTO @StatementList (FullTableName,StatementType,Statement) VALUES ('Common','Set current database',@strSPText)

DECLARE @sqlstatementForTables nvarchar(max) = -- Not test with USE [' + @DatabaseName + '] ISSUE ON Table iDENTITY.Identity: 'Could not complete cursor operation because the set options have changed since the cursor was declared
      N'
      DECLARE Tables_cursor CURSOR FOR
		SELECT TABLE_SCHEMA,TABLE_NAME
		FROM [' + @DatabaseName + '].INFORMATION_SCHEMA.TABLES
		WHERE TABLE_TYPE=''BASE TABLE''
			AND (TABLE_SCHEMA = @pSchemaName OR @pSchemaName IS NULL)
			AND (Table_Name = @pTableName OR @pTableName IS NULL)'


EXEC sp_executesql @sqlstatementForTables, N'@pSchemaName  nvarchar(200),@pTableName  nvarchar(200)', @pSchemaName=@SchemaName, @pTableName=@TableName;

OPEN Tables_cursor
	DECLARE @CurrentSchemaName nvarchar(100),@CurrentFullTableName nvarchar(1000),@CurrentTableName nVarchar(1000),
			@DropStatement nvarchar(max) = ''
Fetch next
from Tables_cursor
INTO @CurrentSchemaName,@CurrentTableName
WHILE @@FETCH_STATUS = 0
BEGIN

    SET @CurrentFullTableName='['+@CurrentSchemaName+'].['+@CurrentTableName+']';


-----------------------------------------------------------------------------------------------------------
---========================================= OBJECTS VARIABLES ====================================--------
    Declare @dbName nVarchar(50)
    Declare @insertSPName nVarchar(4000), @updateSPName nVarchar(4000), @deleteSPName nVarchar(4000), @ValidateSPName nVarchar(4000)--, @ReadSPName nVarchar(50) ;
    Declare @ColumnParametersInsert nVarchar(max), @ColumnDefForInsert nVarchar(max),@ColumnInValueForInsert nVarchar(max),
			@OutputId nVarchar(max) ='@Id BIGINT OUTPUT ',
            @ColumnParametersInsertForExec nvarchar(max)
    Declare @tableCols nVarchar(max), @ColumnParametersUpdate nVarchar(max),@ColumnParametersUpdateForExec nVarchar(max);
    Declare @colName nVarchar(max) ;
    Declare @DataType nvarchar(200),@colVariable nVarchar(200),@colVariableProc nVarchar(200);
    Declare @colParameter nVarchar(max) ;
    Declare @colAllowNull nvarchar(15), @colIsPrimaryKey INT,@ColIsIdentityAutoIncrement INT,@ColLength INT,@ColIsComputed INT,@ColMatchCreationUser INT,@ColMatchCreationDate INT,@ColMatchModificationUser INT,@ColMatchModificationDate INT;

    Declare @updCols nVarchar(max);
    Declare @ColumnParametersDelete nVarchar(max),@ColumnParametersDeleteForExec nVarchar(max),
            @LastPrimaryKey nvarchar(max),@NbPrimaryKey INT=0,@ColNumber int=0
    Declare @whereCols nVarchar(2000);
    DECLARE @SetVariablesForExec nvarchar(max)='',@SetVariablesForExecUpdate nvarchar(max)='', @SetVariablesForExecDelete nvarchar(max)=''
	DECLARE @FunctionPruneColumnName NVARCHAR(MAX)=''
	DECLARE @ValidationProcedureCall NVARCHAR(MAX)=''

        Set @insertSPName = '['+@CurrentSchemaName+'].[usp_' + @CurrentTableName +'Insert]' ;
        Set @updateSPName = '['+@CurrentSchemaName+'].[usp_' + @CurrentTableName +'Update]' ;
        Set @deleteSPName = '['+@CurrentSchemaName+'].[usp_' + @CurrentTableName +'Delete]' ;
        set @ValidateSPName =  '['+@CurrentSchemaName+'].[usp_' + @CurrentTableName +'Validate]' ;

		SET @DropStatement = @DropStatement+ '
		DROP PROCEDURE ' + @insertSPName + '
		DROP PROCEDURE ' + @updateSPName + '
		DROP PROCEDURE ' + @deleteSPName +'
		DROP PROCEDURE ' + @ValidateSPName

-----------------------------------------------------------------------------------------------------------------	
------======================================== ERROR HANDLING ================================================---
-- in this project there are procedures which were created to raise error with specific codes and messages
-- this part can be ommited to get customized 

	DECLARE @ErrorControlInputParams NVARCHAR(MAX)=''
	DECLARE @ErrorThrowCondition NVARCHAR(MAX)=''
	DECLARE @ErrorThrowException NVARCHAR(MAX)=''

	DECLARE @ErrorThrowExceptionInsert	NVARCHAR(MAX)
	DECLARE @ErrorThrowExceptionUpdate	NVARCHAR(MAX)
	DECLARE @ErrorThrowExceptionDelete	NVARCHAR(MAX)
	
	SET @ErrorControlInputParams = @ErrorControlInputParams + ' ^ ' + @space + 'DECLARE @OutputMessage NVARCHAR(1000); '
	SET @ErrorControlInputParams = @ErrorControlInputParams + ' ^ ' + @space + 'DECLARE @ErrorCode INT = NULL; '
	SET @ErrorControlInputParams = @ErrorControlInputParams + ' ^ ' + @space + 'DECLARE @MessageType INT = 0; '
	
	SET @ErrorThrowCondition = @ErrorThrowCondition + ' ^ ' + @space + 'IF @ErrorCode IS NOT NULL THROW 50003, @ErrorCode, 10; '
    Set @ErrorThrowException = @ErrorThrowException + ' ^ ' + ''
	SET @ErrorThrowException = @ErrorThrowException + ' ^ ' + @space + 'IF (@@TRANCOUNT > 0) '
    Set @ErrorThrowException = @ErrorThrowException + ' ^ ' + ''
	SET @ErrorThrowException = @ErrorThrowException + ' ^ ' + @space + @space + ' ROLLBACK TRAN '
    Set @ErrorThrowException = @ErrorThrowException + ' ^ ' + ''
	SET @ErrorThrowException = @ErrorThrowException + ' ^ ' + @space + 'SELECT @OutputMessage = IIF(@OutputMessage IS NULL, ERROR_MESSAGE(), @OutputMessage); '
    Set @ErrorThrowException = @ErrorThrowException + ' ^ ' + ''
	SET @ErrorThrowException = @ErrorThrowException + ' ^ ' + @space + 'SELECT @MessageType = IIF(@ErrorCode > 0, 1, 0); '
    Set @ErrorThrowException = @ErrorThrowException + ' ^ ' + ''
	SET @ErrorThrowException = @ErrorThrowException + ' ^ ' + @space + 'SELECT @ErrorCode = IIF(@ErrorCode > 0, @ErrorCode, ERROR_NUMBER()); '
    Set @ErrorThrowException = @ErrorThrowException + ' ^ ' + ''
	SET @ErrorThrowException = @ErrorThrowException + ' ^ ' + @space + 'EXEC [dbo].[usp_throwException] '
	SET @ErrorThrowException = @ErrorThrowException + ' ^ ' + @space + @space + ' @FriendlyMessage = @OutputMessage, -- nvarchar(max) '
	SET @ErrorThrowException = @ErrorThrowException + ' ^ ' + @space + @space + ' @MessageNumber = @ErrorCode,       -- int '
	SET @ErrorThrowException = @ErrorThrowException + ' ^ ' + @space + @space + ' @MessageType = @MessageType,       -- tinyint '
	SET @ErrorThrowException = @ErrorThrowException + ' ^ ' + @space + @space + ' @Lang = @LangTypeCode,             -- int '
    
	SET @ErrorThrowExceptionInsert = @ErrorThrowException + ' ^ ' + @space + @space + ' @StoreProcedureName = '''+@insertSPName+''''
	SET @ErrorThrowExceptionUpdate = @ErrorThrowException + ' ^ ' + @space + @space + ' @StoreProcedureName = '''+@updateSPName+''''
	SET @ErrorThrowExceptionDelete = @ErrorThrowException + ' ^ ' + @space + @space + ' @StoreProcedureName = '''+@deleteSPName+''''
-----------------------------------------------------------------------------------------------------------------	
-----------------------------------------------------------------------------------------------------------------	

    Set @ColumnParametersInsert = @FirstParameters ;
    SET @ColumnParametersInsertForExec = @FirstParametersForExec
    Set @ColumnParametersUpdate=@FirstParameters
    SET @ColumnParametersUpdateForExec=@FirstParametersForExec
    Set @ColumnParametersDelete = @FirstParameters ;
    SET @ColumnParametersDeleteForExec = @FirstParametersForExec ;

    Set @ColumnDefForInsert = '' ;
    Set @ColumnInValueForInsert = '' ;
    Set @strSPText = '' ;
    Set @tableCols = '' ;
    Set @updCols = '' ;

    Set @whereCols = '' ;

    SET NOCOUNT ON

-----------------------------------------------------------------------------------------------------------------	

    CREATE TABLE #tmp_Structure (colid int,ColumnName nvarchar(max), 
                                 ColumnVariable nvarchar(max),
                                 DataType nvarchar(max), 
                                 ColumnParameter nvarchar(max), 
                                 AllowNull int, 
                                 IsPrimaryKey int, 
                                 IsIdentityAutoIncrement int, 
                                 ColLength int, 
                                 IsIsComputedColumn int,
                                 ColMatchCreationUser int,ColMatchCreationDate int,
                                 ColMatchModificationUser INT,ColMatchModificationDate INT)

    DECLARE @sqlstatementForColumns nvarchar(max) = 
      N'USE [' + @DatabaseName + ']
      SELECT distinct
           --sysobjects.name as ''Table'',
           syscolumns.colid ,
           ''['' + syscolumns.name + '']'' as ''ColumnName'',
           ''@''+syscolumns.name as ''ColumnVariable'',           
           systypes.name +
           Case When systypes.xusertype in (165,167,175,231,239 ) Then ''('' + Convert(varchar(10),Case When syscolumns.length=-1 Then ''max'' else CAST(syscolumns.length AS nvarchar(10)) end) +'')'' Else '''' end as ''DataType'' ,
           systypes.name +  Case When systypes.xusertype in (165,167,175,231,239 ) Then ''('' + Convert(varchar(10),Case When syscolumns.length=-1 Then ''max'' else CAST(syscolumns.length AS nvarchar(10)) end) +'')'' Else '''' end as ''ColumnParameter'',
           COLUMNPROPERTY(OBJECT_ID(@pFullTableName),syscolumns.name,''AllowsNull'') AS AllowNull,
           /*CASE WHEN syscolumns.name IN (SELECT c.name AS ColumnName
                                             FROM [' + @DatabaseName + '].sys.columns AS c
                                                  INNER JOIN [' + @DatabaseName + '].sys.tables AS t ON t.[object_id] = c.[object_id]
                                            where c.is_identity = 1
                                              and t.name=@CurrentTableName) THEN ''1'' ELSE  ''0'' END IsPrimaryKey,
           */
           (SELECT COUNT(*) 
                    FROM [' + @DatabaseName + '].INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab, 
                         [' + @DatabaseName + '].INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col 
                   WHERE Col.Constraint_Name = Tab.Constraint_Name
                    AND Col.Table_Name = Tab.Table_Name
                    AND Constraint_Type = ''PRIMARY KEY''
                    AND Col.Table_Name = @pTableName
                    AND Tab.TABLE_SCHEMA=@pSchemaName
                    AND Col.Column_Name = syscolumns.name
                        ) AS IsPrimaryKey, 
           SC.is_identity AS IsIdentityAutoIncrement,
           syscolumns.length,
           (SELECT COUNT(*)
              FROM sys.computed_columns
             WHERE computed_columns.object_id=sysobjects.id
               AND computed_columns.Name=syscolumns.name) AS IsComputedColumn,
            CASE WHEN ' + @CreationUserMatch +' THEN 1 ELSE 0 END AS ColMatchCreationUser,
            CASE WHEN ' + @CreationDateMatch +' THEN 1 ELSE 0 END AS ColMatchCreationDate,
            CASE WHEN ' + @ModificationUserMatch +' THEN 1 ELSE 0 END AS ColMatchModificationUser,
            CASE WHEN ' + @ModificationDateMatch +' THEN 1 ELSE 0 END AS ColMatchModificationDate
    FROM sysobjects 
         LEFT JOIN syscolumns ON syscolumns.id=sysobjects.id
         LEFT JOIN systypes ON systypes.xusertype=syscolumns.xusertype
         LEFT JOIN sys.columns SC ON SC.object_id = sysobjects.id
                           AND SC.name=syscolumns.name
    Where sysobjects.xtype = ''u''
      and sysobjects.id = OBJECT_ID(@pFullTableName)
    Order by syscolumns.colid'

     --PRINT @sqlstatementForColumns
     --EXEC loopbackServerForDebug.[K2FranceDebugDB].dbo.K2FranceDebug '@sqlstatementForColumns',@sqlstatementForColumns

      INSERT INTO #tmp_Structure
      exec sp_executesql @sqlstatementForColumns, N'@pSchemaName  nvarchar(200),@pTableName  nvarchar(200),@pFullTableName nvarchar(1000)', @pSchemaName=@CurrentSchemaName, @pTableName=@CurrentTableName,@pFullTableName=@CurrentFullTableName;


      --SELECT * FROM #tmp_Structure

    /* Read the table structure and populate variables*/
    DECLARE SpText_Cursor CURSOR FOR
     SELECT ColumnName, ColumnVariable, UPPER(DataType), ColumnParameter, AllowNull, IsPrimaryKey, IsIdentityAutoIncrement,ColLength, IsIsComputedColumn,ColMatchCreationUser,ColMatchCreationDate,ColMatchModificationUser,ColMatchModificationDate
       FROM #tmp_Structure
	  -- WHERE ColumnName NOT LIKE '%RowVersion%'
    OPEN SpText_Cursor

    FETCH NEXT FROM SpText_Cursor INTO @colName, @colVariable,  @DataType, @colParameter, @colAllowNull,@colIsPrimaryKey, @ColIsIdentityAutoIncrement,@ColLength, @ColIsComputed,@ColMatchCreationUser,@ColMatchCreationDate,@ColMatchModificationUser,@ColMatchModificationDate
    WHILE @@FETCH_STATUS = 0
    BEGIN
       SET @ColNumber=@ColNumber+1

       SET @SetVariablesForExec = @SetVariablesForExec  + CASE WHEN @colAllowNull =1 THEN '' 
                                                         ELSE  CASE WHEN @DataType  IN ('datetime','datetime2','smalldatetime','date') AND @SetVariablesForExec NOT LIKE '%@Date%' THEN ' ^ ' + 'DECLARE @Date datetime =GetDate()'  
                                                                            WHEN @DataType  IN ('uniqueidentifier') AND @SetVariablesForExec NOT LIKE '%@GuidTest%' THEN ' ^ ' + 'DECLARE @TheGuid uniqueidentifier  =NEWID()'                                                                             
                                                                            ELSE '' 
                                                                       END
                                                         END

       --RegEx to keep only alphanumeric characters:
       DECLARE @MatchExpression nvarchar(20) =  '%[^a-z0-9]%',@DateTypeWithoutSpecialCharacters nvarchar(100)=@DataType;

       WHILE PatIndex(@MatchExpression, @DateTypeWithoutSpecialCharacters) > 0
        SET @DateTypeWithoutSpecialCharacters = Stuff(@DateTypeWithoutSpecialCharacters, PatIndex(@MatchExpression, @DateTypeWithoutSpecialCharacters), 1, '')


       --Remove Special characters (like space...) for variable name
		WHILE PatIndex(@MatchExpression, @colVariable) > 0
			SET @colVariable = Stuff(@colVariable, PatIndex(@MatchExpression, @colVariable), 1, '')
       
	   SET @colVariableProc = '@p'+ @colVariable 
       SET @colVariable = '@'+ @colVariable 

       SET @colParameter = @colVariable + ' ' + @colParameter 

       DECLARE @AffectationForExec nvarchar(max)=@colVariable + CASE WHEN @colAllowNull =1 THEN ' = NULL' 
                     ELSE ' = ' +  CASE WHEN @DataType IN ('Text','sysname') OR @DataType LIKE '%char%'  THEN '''' + SUBSTRING ( CAST(ABS(@ColLength) AS nvarchar(10)) + 'TEST' + @DateTypeWithoutSpecialCharacters,0,CASE WHEN @ColLength < 0 THEN 1000 WHEN @DataType LIKE 'nchar%' THEN @ColLength/2+1 ELSE @ColLength END) + ''''
                                        WHEN @DataType  IN ('int','numeric','bigint','tinyint') THEN CAST(@ColNumber AS nvarchar(10))   
                                        WHEN @DataType  IN ('bit') THEN '0'   
                                        WHEN @DataType  IN ('float') THEN CAST(@ColNumber AS nvarchar(10))   +  '.' + CAST(@ColNumber+1 AS nvarchar(10)) 
                                        WHEN @DataType  IN ('datetime','datetime2','smalldatetime','date') THEN '@Date'  
                                        WHEN @DataType  IN ('uniqueidentifier') THEN '@TheGuid'  
                                        WHEN @DataType  IN ('xml') THEN '''<testXML><value name="test">' + CAST(@ColNumber AS nvarchar(10)) + '</value></testXML>''' 
                                        ELSE '''1''--Currently Not managed' 
                                   END
                END +  ', --Type ' + @DataType  + ' ^ ' + @space 

       IF @ColIsIdentityAutoIncrement = 0 AND @ColIsComputed = 0 
       BEGIN
          IF @ColMatchModificationUser = 0  AND @ColMatchModificationDate = 0
            Set @ColumnDefForInsert = @ColumnDefForInsert + @colName+ ',' + ' ^ ' + @space + @space + @spaceForTrans ;


          IF @ColMatchCreationUser= 0 AND @ColMatchCreationDate = 0 AND @ColMatchModificationUser = 0  AND @ColMatchModificationDate = 0
          BEGIN
            Set @ColumnParametersInsert = @ColumnParametersInsert + @colParameter + CASE WHEN @colAllowNull =1  THEN ' = NULL' ELSE '' END +  ','  + ' ^ ' + @space ;
            SET @ColumnParametersInsertForExec = @ColumnParametersInsertForExec + @AffectationForExec

          END 
          IF @ColMatchCreationUser= 1 
			SET @ColumnInValueForInsert = @ColumnInValueForInsert + 'SYSTEM_USER'
          ELSE
          BEGIN
            IF @ColMatchCreationDate= 1
              Set @ColumnInValueForInsert = @ColumnInValueForInsert + 'GETDATE()'  
            ELSE
              IF @ColMatchModificationUser = 0  AND @ColMatchModificationDate = 0
                Set @ColumnInValueForInsert = @ColumnInValueForInsert + @colVariable
          END

          IF @ColMatchCreationUser= 1 OR @ColMatchCreationDate= 1 OR @ColMatchModificationUser = 0  AND @ColMatchModificationDate = 0
		   SET @ColumnInValueForInsert =@ColumnInValueForInsert + ',' + ' ^ ' + @space + @space+ @spaceForTrans


          Set @tableCols = @tableCols + @colName + ',' ;


          IF @ColMatchModificationUser = 1
          BEGIN
               Set @updCols = @updCols + @colName + ' = SYSTEM_USER'; 
          END 
          ELSE
          BEGIN
            IF @ColMatchModificationDate = 1
               Set @updCols = @updCols + @colName + ' = GETDATE()';     
            ELSE
              IF @ColMatchCreationUser=0 AND @ColMatchCreationDate=0
              Set @updCols = @updCols + @colName + ' = ' + @colVariable;     
          END   

          IF @ColMatchModificationUser = 1 OR @ColMatchModificationDate = 1 OR @ColMatchCreationUser=0 AND @ColMatchCreationDate=0
        SET @updCols =@updCols + ',' + ' ^ ' + @space + @space+ '   ' + @spaceForTrans


       END


       IF @ColIsIdentityAutoIncrement = 1 AND @DataType='int'
          BEGIN
            SET @SetVariablesForExecUpdate =   ' ^ '+'DECLARE @PrimaryKeyValue INT= (SELECT MIN(' + @colName + ') FROM ' + @CurrentFullTableName + ')'
            SET @AffectationForExec = @colVariable  + '= @PrimaryKeyValue, --Type ' + UPPER(@DataType)  + ' ^ ' + @space 
          END 

       IF @ColIsComputed = 0 AND @ColMatchCreationUser=0 AND @ColMatchCreationDate=0 AND @ColMatchModificationUser=0 AND @ColMatchModificationDate=0
       BEGIN
         Set @ColumnParametersUpdate = @ColumnParametersUpdate + @colParameter + ',' + ' ^ ' + @space ;
         SET @ColumnParametersUpdateForExec = @ColumnParametersUpdateForExec + @AffectationForExec
       END

       IF @colIsPrimaryKey= 1
       BEGIN
          IF @ColIsIdentityAutoIncrement = 1 AND @DataType='int'
          BEGIN
            SET @SetVariablesForExecDelete =   ' ^ '+'DECLARE @PrimaryKeyValue INT= (SELECT MAX(' + @colName + ') FROM ' + @CurrentFullTableName + ')'
          END 

         SET @ColumnParametersDelete = @ColumnParametersDelete + @colParameter +', ' + ' ^ ' + @space ;

         SET @ColumnParametersDeleteForExec = @ColumnParametersDeleteForExec + @AffectationForExec
         SET @whereCols = @whereCols + @colName + ' = ' + @colVariable + ' AND ' ;
         SET @NbPrimaryKey = @NbPrimaryKey +1
         SET @LastPrimaryKey = @colName
       END
    FETCH NEXT FROM SpText_Cursor INTO @colName, @colVariable, @DataType,@colParameter, @colAllowNull,@colIsPrimaryKey,@ColIsIdentityAutoIncrement,@ColLength,@ColIsComputed,@ColMatchCreationUser,@ColMatchCreationDate,@ColMatchModificationUser,@ColMatchModificationDate
    END
    CLOSE SpText_Cursor
    DEALLOCATE SpText_Cursor

    IF @ColumnDefForInsert IS NULL
      RAISERROR('@ColumnDefForInsert IS NULL',16,1)
    IF @ColumnParametersInsert IS NULL
      RAISERROR('@ColumnParametersInsert IS NULL',16,1)
    IF @ColumnParametersInsertForExec IS NULL
      RAISERROR('@ColumnParametersInsertForExec IS NULL',16,1)
    IF @ColumnInValueForInsert IS NULL
      RAISERROR('@ColumnInValueForInsert IS NULL',16,1)
    IF @tableCols IS NULL
      RAISERROR('@tableCols IS NULL',16,1)
    IF @updCols IS NULL
      RAISERROR('@updCols IS NULL',16,1)
    IF @ColumnParametersDelete IS NULL
      RAISERROR('@ColumnParametersDelete IS NULL',16,1)
    IF @whereCols IS NULL
      RAISERROR('@whereCols IS NULL',16,1)
-----------------------------------------------------------------------------------------------------------------	

    DECLARE @LastPosOfComma INT

    If (LEN(@ColumnParametersUpdate)>0)
    BEGIN
      Set @ColumnParametersUpdate = LEFT(@ColumnParametersUpdate,LEN(@ColumnParametersUpdate)-2) ;
      SET @LastPosOfComma = LEN(@ColumnParametersUpdateForExec) - CHARINDEX(' ,',REVERSE(@ColumnParametersUpdateForExec))
      SET @ColumnParametersUpdateForExec = LEFT(@ColumnParametersUpdateForExec,@LastPosOfComma+4) + SUBSTRING(@ColumnParametersUpdateForExec,@LastPosOfComma+5,40000);
	  SET @ColumnParametersUpdate = @ColumnParametersUpdate + ' ^ ' + @space + '@LangTypeCode  TINYINT = 1, '
	  SET @ColumnParametersUpdate = @ColumnParametersUpdate + ' ^ ' + @space + '@UserId  BIGINT = NULL '
    END

If (LEN(@ColumnParametersInsert)>0)
    Begin

      Set @ColumnParametersInsert = LEFT(@ColumnParametersInsert,LEN(@ColumnParametersInsert)-2) ;
      SET @LastPosOfComma = LEN(@ColumnParametersInsertForExec) - CHARINDEX(' ,',REVERSE(@ColumnParametersInsertForExec))
      SET @ColumnParametersInsertForExec = LEFT(@ColumnParametersInsertForExec,@LastPosOfComma+4) + SUBSTRING(@ColumnParametersInsertForExec,@LastPosOfComma+5,40000);
	  SET @ColumnParametersInsert = @ColumnParametersInsert + ' ^ ' + @space + '@LangTypeCode  TINYINT = 1, '
	  SET @ColumnParametersInsert = @ColumnParametersInsert + ' ^ ' + @space + '@UserId  BIGINT = NULL, '

      Set @ColumnParametersDelete = LEFT(@ColumnParametersDelete,LEN(@ColumnParametersDelete)-3) ;
      SET @LastPosOfComma = LEN(@ColumnParametersDeleteForExec) - CHARINDEX(' ,',REVERSE(@ColumnParametersDeleteForExec))
      SET @ColumnParametersDeleteForExec = LEFT(@ColumnParametersDeleteForExec,@LastPosOfComma+3) + SUBSTRING(@ColumnParametersDeleteForExec,@LastPosOfComma+5,40000);
	  SET @ColumnParametersDelete = @ColumnParametersDelete + ' ^ ' + @space + '@CompanyId    BIGINT  = NULL, '
	  SET @ColumnParametersDelete = @ColumnParametersDelete + ' ^ ' + @space + '@BranchId     BIGINT  = NULL, '
	  SET @ColumnParametersDelete = @ColumnParametersDelete + ' ^ ' + @space + '@UserId       BIGINT  = NULL, '
	  SET @ColumnParametersDelete = @ColumnParametersDelete + ' ^ ' + @space + '@LangTypeCode TINYINT = NULL '


      IF LEN(@ColumnInValueForInsert)>0
        Set @ColumnInValueForInsert = LEFT(@ColumnInValueForInsert,LEN(@ColumnInValueForInsert)-3) ;
      IF LEN(@ColumnDefForInsert)>0
        Set @ColumnDefForInsert = LEFT(@ColumnDefForInsert,LEN(@ColumnDefForInsert)-3) ;
      IF LEN(@tableCols)>0
        Set @tableCols = LEFT(@tableCols,LEN(@tableCols)-1) ;
      IF LEN(@updCols)>0
        Set @updCols = LEFT(@updCols,LEN(@updCols)-3) ;

    END


	IF CHARINDEX('@UniqueName ',@ColumnParametersInsert)>0
		SET @FunctionPruneColumnName = 'SET @UniqueName =(SELECT [dbo].[ufn_RemoveNonCharacters](@Name));'

	SET @ValidationProcedureCall = @ValidationProcedureCall + ' ^ ' + @space + 'EXEC [' + @CurrentSchemaName +'].[usp_'+ @CurrentTableName +'Validate] '
	SET @ValidationProcedureCall = @ValidationProcedureCall + ' ^ ' + @space + @space + '@RecordId = @Id, '

	DECLARE @ValidationProcedureCallInsertUpdate NVARCHAR(MAX) =''
	IF CHARINDEX('@UniqueName ',@ColumnParametersInsert)>0 AND CHARINDEX('@Number ',@ColumnParametersInsert)>0 
	BEGIN
		SET @ValidationProcedureCallInsertUpdate = @ValidationProcedureCallInsertUpdate + ' ^ ' + @space + @space + '@CreatedOn = @CreatedOn, '
		SET @ValidationProcedureCallInsertUpdate = @ValidationProcedureCallInsertUpdate + ' ^ ' + @space + @space + '@Number = @Number, '
		SET @ValidationProcedureCallInsertUpdate = @ValidationProcedureCallInsertUpdate + ' ^ ' + @space + @space + '@UniqueName = @UniqueName, '
	END
	SET @ValidationProcedureCall = @ValidationProcedureCall + ' ^ ' + @space + @space + '@CompanyId = @CompanyId, '
	SET @ValidationProcedureCall = @ValidationProcedureCall + ' ^ ' + @space + @space + '@ErrorCode = @ErrorCode OUTPUT, '
	SET @ValidationProcedureCall = @ValidationProcedureCall + ' ^ ' + @space + @space + '@Message = @OutputMessage OUTPUT, '
	SET @ValidationProcedureCall = @ValidationProcedureCall + ' ^ ' + @space + @space + '@LangTypeCode = @LangTypeCode, '

	
	SET @ColumnParametersInsertForExec = @ColumnParametersInsertForExec + '@LangTypeCode = 1,'
	SET @ColumnParametersInsertForExec = @ColumnParametersInsertForExec + ' ^ '  + @space + '@UserId = NULL,'
	SET @ColumnParametersInsertForExec = @ColumnParametersInsertForExec + ' ^ '  + @space + '@Id = 0;'

	SET @ColumnParametersUpdateForExec = @ColumnParametersUpdateForExec + '@LangTypeCode = 1,'
	SET @ColumnParametersUpdateForExec = @ColumnParametersUpdateForExec + ' ^ '  + @space + '@UserId = NULL,'
	SET @ColumnParametersUpdateForExec = @ColumnParametersUpdateForExec + ' ^ '  + @space + '@Id = 0;'

	DECLARE @ValidationProcedureInsert NVARCHAR(MAX)
	SET @ValidationProcedureInsert = @ValidationProcedureCall + @ValidationProcedureCallInsertUpdate
	SET @ValidationProcedureInsert = @ValidationProcedureInsert + ' ^ ' + @space + @space + '@Operation = 1; '
	DECLARE @ValidationProcedureUpdate NVARCHAR(MAX)
	SET @ValidationProcedureUpdate = @ValidationProcedureCall + @ValidationProcedureCallInsertUpdate
	SET @ValidationProcedureUpdate = @ValidationProcedureUpdate + ' ^ ' + @space + @space + '@Operation = 2; '
	DECLARE @ValidationProcedureDelete NVARCHAR(MAX)
	SET @ValidationProcedureDelete = @ValidationProcedureCall + ' ^ ' + @space + @space + '@Operation = 3; '

    If (LEN(@whereCols)>0)
      Set @whereCols = 'WHERE ' + LEFT(@whereCols,LEN(@whereCols)-4) ;
    ELSE
       Set @whereCols = 'WHERE 1=0 --Too dangerous to do update or delete on all the table'

-----------------------------------------------------------------------------------------------------------------	
------===================================== Validate SP Template =============================================---
-- Validatation procedures which control specific conditiones that has to be checked before each CRUD operation.
-- Validation procedures get called in other CRUD templates with related @Operation variable.

	DECLARE @ColumnParametersValidate NVARCHAR(MAX) =''
	
	DECLARE @SPValidateIFmodeInsert	NVARCHAR(1000) = @space + 'IF @Operation = 1'	-- in case of insert
	DECLARE @SPValidateIFmodeUpdate	NVARCHAR(1000) = @space + 'IF @Operation = 2'	-- in case of update
	DECLARE @SPValidateIFmodeDelete	NVARCHAR(1000) = @space + 'IF @Operation = 3'	-- in case of delete
	
	DECLARE @SPValidateComment	NVARCHAR(1000) = @space + ' ---	Your Validation Code Here	---'
	DECLARE @SPValidateDelete	NVARCHAR(1000)  =''
	
	SET @ColumnParametersValidate = @ColumnParametersValidate + ' ^ ' + @space + '@RecordId		BIGINT = NULL,'
	SET @ColumnParametersValidate = @ColumnParametersValidate + ' ^ ' + @space + '@CompanyId	BIGINT = NULL,'
	SET @ColumnParametersValidate = @ColumnParametersValidate + ' ^ ' + @space + '@CreatedOn	DATETIME = NULL,'
	SET @ColumnParametersValidate = @ColumnParametersValidate + ' ^ ' + @space + '@ErrorCode	INT = NULL OUTPUT,'
	SET @ColumnParametersValidate = @ColumnParametersValidate + ' ^ ' + @space + '@Message		NVARCHAR(MAX) = NULL OUTPUT,'
	SET @ColumnParametersValidate = @ColumnParametersValidate + ' ^ ' + @space + '@Operation	TINYINT, -- 1- Insert  2- Update  3- Delete'
	SET @ColumnParametersValidate = @ColumnParametersValidate + ' ^ ' + @space + '@LangTypeCode	TINYINT = NULL'


	SET @SPValidateDelete = @SPValidateDelete + ' ^ ' + @space + @space + ' IF NOT EXISTS '
	SET @SPValidateDelete = @SPValidateDelete + ' ^ ' + @space + @space + ' ( '
	SET @SPValidateDelete = @SPValidateDelete + ' ^ ' + @space + @space + @space + ' SELECT 1 '
	SET @SPValidateDelete = @SPValidateDelete + ' ^ ' + @space + @space + @space + ' FROM ' + @CurrentFullTableName
	SET @SPValidateDelete = @SPValidateDelete + ' ^ ' + @space + @space + @space + ' WHERE Id = @RecordId '
	SET @SPValidateDelete = @SPValidateDelete + ' ^ ' + @space + @space + ' ) '

	DECLARE @SPValidateSetErrorCode		 NVARCHAR(1000) = @space + @space + 'SET @ErrorCode = '
	DECLARE @SPValidateGenerateMessageFn NVARCHAR(1000) = @space + @space + 'SET @Message = [dbo].[ufn_GenerateMessage](@ErrorCode, @Message, '''', @LangTypeCode); '
	
-------------========================================================================================------------

    Set @strSPText = ''
    Set @strSPText = @strSPText + ' ^ ' + '-- ============================================='
    Set @strSPText = @strSPText + ' ^ ' + '-- Author : ' + SYSTEM_USER
    Set @strSPText = @strSPText + ' ^ ' + '-- Create date : ' + Convert(varchar(20),Getdate())
    Set @strSPText = @strSPText + ' ^ ' + '-- Description : Validation Procedure for ' + @CurrentTableName    
    Set @strSPText = @strSPText + ' ^ ' + '-- ============================================='

    Set @strSPText = @strSPText + ' ^ ' + 'IF OBJECT_ID(''' + REPLACE(@ValidateSPName,'''','''''') + ''',''P'') IS NOT NULL'
    Set @strSPText = @strSPText + ' ^ ' + '   DROP PROCEDURE  ' + @ValidateSPName
    Set @strSPText = @strSPText + ' ^ ' + 'GO'
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + ''

    Set @strSPText = @strSPText + ' ^ ' + 'CREATE PROCEDURE ' + @ValidateSPName
    Set @strSPText = @strSPText + ' ^ ' + @space + '' + @ColumnParametersValidate
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @strBegin
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @SPValidateIFmodeDelete
    Set @strSPText = @strSPText + ' ^ ' + @strBeginpart

    Set @strSPText = @strSPText + ' ^ ' + @space + @space + 'IF @RecordId IS NOT NULL '
    Set @strSPText = @strSPText + ' ^ ' + @space + @strBeginpart
    Set @strSPText = @strSPText + ' ^ ' + @space + @SPValidateDelete
    Set @strSPText = @strSPText + ' ^ ' + @space + @space + @strBeginpart
    Set @strSPText = @strSPText + ' ^ ' + @space + @space + @SPValidateSetErrorCode + ' 2 '
    Set @strSPText = @strSPText + ' ^ ' + @space + @space + @SPValidateGenerateMessageFn
    Set @strSPText = @strSPText + ' ^ ' + @space + @space + @strEndpart
    Set @strSPText = @strSPText + ' ^ ' + @space + @strEndpart
    Set @strSPText = @strSPText + ' ^ ' + @space + @space + 'ELSE'
    Set @strSPText = @strSPText + ' ^ ' + @space + @strBeginpart
    Set @strSPText = @strSPText + ' ^ ' + @space + @space + @SPValidateSetErrorCode + ' 10'
    Set @strSPText = @strSPText + ' ^ ' + @space + @space + @SPValidateGenerateMessageFn
    Set @strSPText = @strSPText + ' ^ ' + @space + @strEndpart

    Set @strSPText = @strSPText + ' ^ ' + @strEndpart
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + + '--' + @SPValidateIFmodeInsert
    Set @strSPText = @strSPText + ' ^ ' + + '--' + @strBeginpart
    Set @strSPText = @strSPText + ' ^ ' + + '--' + ''
    Set @strSPText = @strSPText + ' ^ ' + + '--' + @SPValidateComment
    Set @strSPText = @strSPText + ' ^ ' + + '--' + ''
    Set @strSPText = @strSPText + ' ^ ' + + '--' + @strEndpart
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + + '--' + @SPValidateIFmodeUpdate
    Set @strSPText = @strSPText + ' ^ ' + + '--' + @strBeginpart
    Set @strSPText = @strSPText + ' ^ ' + + '--' + ''
    Set @strSPText = @strSPText + ' ^ ' + + '--' + @SPValidateComment
    Set @strSPText = @strSPText + ' ^ ' + + '--' + ''
    Set @strSPText = @strSPText + ' ^ ' + + '--' + @strEndpart
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @strEnd

    Set @strSPText = @strSPText + ' ^ ' + ''

       SELECT * FROM STRING_SPLIT(@strSPText,'^')

    INSERT INTO @StatementList (FullTableName,StatementType,Statement) VALUES (@CurrentFullTableName,'Validate',@strSPText)

------===================================== CRUD SPs Templates =============================================---
-- the body of each can be modified

    Set @strSPText = ''
    Set @strSPText = @strSPText + ' ^ ' + '-- ============================================='
    Set @strSPText = @strSPText + ' ^ ' + '-- Author : ' + SYSTEM_USER
    Set @strSPText = @strSPText + ' ^ ' + '-- Create date : ' + Convert(varchar(20),Getdate())
    Set @strSPText = @strSPText + ' ^ ' + '-- Description : Insert Procedure for ' + @CurrentTableName    
    Set @strSPText = @strSPText + ' ^ ' + '-- ============================================='

    Set @strSPText = @strSPText + ' ^ ' + 'IF OBJECT_ID(''' + REPLACE(@insertSPName,'''','''''') + ''',''P'') IS NOT NULL'
    Set @strSPText = @strSPText + ' ^ ' + '   DROP PROCEDURE  ' + @insertSPName 
    Set @strSPText = @strSPText + ' ^ ' + 'GO'
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + ''

    Set @strSPText = @strSPText + ' ^ ' + 'CREATE PROCEDURE ' + @insertSPName
    Set @strSPText = @strSPText + ' ^ ' + @space + '' + @ColumnParametersInsert   
    Set @strSPText = @strSPText + ' ^ ' + @space + '' + @OutputId   
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @strBegin
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @ErrorControlInputParams
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + '--========================== Validation Tasks ==============================='
    Set @strSPText = @strSPText + ' ^ ' + @strBeginTry
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @space + '' + @FunctionPruneColumnName
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @space + '' + @ValidationProcedureInsert
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @space + '' + @ErrorThrowCondition
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + '-- ============================================='
    Set @strSPText = @strSPText + ' ^ ' + @strBeginTran
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @space + @spaceForTrans + 'INSERT INTO ' + @CurrentFullTableName + '('
    Set @strSPText = @strSPText + ' ^ ' + @space + @spaceForTrans + '    ' + '' + @ColumnDefForInsert + ')'
    Set @strSPText = @strSPText + ' ^ ' + @space + @spaceForTrans + 'VALUES ('
    Set @strSPText = @strSPText + ' ^ ' + @space + @spaceForTrans + '    ' + '' + @ColumnInValueForInsert + ')'
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @strEndTran
    Set @strSPText = @strSPText + ' ^ ' + ''
    IF @NbPrimaryKey =1 --No return if 2 or 0 primarykeys 
      Set @strSPText = @strSPText + ' ^ ' + @space + @spaceForTrans + 'SELECT @Id = SCOPE_IDENTITY() '
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @strEndTry
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @ErrorThrowExceptionInsert
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @strEndCatch
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @strEnd
    Set @strSPText = @strSPText + @SetVariablesForExec
    Set @strSPText = @strSPText + ' ^ ' + 'EXEC ' + @insertSPName + ' '  + @ColumnParametersInsertForExec
    Set @strSPText = @strSPText + ' ^ ' + 'GO'
    Set @strSPText = @strSPText + ' ^ ' + 'SELECT * FROM ' + @CurrentFullTableName + ' ORDER BY 1 DESC'
    IF @UnCommentExecForDebug = 0 Set @strSPText = @strSPText + ' ^ ' + '*/'

       SELECT * FROM STRING_SPLIT(@strSPText,'^')

    INSERT INTO @StatementList (FullTableName,StatementType,Statement) VALUES (@CurrentFullTableName,'Insert',@strSPText)

    Set @strSPText = ''
    Set @strSPText = @strSPText + ' ^ ' + '-- ============================================='
    Set @strSPText = @strSPText + ' ^ ' + '-- Author : ' + SYSTEM_USER
    Set @strSPText = @strSPText + ' ^ ' + '-- Create date : ' + Convert(varchar(20),Getdate())
    Set @strSPText = @strSPText + ' ^ ' + '-- Description : Update Procedure for ' + @CurrentTableName
    Set @strSPText = @strSPText + ' ^ ' + '-- ============================================='

    Set @strSPText = @strSPText + ' ^ ' + 'IF OBJECT_ID(''' + REPLACE(@updateSPName,'''','''''') + ''',''P'') IS NOT NULL'
    Set @strSPText = @strSPText + ' ^ ' + '   DROP PROCEDURE  ' + @updateSPName 
    Set @strSPText = @strSPText + ' ^ ' + 'GO'
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + 'CREATE PROCEDURE ' + @updateSPName
    Set @strSPText = @strSPText + ' ^ ' + @space + '' + @ColumnParametersUpdate
    Set @strSPText = @strSPText + ' ^ ' + @strBegin
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @ErrorControlInputParams
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + '--========================== Validation Tasks ==============================='
    Set @strSPText = @strSPText + ' ^ ' + @strBeginTry
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @space + '' + @FunctionPruneColumnName
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @space + '' + @ValidationProcedureUpdate
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @space + '' + @ErrorThrowCondition
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + '-- ============================================='
    Set @strSPText = @strSPText + ' ^ ' + @strBeginTran
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @space + @spaceForTrans+ 'UPDATE ' + @CurrentFullTableName 
    Set @strSPText = @strSPText + ' ^ ' + @space + @spaceForTrans+ '   SET ' + @updCols
    Set @strSPText = @strSPText + ' ^ ' + @space + @spaceForTrans+ ' ' + @whereCols
	Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @strEndTran
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @strEndTry
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @ErrorThrowExceptionUpdate
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @strEndCatch
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @strEnd
    Set @strSPText = @strSPText + @SetVariablesForExec  + @SetVariablesForExecUpdate  
    Set @strSPText = @strSPText + ' ^ ' + 'EXEC ' + @updateSPName + ' ' + @ColumnParametersUpdateForExec
    Set @strSPText = @strSPText + ' ^ ' + 'GO'
    Set @strSPText = @strSPText + ' ^ ' + 'SELECT * FROM ' + @CurrentFullTableName + ' '
    IF @UnCommentExecForDebug = 0 Set @strSPText = @strSPText + ' ^ ' + '*/'

       SELECT * FROM STRING_SPLIT(@strSPText,'^')
    INSERT INTO @StatementList (FullTableName,StatementType,Statement) VALUES (@CurrentFullTableName,'Update',@strSPText)

    Set @strSPText = ''
    Set @strSPText = @strSPText + ' ^ ' + '-- ============================================='
    Set @strSPText = @strSPText + ' ^ ' + '-- Author : ' + SYSTEM_USER
    Set @strSPText = @strSPText + ' ^ ' + '-- Create date : ' + Convert(varchar(20),Getdate())
    Set @strSPText = @strSPText + ' ^ ' + '-- Description : Delete Procedure for ' + @CurrentTableName
    Set @strSPText = @strSPText + ' ^ ' + '-- ============================================='

    Set @strSPText = @strSPText + ' ^ ' + 'IF OBJECT_ID(''' + REPLACE(@deleteSPName,'''','''''') + ''',''P'') IS NOT NULL'
    Set @strSPText = @strSPText + ' ^ ' + '   DROP PROCEDURE  ' + @deleteSPName 
    Set @strSPText = @strSPText + ' ^ ' + 'GO'
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + ''

    Set @strSPText = @strSPText + ' ^ ' + 'CREATE PROCEDURE ' + @deleteSPName
    Set @strSPText = @strSPText + ' ^ ' + @space + '' + @ColumnParametersDelete
    Set @strSPText = @strSPText + ' ^ ' + @strBegin
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @ErrorControlInputParams
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + '--========================== Validation Tasks ==============================='
    Set @strSPText = @strSPText + ' ^ ' + @strBeginTry
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @space + '' + @ValidationProcedureDelete
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @space + '' + @ErrorThrowCondition
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + '-- ============================================='
    Set @strSPText = @strSPText + ' ^ ' + @strBeginTran
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @space + 'DELETE FROM ' + @CurrentFullTableName
    Set @strSPText = @strSPText + ' ^ ' + @space + ' ' + @whereCols
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @strEndTran
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @strEndTry
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @ErrorThrowExceptionDelete
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @strEndCatch
    Set @strSPText = @strSPText + ' ^ ' + ''
    Set @strSPText = @strSPText + ' ^ ' + @strEnd
    Set @strSPText = @strSPText + @SetVariablesForExecDelete
    Set @strSPText = @strSPText + ' ^ ' + 'EXEC ' + @deleteSPName + ' ' + @ColumnParametersDeleteForExec
    Set @strSPText = @strSPText + ' ^ ' + 'GO'
    Set @strSPText = @strSPText + ' ^ ' + 'SELECT * FROM ' + @CurrentFullTableName + ' ORDER BY 1 DESC'
    IF @UnCommentExecForDebug = 0 Set @strSPText = @strSPText + ' ^ ' + '*/'

       SELECT * FROM STRING_SPLIT(@strSPText,'^')
    INSERT INTO @StatementList (FullTableName,StatementType,Statement) VALUES (@CurrentFullTableName,'Delete',@strSPText)


    Drop table #tmp_Structure   
    Fetch next from Tables_cursor INTO @CurrentSchemaName,@CurrentTableName
END
CLOSE Tables_cursor
DEALLOCATE Tables_cursor



SET @DropStatement = '

------------------------------------------- TO CLEAN COMPLETELY THE APPLICATION ---------------------------------------
/*' + @DropStatement +'
*/' 
INSERT INTO @StatementList (FullTableName,StatementType,Statement) VALUES ('Common','Drop statement to put at the end of final script',@DropStatement)


--SELECT * FROM @StatementList
--ORDER BY 1


END
GO

/*

--For all tables of schema dbo of database "TestDB":
EXEC dbo.GenerateDynamicProc 'TestDB','dbo','Customers' 

*/
