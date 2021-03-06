CREATE PROCEDURE [dbo].[spj_TableBackup]
    @is_table_backup INT = 0  -- 0:Table 백업 안함, 1:Table 백업 함
,   @is_create_view  INT = 0  -- 0:View 생성 안함 , 1:View 생성 함
,   @is_create_index INT = 0  -- 0:Index 생성 안함, 1:Index 생성 함
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
		BEGIN TRAN spj_TableBackup

			DECLARE @table_list TABLE
			(
				table_no   INT IDENTITY(1,1)
			,   table_name NVARCHAR(255)
			)

			--View 구분을 위해 테이블명 1첫째 문자는 대문자를 입력해주세요
			INSERT @table_list SELECT N'Backup_Table'
		
			DECLARE
				@sql         NVARCHAR(MAX) = N''
			,   @table_name  NVARCHAR(255) = N''
			,   @iS          INT           = 1
			,   @iE          INT           = 0
			,   @table_start INT           = 1
			,   @table_count INT           = 0
			SELECT
				@table_count = MAX(table_no)
			FROM @table_list

			WHILE (@table_start <= @table_count)
			BEGIN
        
				SELECT @table_name = table_name
				FROM @table_list
				WHERE table_no = @table_start

				-- 테이블 백업(스위칭)을 할것인가
				IF( @is_table_backup = 1 )
				BEGIN
    
					DECLARE
						@new_table_name  NVARCHAR(255) = N'' --신규 생성될 테이블
					,   @temp_table_name NVARCHAR(255) = N'' --임시 테이블명
					,   @original_table_name NVARCHAR(255) = N'' --원본 테이블명

					SELECT
						@new_table_name  = LOWER(@table_name) + N'_' + CONVERT(NVARCHAR(10), getdate(), 112)
					,   @temp_table_name = LOWER(@table_name) + N'_temp'
					,   @original_table_name = LOWER(@table_name)

					--백업할 원본 테이블이 존재하는지 검사
					IF NOT EXISTS( SELECT * FROM sys.sysobjects WHERE xtype = N'U' AND name = @original_table_name)
					BEGIN
						PRINT @table_name + N' - ' + N'실패, 백업할 원본 테이블이 존재하지 않습니다.'
					END
					--스위칭할 테이블이 이미 존재하는지 검사
					ELSE IF EXISTS( SELECT * FROM sys.sysobjects WHERE xtype = N'U' AND name = @new_table_name)
					BEGIN
						PRINT @table_name + N' - ' + N'실패, 스위칭할 테이블이 이미 존재합니다.'
					END
					ELSE
					BEGIN
						--스위칭될 테이블의 컬럼 정보를 저장할 임시테이블
						CREATE TABLE #new_table_columns
						(
							column_no   INT
						,   column_name NVARCHAR(100)
						,   column_type NVARCHAR(20)
						,   length      NVARCHAR(10)
						,   is_nullable NVARCHAR(20)
						,   is_identity NVARCHAR(20)
						)
						--원본 테이블의 컬럼 구조 저장
						INSERT #new_table_columns
						SELECT
							ROW_NUMBER() OVER (ORDER BY ORDINAL_POSITION ASC)
						,   COLUMN_NAME + N' '
						,   DATA_TYPE   + N' '
						,   CASE
								WHEN CHARACTER_MAXIMUM_LENGTH IS NULL THEN N' ' 
								WHEN CHARACTER_MAXIMUM_LENGTH = -1    THEN N'(MAX) '
								ELSE '('+ CONVERT(NVARCHAR,CHARACTER_MAXIMUM_LENGTH) + N') '
							END
						,   CASE WHEN is_nullable = N'YES' THEN N'NULL ' ELSE N'NOT NULL ' END
						,   0
						FROM INFORMATION_SCHEMA.COLUMNS 
						WHERE TABLE_NAME = @original_table_name

						--데이터 확인용
						--SELECT * from #new_table_columns

						SELECT
							@iS  = 1
						,   @iE  = MAX(column_no)
						,   @sql = N'CREATE TABLE dbo.' + @new_table_name + N'( '
						FROM #new_table_columns
                        
						-- Identity 속성이 있는 테이블의 컬럼 구조 변경
						UPDATE A
						SET is_identity = 1
						FROM
						(
							SELECT b.is_identity
							FROM
							(
								SELECT a.name, a.is_identity, b.name as table_name
								FROM sys.columns as a
								JOIN sys.sysobjects as b
								ON a.object_id = b.id
								WHERE a.is_identity = 1
								AND b.xtype = N'U'
								AND b.name = @table_name
							) AS a
							JOIN #new_table_columns AS b
							ON a.name = b.column_name
						) AS a
                        
						--컬럼 수 만큼 동적 스크립팅
						WHILE (@iS <= @iE)
						BEGIN
							--컬럼별 구분 쉼표 넣을지 여부
							SELECT @sql = @sql + CASE WHEN @iS > 1 THEN ',' ELSE '' END  

							--컬럼 생성구문 추가
							SELECT @sql = @sql + column_name + column_type + length + CASE WHEN is_identity = 1 THEN N'IDENTITY(1,1) ' ELSE ' ' END + is_nullable FROM #new_table_columns WHERE column_no = @iS 

							--컬럼번호 증가
							SELECT @iS = @iS + 1
						END
						SELECT @sql = @sql + N')'

						--최종 쿼리 확인용
						--SELECT @sql
                        
						--신규 테이블 동적 생성(스위칭할 테이블)
						EXEC (@sql)

						--생성된 테이블과 원본 테이블 스위칭
						EXEC sp_rename @original_table_name, @temp_table_name; -- 원본 테이블에 _temp 를 붙임
						EXEC sp_rename @new_table_name , @original_table_name; -- _날짜를 붙여놓은 신규 테이블을 원본 테이블명으로 바꿈
						EXEC sp_rename @temp_table_name, @new_table_name;  -- _temp를 붙여놓은 테이블명을 _날짜가 붙은 테이블 명으로 바꿈

						PRINT @table_name + N' - ' + N'테이블 백업, 스위칭 성공'

						DROP TABLE #new_table_columns
					END
				END

				-- 뷰를 생성할 것인가
				IF( @is_create_view = 1 )
				BEGIN

					DECLARE
						@view_name NVARCHAR(255) = '' --생성될 View 이름
					SELECT
						@view_name = N'v' + @table_name

					--View에 포함될 테이블 리스트를 저장할 임시테이블
					CREATE TABLE #view_tables
					(
						no         INT IDENTITY(1,1)
					,   table_name NVARCHAR(255)
					)

					-- 테이블명에 @table_name이 포함된 모든 테이블을 조회하여 테이블명 저장
					INSERT #view_tables
					SELECT
						CONVERT(NVARCHAR(255), name)
					FROM sys.sysobjects
					WHERE type = N'U' AND name LIKE (@table_name + N'%')
					ORDER BY name ASC

					--데이터 확인용
					--SELECT * FROM #view_tables

					SELECT
						@iS  = 1
					,   @iE  = MAX(no)
					,   @sql = N'CREATE VIEW ' + @view_name + N' AS '
					FROM #view_tables

					--뷰에 포함될 테이블 수 만큼 반복으로 스크립팅
					WHILE(@iS <= @iE)
					BEGIN
						--컬럼별 구분 쉼표 넣을지 여부
						SELECT @sql = @sql + CASE WHEN @iS > 1 THEN N' UNION ALL ' ELSE '' END  

						--컬럼 생성구문 추가
						SELECT @sql = @sql + N'SELECT * FROM dbo.' + LOWER(table_name) + N' ' FROM #view_tables WHERE no = @iS

						--컬럼번호 증가
						SELECT @iS = @iS + 1
					END					

					--기존에 생성된 View 제거
					IF EXISTS( SELECT * FROM sys.sysobjects WHERE xtype = 'V' AND name = @view_name)
					BEGIN
						EXEC (N'DROP VIEW ' + @view_name)
					END
                    
					--최종 쿼리 확인용
					--SELECT @sql

					--신규 View 생성
					EXEC (@sql)
            
					PRINT @table_name + N' - ' + N'View 생성 성공'

					DROP TABLE #view_tables
				END

				-- 인덱스를 생성할 것인가
				IF ( @is_create_index = 1 )
				BEGIN
                    
					DECLARE
						@check_name NVARCHAR(50)  = N'' --Index 존재 여부 체크
					,   @index_name NVARCHAR(255) = N''

					-- 인덱스 존재 유무 확인
					SELECT 
						@check_name = @check_name + b.name
					FROM sys.index_columns AS a
					JOIN sys.columns AS b
					ON a.column_id = b.column_id
					AND a.object_id  = b.object_id
					JOIN sys.objects AS c
					ON a.object_id = c.object_id
					WHERE c.type = N'U'
					AND c.name = LOWER(@table_name) + N'_' + CONVERT(NVARCHAR(10), getdate(), 112)
                    
					IF @check_name = N''
					BEGIN
						PRINT @table_name + N' - ' + N'Index 없음'
					END
					ELSE
					BEGIN
                        
						-- Index 리스트를 저장 할 임시 테이블
						CREATE TABLE #index_tables
						(
							no         INT IDENTITY(1,1)
						,   index_type INT
						,   index_name NVARCHAR(255)
						)

						-- 테이블에 있는 Index를 조회하여 임시 테이블에 저장
						INSERT #index_tables
						SELECT
							index_id
						,   b.name
						FROM sys.index_columns AS a
						JOIN sys.columns AS b
						ON a.column_id = b.column_id
						AND a.object_id  = b.object_id
						JOIN sys.objects AS c
						ON a.object_id = c.object_id
						WHERE c.type = N'U'
						AND c.name = LOWER(@table_name) + N'_' + CONVERT(NVARCHAR(10), getdate(), 112)
						ORDER BY a.index_id, a.key_ordinal

						SELECT
							@iS  = 1
						,   @iE  = MAX(index_type)
						FROM #index_tables
                        
						--Index에 포함될 컬럼 수 만큼 반복으로 스크립팅
						WHILE(@iS <= @iE)
						BEGIN                            
              
							SELECT @sql =
								CASE WHEN index_type = 1 THEN N'CREATE CLUSTERED INDEX CIX__' + LOWER(@table_name) + N' ON dbo.' + LOWER(@table_name) + N'(' + a + N')'
										WHEN index_type > 1 THEN N'CREATE NONCLUSTERED INDEX IX__' + LOWER(@table_name) + N' ON dbo.' + LOWER(@table_name) + N'(' + a + N')'
								END
							FROM 
							(
								SELECT DISTINCT index_type,
								STUFF
								(
									(
										SELECT ','+ index_name
										FROM #index_tables AS b
										WHERE b.index_type = a.index_type
										FOR XML PATH('')
									), 1, 1, '') AS a
								FROM #index_tables AS a
								WHERE index_type = @iS
							) AS a

							--최종 쿼리 확인용
							--SELECT @sql

							-- 인덱스 생성
							EXEC (@sql)

							-- 컬럼번호 증가
							SELECT @iS = @iS + 1
						END
                        
						PRINT @table_name + N' - ' + N'Index 생성 성공'
                    
						DROP TABLE #index_tables
					END
				END

				SELECT @table_start = @table_start + 1
			END

		COMMIT TRAN spj_TableBackup
		RETURN 0
    END TRY
    BEGIN CATCH
        ROLLBACK TRAN spj_TableBackup
        DECLARE @ErrNum int, @ErrMsg nvarchar(4000);
        SELECT @ErrNum = ERROR_NUMBER()+100000, @ErrMsg = ERROR_PROCEDURE() + N'(' + CAST(ERROR_LINE()+7 AS NVARCHAR(100)) + N'): ' + ERROR_MESSAGE()
        IF @ErrNum < 160000
            PRINT 'ErrorCode:' + CONVERT(nvarchar, @ErrNum) + ', ' + @ErrMsg
        RETURN @ErrNum;
    END CATCH
END