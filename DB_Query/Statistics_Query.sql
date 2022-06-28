SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[spj_DailyStatisticsSchedule]
    @now_date DATETIME
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        
        /*------------------------------------------------------
            Type  /         DESC              /  Data Type
             1       �� ���� ��                  INT
             2       ���� ������ ��(Idx)         INT
             3       ���� ������ ��              INT
             4       ���� ������ ��              INT
             5       ���� �� ����                BIGINT    -- ����
             6       ��� CCU                    INT
             7       �ִ� CCU                    INT
             8       �ִ� CCU �ð�               DATETIME
             9       ���� Ż���� ��              INT
             10      ���� Ż��öȸ�� ��          INT
             11      ���� ������ ��(device_key)  INT
			 12		 ���� �÷��� Ÿ��			 INT
			 13		 ���� ������ ��� ���� ��	 INT
			 14		 ���� ��� ���� �ð�		 INT
			 15		 ���� ������ ��(effective)	 INT
			 16		 ���� �� ���� ��			 INT
        ------------------------------------------------------*/

        DECLARE
            @iS          INT          = 1
        ,   @iE          INT          = 0
        ,   @start_date  NVARCHAR(19) = CONVERT(NVARCHAR(10), DATEADD(DD, -1, @now_date), 120) + N' 00:00:00' -- ���� ���� �ð� ex) 2016-01-01
        ,   @end_date    NVARCHAR(19) = CONVERt(NVARCHAR(10),                 @now_date , 120) + N' 00:00:00' -- ���� ���� �ð� ex) 2016-01-02

        DECLARE @data TABLE
        (
            no             INT IDENTITY(1,1)
        ,   type           INT
        ,   value_int      INT
        ,   value_bigint   BIGINT
        ,   value_nvarchar NVARCHAR(100)
        ,   value_date     DATETIME
        ,   location       INT
		,	app_store	   INT
        )
        
        --���� �� ���� ��
        INSERT @data (type, location, app_store, value_int)
        SELECT 1, -1, -1, count(*) FROM dbo.signup_log WITH(NOLOCK) WHERE log_time < @end_date

        --���� �� ������ ��(account_idx ����)
        INSERT @data (type, location, app_store, value_int)
        SELECT 2, -1, -1, count(*) FROM dbo.signup_log WITH(NOLOCK) WHERE log_time >= @start_date AND log_time < @end_date

        --���� ������ ��
        INSERT @data (type, location, app_store, value_int)
        SELECT 3, -1, -1, count(*) FROM ( SELECT account_idx FROM dbo.daily_unique_login WITH(NOLOCK) WHERE login_date = @start_date GROUP BY account_idx) AS a

        --���� ������ ��
        INSERT @data (type, location, app_store, value_int)
        SELECT 4, -1, -1, count(*) FROM ( SELECT account_idx FROM dbo.purchase_cash_log WITH(NOLOCK) WHERE log_time >= @start_date AND log_time < @end_date GROUP BY account_idx ) AS a

        --���� �� ����
        INSERT @data (type, value_bigint)
        SELECT 5, ISNULL(SUM(real_price),0) FROM dbo.purchase_cash_log WITH(NOLOCK) WHERE log_time >= @start_date AND log_time < @end_date

        --���� ��� CCU
        INSERT @data (type, location, app_store, value_int)
        SELECT 6, -1, -1, ISNULL(AVG(ccu_count),0) FROM dbo.ccu_log WITH(NOLOCK) WHERE ccu_time >= @start_date AND ccu_time < @end_date

        --���� �ִ� CCU
        INSERT @data (type, location, app_store, value_int)
        SELECT 7, -1, -1, ISNULL(MAX(ccu_count),0) FROM dbo.ccu_log WITH(NOLOCK) WHERE ccu_time >= @start_date AND ccu_time < @end_date

        --���� �ִ� CCU�ð�
        INSERT @data (type, location, app_store, value_date)
        SELECT 8, -1, -1, ISNULL(MAX(a.ccu_time), @start_date) FROM dbo.ccu_log AS a WITH(NOLOCK) INNER JOIN (SELECT ISNULL(MAX(ccu_count),0) AS max_ccu FROM dbo.ccu_log WITH(NOLOCK) WHERE ccu_time >= @start_date AND ccu_time < @end_date ) AS b ON a.ccu_count = b.max_ccu WHERE ccu_time >= @start_date AND ccu_time < @end_date

        --���� Ż���� ��
        INSERT @data (type, location, app_store, value_int)
        SELECT 9, -1, -1, count(*) FROM dbo.withdrawal_log WITH(NOLOCK) WHERE log_type = 1 AND log_time >= @start_date AND log_time < @now_date

        --���� Ż�� öȸ�� ��
        INSERT @data (type, location, app_store, value_int)
        SELECT 10, -1, -1, count(*) FROM dbo.withdrawal_log WITH(NOLOCK) WHERE log_type = 0 AND log_time >= @start_date AND log_time < @now_date

        --���� �� ������ ��(device_key ����)
        INSERT @data (type, location, app_store, value_int)
        SELECT 11, -1, -1, COUNT(*) FROM ( SELECT count(*) AS cnt FROM dbo.signup_log WITH(NOLOCK) WHERE log_time >= @start_date AND log_time < @end_date GROUP BY device_key) AS a

		--���� �� �÷��� Ÿ��
		INSERT @data (type, location, app_store, value_bigint)
		SELECT 12, -1, -1, ISNULL(SUM(play_time_sec), 0) AS play_time FROM dbo.logout_log WITH(NOLOCK) WHERE log_time >= @start_date AND log_time < @end_date

		--���� ������ ��� ����(Login/Out) ��
		INSERT @data (type, location, app_store, value_nvarchar)
		SELECT 13, -1, -1, (SELECT CASE WHEN login_cnt > 0 THEN CONVERT(NVARCHAR(20), CONVERT(DECIMAL(7,2), ( ( login_cnt * 1.0 ) / DAU )) ) ELSE N'0' END AS value_nvarchar FROM ( SELECT @start_date AS start_date, COUNT(*) AS login_cnt FROM dbo.login_log WITH(NOLOCK) WHERE log_time >= @start_date AND log_time < @end_date) AS a
						JOIN (SELECT @start_date AS start_date, value_int AS DAU FROM @data WHERE type = 3 AND location = -1) AS b ON a.start_date = b.start_date )

		--�ѹ��� ����(Login/Out)�� ��� ���� �ð�
		INSERT @data (type, location, app_store, value_nvarchar)
		SELECT 14, -1, -1, (SELECT CASE WHEN pt > 0 THEN CONVERT(NVARCHAR(20), CONVERT(DECIMAL(7,2), ( ( pt * 1.0 ) / login_cnt / 60)) ) ELSE N'0' END AS value_nvarchar FROM ( SELECT @start_date AS start_date, SUM(CAST(play_time_sec AS bigint)) as pt FROM dbo.logout_log WITH(NOLOCK) WHERE log_time >= @start_date AND log_time < @end_date) AS a
						JOIN (SELECT @start_date AS start_date, COUNT(*) AS login_cnt FROM dbo.login_log WITH(NOLOCK) WHERE log_time >= @start_date AND log_time < @end_date) AS b ON a.start_date = b.start_date )

		--���� �� ������ ��(effective ����)
		INSERT @data (type, location, app_store, value_int)
		SELECT 15, -1, -1, (SELECT COUNT(*) FROM dbo.MPX_GAME_DungeonSignup WHERE create_time >= @start_date AND create_time < @end_date)

		--���� �� ���� ��
		INSERT @data (type, location, app_store, value_int)
		SELECT 16, -1, -1, COUNT(*) AS login_cnt FROM dbo.login_log WITH(NOLOCK) WHERE log_time >= @start_date AND log_time < @end_date

        SELECT
            @iE = MAX(no) FROM @data

		-- ������ ����� Update, �������� ������ Insert
        WHILE(@iS <= @iE)
        BEGIN
            
            IF EXISTS
            (
                SELECT *
                FROM dbo.game_stat AS a
                INNER JOIN @data AS b ON a.type = b.type
                WHERE
                    a.created_date = @start_date
                AND a.location = b.location
				AND a.app_store = b.app_store
                AND b.no = @iS
            )
            BEGIN

                UPDATE a
                SET a.value_int      = b.value_int
                ,   a.value_bigint   = b.value_bigint
                ,   a.value_nvarchar = b.value_nvarchar
                ,   a.value_date     = b.value_date
                ,   a.refresh_time   = GETDATE()
                FROM dbo.game_stat AS a
                INNER JOIN @data AS b ON a.type = b.type
                WHERE
                    a.created_date = @start_date
                AND a.location = b.location
				AND a.app_store = b.app_store
                AND b.no = @iS

            END
            ELSE
            BEGIN

                INSERT dbo.game_stat
                (
                    created_date
                ,   type
                ,   value_int
                ,   value_bigint
                ,   value_nvarchar
                ,   value_date
                ,   location
				,	app_store
                ,   refresh_time
                )
                SELECT
                    @start_date
                ,   type
                ,   value_int
                ,   value_bigint
                ,   value_nvarchar
                ,   value_date
                ,   location
				,	-1
                ,   GETDATE()
                FROM @data
                WHERE no = @iS
                    
            END

            SELECT
                @iS = @iS + 1
        END

    END TRY
    BEGIN CATCH
        DECLARE @ErrNum int, @ErrMsg nvarchar(4000);
        SELECT @ErrNum = ERROR_NUMBER()+100000, @ErrMsg = ERROR_PROCEDURE() + N'(' + CAST(ERROR_LINE()+7 AS NVARCHAR(100)) + N'): ' + ERROR_MESSAGE()
        IF @ErrNum < 160000
            PRINT 'ErrorCode:' + CONVERT(nvarchar, @ErrNum) + ', ' + @ErrMsg
        RETURN @ErrNum;
    END CATCH
    SET XACT_ABORT OFF;
    SET NOCOUNT OFF;
END