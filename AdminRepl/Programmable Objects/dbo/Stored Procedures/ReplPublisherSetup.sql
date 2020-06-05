IF OBJECT_ID('[dbo].[ReplPublisherSetup]') IS NOT NULL
	DROP PROCEDURE [dbo].[ReplPublisherSetup];

GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
CREATE PROC [dbo].[ReplPublisherSetup]
    @databasename sysname,
    @drop BIT = 1,     /* Nukes replication and drops all databases*/
    @create BIT = 1,   /* Creates all databases */
    @populate BIT = 1, /* Adds tables with sample data AND sets up replication pubs and subs*/
    @dsql NVARCHAR(MAX),
    @publication sysname,
    @password sysname,
    @table sysname,
    @filterclause NVARCHAR(500),
    @schemaowner sysname,
    @login sysname,
    @msg NVARCHAR(2000)
AS
BEGIN
    SET NOCOUNT ON;


    DECLARE @logreaderagent TABLE
    (
        id INT NOT NULL,
        name NVARCHAR(100) NOT NULL,
        publisher_security_mode SMALLINT NULL,
        publisher_login sysname NULL,
        publisher_password NVARCHAR(524) NULL,
        job_id UNIQUEIDENTIFIER NULL,
        job_login NVARCHAR(512) NULL,
        job_password sysname NULL
    );

    SET @login = SYSTEM_USER;

    DECLARE createdbcursor CURSOR LOCAL FAST_FORWARD READ_ONLY FOR
    SELECT dbs.ourdatabases
    FROM
    (
        VALUES
            ('ReplProdPub'),
            ('ReplProdSubA'),
            ('ReplProdSubB'),
            ('ReplDevPub'),
            ('ReplDevSubA'),
            ('ReplDevSubB')
    ) AS dbs (ourdatabases);
    OPEN createdbcursor;
    FETCH NEXT FROM createdbcursor
    INTO @databasename;
    WHILE @@FETCH_STATUS = 0
    BEGIN

        SET @msg
            = CHAR(10) + N'********************************************************' + CHAR(10)
              + CAST(SYSDATETIME() AS NVARCHAR(23)) + N': WORKING ON DATABASE ' + @databasename + CHAR(10)
              + N'********************************************************';
        RAISERROR(@msg, 1, 1) WITH NOWAIT;


        IF DB_ID(@databasename) IS NOT NULL
           AND @drop = 1
        BEGIN

            SET @msg
                = CAST(SYSDATETIME() AS NVARCHAR(23)) + N': Remove replication and drop database ' + @databasename
                  + CHAR(10)
                  + N'Note: you may see Msg 18752 (level 16 error) at this point. That''s because we''re running sp_removedbreplication.'
                  + CHAR(10)
                  + N' That is the ''nuclear'' option. In a production environment you should remove replication gently, '
                  + CHAR(10)
                  + N'using a sequence of planned steps, not pull the rug out under it like we are doing in this prototype.'
                  + CHAR(10);
            RAISERROR(@msg, 1, 1) WITH NOWAIT;


            EXEC master.sys.sp_removedbreplication @dbname = @databasename;

            EXEC ('
        ALTER DATABASE ' + @databasename + ' SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
        DROP DATABASE ' + @databasename + ' ;
        '        );
        END;
        IF @create = 1
        BEGIN
            SET @msg = CAST(SYSDATETIME() AS NVARCHAR(23)) + N': Create database ' + @databasename;
            RAISERROR(@msg, 1, 1) WITH NOWAIT;


            EXEC ('CREATE DATABASE ' + @databasename);
        END;

        IF @populate = 1
           AND @databasename LIKE '%Pub'
        BEGIN

            SET @msg
                = CAST(SYSDATETIME() AS NVARCHAR(23)) + N': Create tables with sample data in publisher database '
                  + @databasename;
            RAISERROR(@msg, 1, 1) WITH NOWAIT;


            SET @dsql
                = N'
    CREATE TABLE ' + @databasename
                  + N'.dbo.colors
    (colorid INT IDENTITY,
    color VARCHAR(512) NOT NULL,
    CONSTRAINT cx_colors PRIMARY KEY CLUSTERED(colorid),
    CONSTRAINT uq_color UNIQUE(color));

    CREATE TABLE ' + @databasename
                  + N'.dbo.breeds
    (breedid INT IDENTITY,
    breed VARCHAR(256) NOT NULL,
    CONSTRAINT cx_breeds PRIMARY KEY CLUSTERED(breedid),
    CONSTRAINT uq_breed UNIQUE(breed));

    CREATE TABLE ' + @databasename
                  + N'.dbo.doggos
    (dogid BIGINT IDENTITY,
    name VARCHAR(256) NOT NULL,
    colorid INT NOT NULL,
    birthdate DATE NULL,
    breedid INT NULL,
    CONSTRAINT cx_doggos PRIMARY KEY CLUSTERED(dogid),
    CONSTRAINT fk_doggos_colorid FOREIGN KEY(colorid)REFERENCES dbo.colors(colorid),
    CONSTRAINT fk_doggos_breedid FOREIGN KEY(breedid)REFERENCES dbo.breeds(breedid));

    INSERT ' + @databasename
                  + N'.dbo.colors(color)
    VALUES(''Black with white socks''),
        (''Tricolor''),
        (''Goldenfloof''),
        (''Velveteen''),
        (''Mud puddle'');

    INSERT ' + @databasename
                  + N'.dbo.breeds(breed)
    VALUES(''Muttski''),
        (''Corgi''),
        (''Retriever''),
        (''Goldendoodle''),
        (''Frenchie'');

    INSERT ' + @databasename
                  + N'.dbo.doggos(name, colorid, breedid)
    VALUES(''Mister'', 1, 1),
        (''Stormy'', 2, 2),
        (''Wendell'', 3, 3);
    '       ;
            EXEC sys.sp_executesql @stmt = @dsql;

            SET @msg = CAST(SYSDATETIME() AS NVARCHAR(23)) + N': Enable replication for ' + @databasename;
            RAISERROR(@msg, 1, 1) WITH NOWAIT;

            DECLARE @procname sysname;
            SET @procname = @databasename + '..' + 'sp_replicationdboption';

            EXEC @procname @dbname = @databasename,
                           @optname = 'publish',
                           @value = 'true';


            SET @procname = @databasename + '..' + 'sp_helplogreader_agent';
            INSERT @logreaderagent
            (
                id,
                name,
                publisher_security_mode,
                publisher_login,
                publisher_password,
                job_id,
                job_login,
                job_password
            )
            EXEC @procname;

            IF
            (
                SELECT COUNT(*) FROM @logreaderagent
            ) < 1
            BEGIN

                SET @msg = CAST(SYSDATETIME() AS NVARCHAR(23)) + N': Add log reader agent for ' + @databasename;
                RAISERROR(@msg, 1, 1) WITH NOWAIT;

                SET @procname = @databasename + '..' + 'sp_addlogreader_agent';

                EXEC @procname @job_login = NULL, @publisher_security_mode = 1;
            END;

            SET @msg = CAST(SYSDATETIME() AS NVARCHAR(23)) + N': Add publication ' + @publication;
            RAISERROR(@msg, 1, 1) WITH NOWAIT;


            SET @publication = @databasename + N'_Pub1';
            SET @procname = @databasename + '..' + 'sp_addpublication';

            EXEC @procname @publication = @publication,
                           @status = N'active',
                           @allow_push = N'true',
                           @allow_pull = N'true',
                           @independent_agent = N'true',
                           @allow_anonymous = true, /* NO BUENO! NO BUENO!!!!*/
                           @immediate_sync = true;

            SET @msg
                = CAST(SYSDATETIME() AS NVARCHAR(23)) + N': Create snapshot agent for publication ' + @publication;
            RAISERROR(@msg, 1, 1) WITH NOWAIT;


            SET @procname = @databasename + '..' + 'sp_addpublication_snapshot';

            EXEC @procname @publication = @publication,
                           @job_login = NULL,
                           @job_password = @password,
                           @publisher_security_mode = 1;

            SET @msg = CAST(SYSDATETIME() AS NVARCHAR(23)) + N': Add articles to publications for ' + @databasename;
            RAISERROR(@msg, 1, 1) WITH NOWAIT;

            DECLARE addarticlescursor CURSOR LOCAL FAST_FORWARD READ_ONLY FOR
            SELECT tbls.tablename
            FROM
            (
                VALUES
                    ('doggos'),
                    ('breeds'),
                    ('colors')
            ) AS tbls ([tablename]);


            OPEN addarticlescursor;

            FETCH NEXT FROM addarticlescursor
            INTO @table;

            WHILE @@FETCH_STATUS = 0
            BEGIN

                SET @msg = CAST(SYSDATETIME() AS NVARCHAR(23)) + N': Add article ' + @table;
                RAISERROR(@msg, 1, 1) WITH NOWAIT;

                SET @filterclause = NULL;
                SET @schemaowner = N'dbo';

                SET @procname = @databasename + '..' + 'sp_addarticle';

                EXEC @procname @publication = @publication,
                               @article = @table,
                               @source_object = @table,
                               @source_owner = @schemaowner,
                               @schema_option = 0x80030F3,
                               @vertical_partition = N'true',
                               @type = N'logbased',
                               @filter_clause = @filterclause; --, @status=null;


                FETCH NEXT FROM addarticlescursor
                INTO @table;
            END;


            CLOSE addarticlescursor;
            DEALLOCATE addarticlescursor;


        END;


        IF @populate = 1
           AND @databasename LIKE '%Sub%'
        BEGIN

            SET @msg = CAST(SYSDATETIME() AS NVARCHAR(23)) + N': Populate subscriber database ' + @databasename;
            RAISERROR(@msg, 1, 1) WITH NOWAIT;
            SET @msg
                = CAST(SYSDATETIME() AS NVARCHAR(23)) + N': Create table with sample data in each subscriber database '
                  + @databasename;
            RAISERROR(@msg, 1, 1) WITH NOWAIT;


            SET @dsql
                = N'
    CREATE TABLE ' + @databasename
                  + N'.dbo.subscribertableonly
    (id INT IDENTITY,
    col1 VARCHAR(512) NOT NULL,
    CONSTRAINT cx_subscribertableonly PRIMARY KEY CLUSTERED(id)
    );

    INSERT ' + @databasename
                  + N'.dbo.subscribertableonly(col1)
    VALUES(''Imma row''),
        (''Imma row, also''),
        (''Make it three'');
    '       ;
            EXEC sp_executesql @dsql;

        --SET @msg=CAST(SYSDATETIME() AS NVARCHAR(23))+N': Enable replication for subscriber '+@databasename;
        --RAISERROR(@msg, 1, 1) WITH NOWAIT;

        --SET @procname=@databasename+'..'+'sp_replicationdboption';

        --EXEC @procname @dbname=@databasename, @optname='subscribe', @value='true';

        END;



        IF @populate = 1
           AND @databasename LIKE '%Pub'
        BEGIN

            SET @publication = @databasename + N'_Pub1';

            SET @msg
                = CAST(SYSDATETIME() AS NVARCHAR(23)) + N': Doing final replication setup steps on publisher database '
                  + @databasename + N' for pub' + @publication;
            RAISERROR(@msg, 1, 1) WITH NOWAIT;

            DECLARE @subscriberdatabasename sysname;

            DECLARE addsubscriptioncursor CURSOR LOCAL FAST_FORWARD READ_ONLY FOR
            SELECT dbs.ourdatabases
            FROM
            (
                VALUES
                    ('ReplProdSubA'),
                    ('ReplProdSubB'),
                    ('ReplDevSubA'),
                    ('ReplDevSubB')
            ) AS dbs (ourdatabases);

            OPEN addsubscriptioncursor;

            FETCH NEXT FROM addsubscriptioncursor
            INTO @subscriberdatabasename;


            WHILE @@FETCH_STATUS = 0
            BEGIN

                SET @msg
                    = CAST(SYSDATETIME() AS NVARCHAR(23)) + N': Running sp_addsubscription in publisher database '
                      + @databasename + N' for pub: ' + @publication + N' and subscriber ' + @subscriberdatabasename;
                RAISERROR(@msg, 1, 1) WITH NOWAIT;


                -- At the Publisher, register the subscription, using the defaults.  
                SET @procname = @databasename + '..' + 'sp_addsubscription';
                EXEC @procname @publication = @publication,
                               @subscriber = @@SERVERNAME,
                               @destination_db = @subscriberdatabasename,
                               @subscription_type = N'push',
                               @status = NULL;


                SET @msg = CAST(SYSDATETIME() AS NVARCHAR(23)) + N': Add push subscription agent for ' + @databasename;
                RAISERROR(@msg, 1, 1) WITH NOWAIT;

                SET @procname = @databasename + '..' + 'sp_addpushsubscription_agent';

                --SET @publication=CASE WHEN @databasename LIKE '%Dev%' THEN 'ReplDevPub_Pub1' ELSE 'ReplProdPub_Pub1' END;
                --SET @publicationdatabase=CASE WHEN @databasename LIKE '%Dev%' THEN 'ReplDevPub' ELSE 'ReplProdPub' END;

                EXEC @procname @publisher = NULL,
                               @publication = @publication,
                               @subscriber = @@SERVERNAME,
                               @subscriber_db = @subscriberdatabasename,
                               @job_login = NULL,
                               @job_password = @password;

                FETCH NEXT FROM addsubscriptioncursor
                INTO @subscriberdatabasename;

            END;

            CLOSE addsubscriptioncursor;
            DEALLOCATE addsubscriptioncursor;

        END;



        FETCH NEXT FROM createdbcursor
        INTO @databasename;
    END;
    CLOSE createdbcursor;
    DEALLOCATE createdbcursor;
END;
GO
