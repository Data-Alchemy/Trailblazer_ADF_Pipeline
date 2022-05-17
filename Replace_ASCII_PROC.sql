CREATE FUNCTION [dbo].[ReplaceASCII](@inputString VARCHAR(8000))
RETURNS VARCHAR(55)
AS
     BEGIN
         DECLARE @badStrings VARCHAR(100);
         DECLARE @increment INT= 1;
         WHILE @increment <= DATALENGTH(@inputString)
             BEGIN
                 IF(ASCII(SUBSTRING(@inputString, @increment, 1)) < 48)
                     BEGIN
                         SET @badStrings = CHAR(ASCII(SUBSTRING(@inputString, @increment, 1)));
                         SET @inputString = REPLACE(REPLACE(REPLACE(REPLACE(@inputString, @badStrings, '_'),'(','_'),'.','_'),'__','_');
                 END;
                 SET @increment = @increment + 1;
             END;
         RETURN UPPER(TRIM('_' FROM @inputString));
     END;
GO;
