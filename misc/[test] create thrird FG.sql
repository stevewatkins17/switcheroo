-- Step 1: Create the new filegroup
use [TEMP_ABSOLUTE_20241108172936867_STEVEWATKINS]


ALTER DATABASE [TEMP_ABSOLUTE_20241108172936867_STEVEWATKINS]
ADD FILEGROUP [Third];

-- Step 2: Add a file to the filegroup
ALTER DATABASE [TEMP_ABSOLUTE_20241108172936867_STEVEWATKINS]
ADD FILE 
(
    NAME = N'HCHB_TSCH_DATA_3',
    FILENAME = N'M:\\PBPURE113-DATA07\\SQL\\DATA\\TEMP_ABSOLUTE_Data_3.ndf', -- Specify the path for the new file
    SIZE = 16GB,  -- Initial size of the file
    MAXSIZE = UNLIMITED,  -- Maximum size of the file
    FILEGROWTH = 12800MB  -- File growth increment
)
TO FILEGROUP [Third];

-- Step 3: Make "Third" the default filegroup
ALTER DATABASE [TEMP_ABSOLUTE_20241108172936867_STEVEWATKINS]
MODIFY FILEGROUP [Third] DEFAULT;

