-- TODO: add timestamp and module like other logs

-- Global Properties (User Configurable)
property topFolderName : "Media Organizer on LaCie"
property midFolderName : "Google Photos Pipeline"
property bottomFolderName : "MonthlyExports"
property destinationFolderPath : "/Volumes/LaCie/Media Organizer/Google Photos/01-MonthlyExports"
property debugLogPath : "/Users/nickolaycohen/dev/media-organizer-pipeline/logs/media_organizer.log"

on run argv
    with timeout of 600 seconds
        -- Get album name from command line argument
        set albumName to item 1 of argv

        -- ========================================
        -- Initialization
        -- ========================================

        -- Clear previous debug log
        do shell script "echo '' > " & quoted form of debugLogPath

        -- Create final export folder path
        set fullDestinationPath to destinationFolderPath & "/" & albumName

        -- Check if folder exists and rename it with timestamp
        set folderCheckCmd to "test -d " & quoted form of fullDestinationPath & " && echo exists || echo missing"
        set folderStatus to do shell script folderCheckCmd

        if folderStatus is "exists" then
            set timestamp to do shell script "date +%Y%m%d-%H%M"
            set archivedPath to fullDestinationPath & "__" & timestamp

            -- TODO: Move to dedicated archive folder instead of renaming in place
            -- Example: /ArchivedExports/2025-03__20240429-1512
            do shell script "mv " & quoted form of fullDestinationPath & " " & quoted form of archivedPath
            logMessage("Existing folder found. Renamed to: " & archivedPath)
        end if

        -- Create fresh destination folder
        do shell script "mkdir -p " & quoted form of fullDestinationPath
        set destinationFolder to POSIX file fullDestinationPath as alias

        logMessage("Starting export process for album: " & albumName)

        -- ========================================
        -- Main Workflow
        -- ========================================

        -- Step 1: Activate Photos
        tell application "Photos"
            activate
            delay 1
        end tell

        -- Step 2: Find Top-Level Folder
        set topContainer to missing value
        tell application "Photos"
            set allTopFolders to folders
            repeat with f in allTopFolders
                if (name of f) is equal to topFolderName then
                    set topContainer to f
                    exit repeat
                end if
            end repeat
        end tell

        if topContainer is missing value then
            logMessage("Top folder '" & topFolderName & "' not found.")
            display dialog "Top folder '" & topFolderName & "' not found." buttons {"OK"} default button 1
            return
        else
            logMessage("Found top folder: " & topFolderName)
        end if

        -- Step 3: Find Mid-Level Folder
        set midContainer to missing value
        tell application "Photos"
            set midFolders to folders of topContainer
            repeat with f in midFolders
                if (name of f) is equal to midFolderName then
                    set midContainer to f
                    exit repeat
                end if
            end repeat
        end tell

        if midContainer is missing value then
            logMessage("Mid folder '" & midFolderName & "' not found.")
            display dialog "Mid folder '" & midFolderName & "' not found." buttons {"OK"} default button 1
            return
        else
            logMessage("Found mid folder: " & midFolderName)
        end if

        -- Step 4: Find Bottom-Level Folder
        set bottomContainer to missing value
        tell application "Photos"
            set bottomFolders to folders of midContainer
            repeat with f in bottomFolders
                if (name of f) is equal to bottomFolderName then
                    set bottomContainer to f
                    exit repeat
                end if
            end repeat
        end tell

        if bottomContainer is missing value then
            logMessage("Bottom folder '" & bottomFolderName & "' not found.")
            display dialog "Bottom folder '" & bottomFolderName & "' not found." buttons {"OK"} default button 1
            return
        else
            logMessage("Found bottom folder: " & bottomFolderName)
        end if

        -- Step 5: Find the Album
        set targetAlbum to missing value
        tell application "Photos"
            set albumsInBottom to albums of bottomContainer
            repeat with a in albumsInBottom
                if (name of a) is equal to albumName then
                    set targetAlbum to a
                    exit repeat
                end if
            end repeat
        end tell

        if targetAlbum is missing value then
            logMessage("Album '" & albumName & "' not found inside '" & bottomFolderName & "'.")
            display dialog "Album '" & albumName & "' not found inside '" & bottomFolderName & "'." buttons {"OK"} default button 1
            return
        else
            logMessage("Found album: " & albumName)
        end if

        -- Step 6: Export Media Items
        set mediaItems to {}

        tell application "Photos"
            set mediaItems to media items of targetAlbum
        end tell

        if (count of mediaItems) is greater than 0 then
            try
                tell application "Photos"
                    export mediaItems to destinationFolder with using originals
                end tell
                logMessage("✅ Export completed successfully for album: " & albumName)

                -- Run the remove_mov.sh cleanup script after export
                -- This fuctionality will be moved to a deduplication step
                -- set cleanupScript to "/Users/nickolaycohen/dev/media-organizer-pipeline/scripts/traverse_remove_mov/traverse_remove_mov.sh"
                -- set shellCommand to "/bin/zsh " & quoted form of cleanupScript & " " & quoted form of fullDestinationPath

                -- try
                    -- set cleanupOutput to do shell script shellCommand
                    -- logMessage("Post-export cleanup output:\n" & cleanupOutput)
                -- on error errMsg
                    -- logMessage("⚠️ Error during post-export cleanup:\n" & errMsg)
                -- end try

                tell application "System Events"
                    display notification "Export of '" & albumName & "' completed successfully!" with title "Photos Export"
                end tell

            on error errMsg
                logMessage("❌ Error exporting photos: " & errMsg)
                display dialog "Error exporting photos: " & errMsg buttons {"OK"} default button 1
            end try
        else
            logMessage("⚠️ No media items found in album '" & albumName & "'.")
            display dialog "No media items found in album '" & albumName & "'." buttons {"OK"} default button 1
        end if
    end timeout
end run

-- ========================================
-- Helper Functions
-- ========================================
on logMessage(messageText)
    do shell script "echo " & quoted form of messageText & " >> " & quoted form of debugLogPath
end logMessage