-- ========================================
-- Configurable Properties
-- ========================================
property topFolderName : "Media Organizer on LaCie"
property midFolderName : "Google Photos Pipeline"
property bottomFolderName : "MonthlyExports"
property destinationFolderPath : "/Volumes/LaCie/Media Organizer/Google Photos/01-MonthlyExports"
property debugLogPath : "/Users/nickolaycohen/dev/media-organizer-pipeline/logs/media_organizer.log"

-- ========================================
-- Main
-- ========================================
on run argv
	with timeout of 600 seconds
		set albumName to item 1 of argv
		
		-- Clear previous debug log
		do shell script "echo '' > " & quoted form of debugLogPath
		
		-- Prepare destination folder
		set fullDestinationPath to destinationFolderPath & "/" & albumName
		set folderCheckCmd to "test -d " & quoted form of fullDestinationPath & " && echo exists || echo missing"
		set folderStatus to do shell script folderCheckCmd
		
		--if folderStatus is "exists" then
		--	set timestamp to do shell script "date +%Y%m%d-%H%M"
		--	set archivedPath to fullDestinationPath & "__" & timestamp
		--	do shell script "mv " & quoted form of fullDestinationPath & " " & quoted form of archivedPath
		--	logMessage("Existing folder found. Renamed to: " & archivedPath)
		--end if
		
		do shell script "mkdir -p " & quoted form of fullDestinationPath
		set destinationFolder to POSIX file fullDestinationPath as alias
		
		logMessage("Starting export process for album: " & albumName)
		
		-- ========================================
		-- Locate Album
		-- ========================================
		set targetAlbum to my findAlbum(albumName)
		if targetAlbum is missing value then return
		
		-- ========================================
		-- Validate Staging vs Album Counts
		-- ========================================
		tell application "Photos"
			set albumCount to count of media items of targetAlbum
		end tell
		set fileCountCmd to "ls -1 " & quoted form of fullDestinationPath & " | wc -l"
		set stagingCount to (do shell script fileCountCmd) as integer
		
		if albumCount < stagingCount then
			my logMessage("⚠️ Album count (" & albumCount & ") is smaller than staging count (" & stagingCount & "). Resetting staging folder.")
			do shell script "rm -rf " & quoted form of fullDestinationPath & "/*"
		else
			my logMessage("ℹ️ Album count (" & albumCount & ") >= staging count (" & stagingCount & "). Using incremental export logic.")
		end if
		
		-- ========================================
		-- Export Media Items in Batches
		-- ========================================
		tell application "Photos"
			set mediaItems to media items of targetAlbum
		end tell
		set fileCountCmd to "ls -1 " & quoted form of fullDestinationPath & " | wc -l"
		set stagingCount to (do shell script fileCountCmd) as integer
		
		if albumCount < stagingCount then
			my logMessage("⚠️ Album count (" & albumCount & ") is smaller than staging count (" & stagingCount & "). Resetting staging folder.")
			do shell script "rm -rf " & quoted form of fullDestinationPath & "/*"
		else
			my logMessage("ℹ️ Album count (" & albumCount & ") >= staging count (" & stagingCount & "). Using incremental export logic.")
		end if
		
		if (count of mediaItems) = 0 then
			my logMessage("⚠️ No media items found in album '" & albumName & "'.")
			display dialog "No media items found in album '" & albumName & "'." buttons {"OK"} default button 1
			return
		end if
		
		set batchSize to 50
		set totalItems to count of mediaItems
		set startIndex to 1
		
		repeat while startIndex ≤ totalItems
			set endIndex to startIndex + batchSize - 1
			if endIndex > totalItems then set endIndex to totalItems
			set batchItems to items startIndex thru endIndex of mediaItems
			my logMessage("startIndex: " & startIndex)
			my logMessage("endIndex: " & endIndex)
			my logMessage("destinationFolder: " & destinationFolder)
			my logMessage("fullDestinationPath: " & fullDestinationPath)
			
			my exportBatch(batchItems, destinationFolder, fullDestinationPath)
			
			set startIndex to endIndex + 1
		end repeat
		
		logMessage("✅ Export completed successfully for album: " & albumName)
		tell application "System Events"
			display notification "Export of '" & albumName & "' completed successfully!" with title "Photos Export"
		end tell
	end timeout
end run

-- ========================================
-- Helper Functions
-- ========================================
on logMessage(messageText)
	do shell script "echo " & quoted form of messageText & " >> " & quoted form of debugLogPath
end logMessage

on findAlbum(albumName)
	tell application "Photos"
		-- Top Folder
		set topContainer to missing value
		repeat with f in folders
			if (name of f) is equal to topFolderName then
				set topContainer to f
				exit repeat
			end if
		end repeat
		if topContainer is missing value then
			logMessage("Top folder '" & topFolderName & "' not found.")
			display dialog "Top folder '" & topFolderName & "' not found." buttons {"OK"} default button 1
			return missing value
		end if
		
		-- Mid Folder
		set midContainer to missing value
		repeat with f in folders of topContainer
			if (name of f) is equal to midFolderName then
				set midContainer to f
				exit repeat
			end if
		end repeat
		if midContainer is missing value then
			logMessage("Mid folder '" & midFolderName & "' not found.")
			display dialog "Mid folder '" & midFolderName & "' not found." buttons {"OK"} default button 1
			return missing value
		end if
		
		-- Bottom Folder
		set bottomContainer to missing value
		repeat with f in folders of midContainer
			if (name of f) is equal to bottomFolderName then
				set bottomContainer to f
				exit repeat
			end if
		end repeat
		if bottomContainer is missing value then
			logMessage("Bottom folder '" & bottomFolderName & "' not found.")
			display dialog "Bottom folder '" & bottomFolderName & "' not found." buttons {"OK"} default button 1
			return missing value
		end if
		
		-- Album
		set targetAlbum to missing value
		repeat with a in albums of bottomContainer
			if (name of a) is equal to albumName then
				set targetAlbum to a
				exit repeat
			end if
		end repeat
		if targetAlbum is missing value then
			logMessage("Album '" & albumName & "' not found inside '" & bottomFolderName & "'.")
			display dialog "Album '" & albumName & "' not found inside '" & bottomFolderName & "'." buttons {"OK"} default button 1
			return missing value
		end if
	end tell
	
	return targetAlbum
end findAlbum

on exportBatch(batchItems, destinationFolder, fullDestinationPath)
	set maxRetries to 3
	repeat with attempt from 1 to maxRetries
		try
			set itemsToExport to {}
			tell application "Photos"
				repeat with i from 1 to count of batchItems
					set thisItem to item i of batchItems
					set itemName to filename of thisItem
					set itemPath to fullDestinationPath & "/" & itemName
					
					set fileCheckCmd to "test -f " & quoted form of itemPath & " && echo exists || echo missing"
					set fileStatus to do shell script fileCheckCmd
					
					if fileStatus is "exists" then
						my logMessage("⏭ Skipping existing file: " & itemName)
					else
						copy thisItem to end of itemsToExport
						my logMessage("📝 Queued for export: " & itemName)
					end if
				end repeat
				
				if (count of itemsToExport) > 0 then
					export itemsToExport to destinationFolder with using originals
					my logMessage("⬇️ Exported batch of " & (count of itemsToExport) & " items")
				else
					my logMessage("✅ Nothing new to export in this batch")
				end if
			end tell
			exit repeat
		on error errMsg
			my logMessage("❌ Attempt " & attempt & " failed: " & errMsg)
			if attempt = maxRetries then
				display dialog "Export failed after 3 attempts: " & errMsg buttons {"OK"} default button 1
			end if
		end try
	end repeat
end exportBatch