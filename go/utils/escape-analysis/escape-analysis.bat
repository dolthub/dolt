:: This batch file is for Windows-users only, and will only work in a Windows environment.
:: File Watchers uses the Command Prompt, which eliminates any potential references to Unix tooling.
:: Refer to the .sh script for comments covering the Go flags.
:: All other commands are equivalent to their Unix counterparts.
:: Of note, the directory path must be preceded by ".\" due to Go's handling of paths in Windows.
go build -gcflags="-m=1 -N -l" ".\%1" 2>&1 >NUL | findstr /v /l "<does not escape>" | sort /unique