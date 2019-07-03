package osutil

// StartsWithWindowsVolume checks if the given string begins with a valid Windows Volume e.g. "C:" or "Z:"
func StartsWithWindowsVolume(p string) bool {
	if len(p) >= 2 && p[0] >= 'A' && p[0] <= 'Z' && p[1] == ':' {
		return true
	}
	return false
}