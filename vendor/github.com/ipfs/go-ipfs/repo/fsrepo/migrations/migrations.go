package mfsr

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

var DistPath = "https://ipfs.io/ipfs/QmNjXP6N98fYT1i7abgeBHmdR5WoeBeik4DtGiX9iFWK31"

func init() {
	if dist := os.Getenv("IPFS_DIST_PATH"); dist != "" {
		DistPath = dist
	}
}

const migrations = "fs-repo-migrations"

func migrationsBinName() string {
	switch runtime.GOOS {
	case "windows":
		return migrations + ".exe"
	default:
		return migrations
	}
}

func RunMigration(newv int) error {
	migrateBin := migrationsBinName()

	fmt.Println("  => Looking for suitable fs-repo-migrations binary.")

	var err error
	migrateBin, err = exec.LookPath(migrateBin)
	if err == nil {
		// check to make sure migrations binary supports our target version
		err = verifyMigrationSupportsVersion(migrateBin, newv)
	}

	if err != nil {
		fmt.Println("  => None found, downloading.")

		loc, err := GetMigrations()
		if err != nil {
			fmt.Println("  => Failed to download fs-repo-migrations.")
			return err
		}

		err = verifyMigrationSupportsVersion(loc, newv)
		if err != nil {
			return fmt.Errorf("no fs-repo-migration binary found for version %d: %s", newv, err)
		}

		migrateBin = loc
	}

	cmd := exec.Command(migrateBin, "-to", fmt.Sprint(newv), "-y")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	fmt.Printf("  => Running: %s -to %d -y\n", migrateBin, newv)

	err = cmd.Run()
	if err != nil {
		fmt.Printf("  => Failed: %s -to %d -y\n", migrateBin, newv)
		return fmt.Errorf("migration failed: %s", err)
	}

	fmt.Printf("  => Success: fs-repo has been migrated to version %d.\n", newv)

	return nil
}

func GetMigrations() (string, error) {
	latest, err := GetLatestVersion(DistPath, migrations)
	if err != nil {
		return "", fmt.Errorf("failed to find latest fs-repo-migrations: %s", err)
	}

	dir, err := ioutil.TempDir("", "go-ipfs-migrate")
	if err != nil {
		return "", fmt.Errorf("failed to create fs-repo-migrations tempdir: %s", err)
	}

	out := filepath.Join(dir, migrationsBinName())

	err = GetBinaryForVersion(migrations, migrations, DistPath, latest, out)
	if err != nil {
		return "", fmt.Errorf("failed to download latest fs-repo-migrations: %s", err)
	}

	err = os.Chmod(out, 0755)
	if err != nil {
		return "", err
	}

	return out, nil
}

func verifyMigrationSupportsVersion(fsrbin string, vn int) error {
	sn, err := migrationsVersion(fsrbin)
	if err != nil {
		return err
	}

	if sn >= vn {
		return nil
	}

	return fmt.Errorf("migrations binary doesnt support version %d: %s", vn, fsrbin)
}

func migrationsVersion(bin string) (int, error) {
	out, err := exec.Command(bin, "-v").CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("failed to check migrations version: %s", err)
	}

	vs := strings.Trim(string(out), " \n\t")
	vn, err := strconv.Atoi(vs)
	if err != nil {
		return 0, fmt.Errorf("migrations binary version check did not return a number: %s", err)
	}

	return vn, nil
}

func GetVersions(ipfspath, dist string) ([]string, error) {
	rc, err := httpFetch(ipfspath + "/" + dist + "/versions")
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	var out []string
	scan := bufio.NewScanner(rc)
	for scan.Scan() {
		out = append(out, scan.Text())
	}

	return out, nil
}

func GetLatestVersion(ipfspath, dist string) (string, error) {
	vs, err := GetVersions(ipfspath, dist)
	if err != nil {
		return "", err
	}
	var latest string
	for i := len(vs) - 1; i >= 0; i-- {
		if !strings.Contains(vs[i], "-dev") {
			latest = vs[i]
			break
		}
	}
	if latest == "" {
		return "", fmt.Errorf("couldnt find a non dev version in the list")
	}
	return vs[len(vs)-1], nil
}

func httpGet(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("http.NewRequest error: %s", err)
	}

	req.Header.Set("User-Agent", "go-ipfs")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http.DefaultClient.Do error: %s", err)
	}

	return resp, nil
}

func httpFetch(url string) (io.ReadCloser, error) {
	resp, err := httpGet(url)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		mes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error reading error body: %s", err)
		}

		return nil, fmt.Errorf("GET %s error: %s: %s", url, resp.Status, string(mes))
	}

	return resp.Body, nil
}

func GetBinaryForVersion(distname, binnom, root, vers, out string) error {
	dir, err := ioutil.TempDir("", "go-ipfs-auto-migrate")
	if err != nil {
		return err
	}

	var archive string
	switch runtime.GOOS {
	case "windows":
		archive = "zip"
		binnom += ".exe"
	default:
		archive = "tar.gz"
	}
	osv, err := osWithVariant()
	if err != nil {
		return err
	}
	finame := fmt.Sprintf("%s_%s_%s-%s.%s", distname, vers, osv, runtime.GOARCH, archive)
	distpath := fmt.Sprintf("%s/%s/%s/%s", root, distname, vers, finame)

	data, err := httpFetch(distpath)
	if err != nil {
		return err
	}

	arcpath := filepath.Join(dir, finame)
	fi, err := os.Create(arcpath)
	if err != nil {
		return err
	}

	_, err = io.Copy(fi, data)
	if err != nil {
		return err
	}
	fi.Close()

	return unpackArchive(distname, binnom, arcpath, out, archive)
}

// osWithVariant returns the OS name with optional variant.
// Currently returns either runtime.GOOS, or "linux-musl".
func osWithVariant() (string, error) {
	if runtime.GOOS != "linux" {
		return runtime.GOOS, nil
	}

	// ldd outputs the system's kind of libc.
	// - on standard ubuntu: ldd (Ubuntu GLIBC 2.23-0ubuntu5) 2.23
	// - on alpine: musl libc (x86_64)
	//
	// we use the combined stdout+stderr,
	// because ldd --version prints differently on different OSes.
	// - on standard ubuntu: stdout
	// - on alpine: stderr (it probably doesn't know the --version flag)
	//
	// we supress non-zero exit codes (see last point about alpine).
	out, err := exec.Command("sh", "-c", "ldd --version || true").CombinedOutput()
	if err != nil {
		return "", err
	}

	// now just see if we can find "musl" somewhere in the output
	scan := bufio.NewScanner(bytes.NewBuffer(out))
	for scan.Scan() {
		if strings.Contains(scan.Text(), "musl") {
			return "linux-musl", nil
		}
	}

	return "linux", nil
}
