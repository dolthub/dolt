#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail
if [[ "${TRACE-0}" == "1" ]]; then
    set -o xtrace
fi

usage() {
    cat <<'EOF'
Usage: mariadb-install.sh [-o output_file] [-d install_dir] <version>

Downloads a MariaDB bintar archive from archive.mariadb.org.
If -d is provided, the script also extracts and installs the archive directory.

Examples:
  mariadb-install.sh 11.8.3
  mariadb-install.sh -o mariadb-11.8.3.tar.gz 11.8.3
  mariadb-install.sh -o mariadb-11.8.3.tar.gz -d mariadb-11.8 11.8.3
EOF
}

main() {
    local output_file=""
    local install_dir=""
    local version=""

    while getopts ":o:d:h" opt; do
        case "$opt" in
            o) output_file="$OPTARG" ;;
            d) install_dir="$OPTARG" ;;
            h)
                usage
                exit 0
                ;;
            :)
                echo "Missing value for -$OPTARG." >&2
                usage >&2
                exit 1
                ;;
            \?)
                echo "Invalid option: -$OPTARG" >&2
                usage >&2
                exit 1
                ;;
        esac
    done
    shift $((OPTIND - 1))

    if [[ "${1-}" == "" || "${2-}" != "" ]]; then
        usage >&2
        exit 1
    fi
    version="$1"

    local archive_name
    archive_name="mariadb-${version}-linux-systemd-x86_64.tar.gz"
    local url
    url="https://archive.mariadb.org/mariadb-${version}/bintar-linux-systemd-x86_64/${archive_name}"

    # Use the URL basename when -o is not provided.
    if [[ -z "$output_file" ]]; then
        output_file="$archive_name"
    fi

    curl --fail --location --silent --show-error --output "$output_file" "$url"

    # When an install directory is provided, perform extract, rename, and cleanup.
    if [[ -n "$install_dir" ]]; then
        local extracted_dir
        extracted_dir="mariadb-${version}-linux-systemd-x86_64"

        tar -xzf "$output_file"
        if [[ ! -d "$extracted_dir" ]]; then
            echo "Expected extracted directory '$extracted_dir' was not found." >&2
            exit 1
        fi
        if [[ -e "$install_dir" ]]; then
            echo "Install directory '$install_dir' already exists." >&2
            exit 1
        fi

        mv "$extracted_dir" "$install_dir"
        rm "$output_file"
    fi
}

main "$@"
