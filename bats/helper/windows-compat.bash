skiponwindows() { :; }
	
unameOut="$(uname -s)"
case "${unameOut}" in
	CYGWIN*)    machine=Windows;;
	MINGW*)     machine=Windows;;
	*)          machine=Unix;;
esac

if [ ${machine} = "Windows" ]; then
	skiponwindows() {
		skip "$1"
	}
fi

if ! [ -x "$(command -v pkill)" ]; then
	pkill() {
		taskkill -fi "IMAGENAME eq $2*" -f
	}
fi

if ! [ -x "$(command -v pgrep)" ]; then
	pgrep() {
		tasklist -fi "IMAGENAME eq $1*"
	}
fi
