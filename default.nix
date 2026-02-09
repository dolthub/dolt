{ pkgs, self }:
pkgs.buildGoModule {
  pname = "dolt";
  version =
    let
      versionFile = builtins.readFile ./go/cmd/dolt/doltversion/version.go;
      match = builtins.match ".*Version = \"([0-9]+\\.[0-9]+\\.[0-9]+)\".*" versionFile;
    in
    if match != null then builtins.head match else "0.0.0-unknown";

  src = self;
  modRoot = "./go";

  subPackages = [ "cmd/dolt" ];
  doCheck = false;

  # vendorHash must be updated when go.mod/go.sum change. When incorrect,
  # `nix build` will fail and print the correct hash in the "got:" line.
  # To compute without nix installed:
  #   docker run --rm -v $(pwd):/workspace -w /workspace nixos/nix \
  #     sh -c 'echo "experimental-features = nix-command flakes" >> /etc/nix/nix.conf && nix build .#default 2>&1'
  vendorHash = pkgs.lib.fakeHash;

  nativeBuildInputs = [ pkgs.git ];
  buildInputs = [ pkgs.icu ];

  meta = with pkgs.lib; {
    description = "Dolt – a SQL database you can diff, branch, and merge";
    homepage = "https://github.com/dolthub/dolt";
    license = licenses.asl20;
    mainProgram = "dolt";
    maintainers = [ ];
  };
}
