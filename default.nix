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

  # Placeholder hash — the first CI run will fail and print the correct value.
  # Replace with the "got:" hash from the nix build log.
  vendorHash = pkgs.lib.fakeHash;

  # nixpkgs may lag behind the Go version in go.mod.
  # GOTOOLCHAIN=auto lets the Go toolchain download the required version.
  env.GOTOOLCHAIN = "auto";

  nativeBuildInputs = [ pkgs.git ];

  meta = with pkgs.lib; {
    description = "Dolt – a SQL database you can diff, branch, and merge";
    homepage = "https://github.com/dolthub/dolt";
    license = licenses.asl20;
    mainProgram = "dolt";
    maintainers = [ ];
  };
}
