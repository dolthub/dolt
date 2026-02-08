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

  # Placeholder hash — cannot be computed until nixpkgs ships a Go version
  # that satisfies go.mod (currently requires >= 1.25.6, nixpkgs has 1.25.5).
  # Once nixpkgs catches up, the build will fail with a hash mismatch and
  # print the correct value in the "got:" line of the error output.
  vendorHash = pkgs.lib.fakeHash;

  # Note: GOTOOLCHAIN=auto does NOT work inside the nix build sandbox because
  # the sandbox has no network access, so Go cannot download a newer toolchain.
  # When nixpkgs Go is older than what go.mod requires, the build will fail.
  # This is intentional — it is the signal this CI job exists to surface.

  nativeBuildInputs = [ pkgs.git ];

  meta = with pkgs.lib; {
    description = "Dolt – a SQL database you can diff, branch, and merge";
    homepage = "https://github.com/dolthub/dolt";
    license = licenses.asl20;
    mainProgram = "dolt";
    maintainers = [ ];
  };
}
