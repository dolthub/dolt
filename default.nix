{ pkgs, go, self }:
(pkgs.buildGoModule.override { inherit go; }) {
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

  vendorHash = "sha256-cQRIb5EbWg1BULyrFyHuRJmcx//72R15STEONRbgQ+A=";

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
