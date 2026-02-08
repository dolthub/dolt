{
  description = "Dolt – a SQL database you can diff, branch, and merge";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachSystem
      [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ]
      (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          dolt = pkgs.callPackage ./default.nix { inherit pkgs self; };
        in
        {
          packages.default = dolt;

          apps.default = {
            type = "app";
            program = "${dolt}/bin/dolt";
          };

          devShells.default = pkgs.mkShell {
            buildInputs = with pkgs; [
              go
              git
              gopls
              gotools
            ];

            shellHook = ''
              echo "Dolt development shell"
              echo "Go version: $(go version)"
            '';
          };
        }
      );
}
