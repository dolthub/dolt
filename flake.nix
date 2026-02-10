{
  description = "Dolt – a SQL database you can diff, branch, and merge";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";

    # TODO: Remove go-overlay as soon as 1.25.6 is published to nixpkgs-unstable.
    # Then this can be merged to main.
    go-overlay.url = "github:purpleclay/go-overlay";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      go-overlay,
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
          pkgs = import nixpkgs {
	    inherit system;
	    overlays = [ go-overlay.overlays.default ];
	  };
          dolt = pkgs.callPackage ./default.nix { inherit pkgs self; go = pkgs.go-bin.latestStable; };
        in
        {
          packages.default = dolt;

          apps.default = {
            type = "app";
            program = "${dolt}/bin/dolt";
          };

          devShells.default = pkgs.mkShell {
            buildInputs = with pkgs; [
              icu
            ];
            nativeBuildInputs = with pkgs; [
              go-bin.latestStable
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
