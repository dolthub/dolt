{
  description = "Relational database with version control and CLI a-la Git";

  inputs = {
    /** Uncomment to use a specific release of NixOS.
      * NixOS releases new versions every 6 months. */
    # nixpkgs.url = "github:NixOS/nixpkgs/nixos-23.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, flake-utils, nixpkgs }: flake-utils.lib.eachDefaultSystem (system:
    let
      pkgs = nixpkgs.legacyPackages.${system};
      lib = nixpkgs.lib;
    in
    {
      packages.default = pkgs.buildGoModule {
        name = "dolt";

        /* Based on https://github.com/NixOS/nixpkgs/blob/master/pkgs/servers/sql/dolt/default.nix */
        pname = "dolt";

        src = ./.;

        modRoot = "./go";
        subPackages = [ "cmd/dolt" ];
        # Have to update every time the go dependencies change.
        vendorHash = "sha256-fT5WKgu1Qd3hWXU+zU08O11pd1AguUP85tgvxZWbESE=";
        proxyVendor = true;
        doCheck = false;

        meta = with lib; {
          description = "Relational database with version control and CLI a-la Git";
          homepage = "https://github.com/dolthub/dolt";
          license = licenses.asl20;
          maintainers = with maintainers; [ danbst ];
        };
      };

      #packages.default = self.packages.${system}.dolt;
    });
}
