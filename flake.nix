{
  description = "Basic devshell for polybase";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
    naersk.url = "github:nix-community/naersk";
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay, ... } @ inputs:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };

      in

      {
        devShells.default = pkgs.mkShell {

          buildInputs = with pkgs; [
            clang # required for rocksdb
          ];

          nativeBuildInputs = with pkgs; [
            rust-bin.stable.latest.default

            pkg-config
            openssl
            protobuf
          ];

          LIBCLANG_PATH = pkgs.libclang.lib + "/lib/";
        };
      });
}
