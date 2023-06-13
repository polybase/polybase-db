{
  description = "Basic devshell for polybase";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
    naersk.url = "github:nix-community/naersk";
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay, ... }@inputs:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };

      in {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            # chose this date because the metadata format works with today's rust-analyzer 🤭
            (rust-bin.nightly."2023-01-01".default.override {
              extensions = [ "rust-src" ];
            })

            pkg-config
            openssl
            protobuf

            clang # required for rocksdb
          ];

          LIBCLANG_PATH = "${pkgs.libclang.lib}/lib/";
          RUST_SRC_PATH =
            "${pkgs.rust-bin.stable.latest.default}/lib/rustlib/src/rust/library";
        };
      });
}
