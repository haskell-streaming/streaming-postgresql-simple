{ nixpkgs ? import <nixpkgs> {}, compiler ? "default" }:

let

  inherit (nixpkgs) pkgs;

  f = { mkDerivation, base, postgresql-simple, stdenv, streaming }:
      mkDerivation {
        pname = "streaming-postgresql-simple";
        version = "0.1.0.0";
        src = ./.;
        libraryHaskellDepends = [ base postgresql-simple streaming ];
        license = stdenv.lib.licenses.bsd3;
      };

  haskellPackages = if compiler == "default"
                       then pkgs.haskellPackages
                       else pkgs.haskell.packages.${compiler};

  drv = haskellPackages.callPackage f {};

in

  if pkgs.lib.inNixShell then drv.env else drv
