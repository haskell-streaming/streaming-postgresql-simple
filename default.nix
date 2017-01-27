{ mkDerivation, base, bytestring, exceptions, postgresql-libpq
, postgresql-simple, resourcet, stdenv, streaming, transformers
, safe-exceptions
}:
mkDerivation {
  pname = "streaming-postgresql-simple";
  version = "0.1.0.0";
  src = ./.;
  libraryHaskellDepends = [
    base bytestring exceptions postgresql-libpq postgresql-simple
    resourcet streaming transformers safe-exceptions
  ];
  license = stdenv.lib.licenses.bsd3;
}
