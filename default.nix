{ rev ? "0b1a0e33309f22274f30bfae2e04377314262779",
  outputSha256 ? "0kvrknl5nfr0fkqcxinkwvs12ldqm1j6xzq34bbdx3i4zwy99135"
}:
let
  nixpkgs = builtins.fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/${rev}.tar.gz";
    sha256 = outputSha256;
  };
  pkgs = import nixpkgs {};
  haskellPackages = (pkgs.haskell.packages.ghc842.extend (pkgs.haskell.lib.packageSourceOverrides {
    robinhood = ./.;
    primitive = ./primitive;
  }));
in
{ robinhood = haskellPackages.robinhood;
}
