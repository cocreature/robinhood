{ rev ? "60ae563293ec956d683e3b9a62dbdef46f71437e",
  outputSha256 ? "0nqpmwlzvpwj2wwikappcn7lkbhb3jj076ydn6k1fcy8k0rhp3r5"
}:
let
  nixpkgs = builtins.fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/${rev}.tar.gz";
    sha256 = outputSha256;
  };
  pkgs = import nixpkgs {};
  robinhood-src = pkgs.lib.cleanSourceWith {
    src = pkgs.lib.cleanSource ./.;
    filter = path: type:
      let base = baseNameOf path;
      in !(pkgs.lib.hasPrefix ".ghc.environment." base) &&
         !(pkgs.lib.hasSuffix ".nix" base);
  };
  haskellPackages = pkgs.haskell.packages.ghc843.override(old: {
    all-cabal-hashes = builtins.fetchurl {
      url = "https://github.com/commercialhaskell/all-cabal-hashes/archive/e8599faa9bb6158597fa849fb9f466ee385fb0d9.tar.gz";
      sha256 = "19h087c16hh8kksm73cifzizzg46dk2ww755xz2i95z2nks82div";
    };
    overrides = self: super: {
      robinhood = pkgs.haskell.lib.overrideCabal (super.callCabal2nix "robinhood" robinhood-src {
      }) (old: {
        doBenchmark = true;
        configureFlags = "--enable-benchmarks";
      });
      primitive = super.primitive_0_6_4_0;
      criterion = super.criterion_1_5_2_0;
      vector-hashtables = super.callCabal2nix "vector-hashtables" (pkgs.fetchFromGitHub {
        owner = "klapaucius";
        repo = "vector-hashtables";
        rev = "688b5791237919b3054accb0abdeaf8394759e9e";
        sha256 = "0crn3xzn13kv2vxffr6pf4r55bsnmwrwl32vqnyxf0661r6qly0m";
      }) {};
    };
  });
in
{ robinhood = haskellPackages.robinhood;
}
