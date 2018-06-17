{ rev ? "95a8cb3ade1ad0e2649f0f3128e90c53964af5e1",
  outputSha256 ? "0jxn25l8d65h6qnmx9f4dsi2fscqyxc0gvhnkj1bc7kicn9rr5hj",
}:
let
  nixpkgs = builtins.fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/${rev}.tar.gz";
    sha256 = outputSha256;
  };
  pkgs = import nixpkgs {};
  haskellPackages = pkgs.haskell.packages.ghc843.override(old: {
    overrides = self: super: {
      robinhood = pkgs.haskell.lib.doBenchmark (super.callCabal2nix "robinhood" ./. {});
      primitive = super.callHackage "primitive" "0.6.4.0" {};
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
