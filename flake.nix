{
  description = "shrink-media development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};

      pythonEnv = pkgs.python312.withPackages (ps: with ps; [
        # Server dependencies
        fastapi
        uvicorn
        sqlalchemy
        httpx
        pydantic

        # Worker dependencies
        requests

        # Common
        aiofiles
      ]);
    in
    {
      devShells.${system}.default = pkgs.mkShell {
        packages = [
          pythonEnv
          pkgs.ffmpeg-full
          pkgs.p7zip
          pkgs.uv
          pkgs.openlist
        ];
        shellHook = ''
    export no_proxy=*
    if [ -f .env ]; then
      set -a
      . ./.env
      set +a
    fi
        '';
      };
    };
}
