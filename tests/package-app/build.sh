#!/usr/bin/env bash
# Packs SproutDB into a local package folder and builds the minimal app of this folder against
# that package, as a consumer of the NuGet package would: once as a build (Development) and once
# published (Production). The app uses a fresh NuGet packages folder, so nothing comes from
# earlier builds.
# Usage: bash tests/package-app/build.sh <work-dir>
#   <work-dir>/packages   the packed SproutDB package
#   <work-dir>/app        the app's project (copied from this folder)
#   <work-dir>/build      the build output: dotnet PackageApp.dll, started in <work-dir>/app
#   <work-dir>/publish    the published app: dotnet PackageApp.dll, started in <work-dir>/publish
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: $0 <work-dir>" >&2
  exit 2
fi

APP_SRC="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$APP_SRC/../.." && pwd)"
mkdir -p "$1"
WORK="$(cd "$1" && pwd)"
VERSION="0.0.0-local"

rm -rf "$WORK/packages" "$WORK/app" "$WORK/build" "$WORK/publish" "$WORK/nuget-packages"
mkdir -p "$WORK/app"

echo "-- pack SproutDB $VERSION"
dotnet pack "$REPO_ROOT/src/SproutDB.Core/SproutDB.Core.csproj" --configuration Release --nologo -v q \
  -p:Version="$VERSION" --output "$WORK/packages"

cp "$APP_SRC/PackageApp.csproj" "$APP_SRC/Program.cs" "$WORK/app/"
cat > "$WORK/app/nuget.config" <<XML
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <config>
    <add key="globalPackagesFolder" value="$WORK/nuget-packages" />
  </config>
  <packageSources>
    <clear />
    <add key="local" value="$WORK/packages" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
</configuration>
XML

echo "-- build the app (Development)"
dotnet build "$WORK/app/PackageApp.csproj" --configuration Release --nologo -v q \
  -p:SproutDBPackageVersion="$VERSION" --output "$WORK/build"

echo "-- publish the app (Production)"
dotnet publish "$WORK/app/PackageApp.csproj" --configuration Release --nologo -v q \
  -p:SproutDBPackageVersion="$VERSION" --output "$WORK/publish"
