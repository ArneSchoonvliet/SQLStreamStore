FROM mcr.microsoft.com/dotnet/sdk:6.0-alpine

RUN apk update \
    && apk add git \
    && apk add docker-cli \
    && apk add zip

# Workaround for https://github.com/grpc/grpc/issues/18428
# Also see https://github.com/sgerrand/alpine-pkg-glibc
# https://stackoverflow.com/a/61268529

ENV GLIBC_VER=2.32-r0

RUN apk update && apk --no-cache add ca-certificates wget \
    && wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub \
    && wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/${GLIBC_VER}/glibc-${GLIBC_VER}.apk \
    && wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/${GLIBC_VER}/glibc-bin-${GLIBC_VER}.apk \
    && apk add --no-cache \
        glibc-${GLIBC_VER}.apk \
        glibc-bin-${GLIBC_VER}.apk \
    && rm glibc-${GLIBC_VER}.apk \
    && rm glibc-bin-${GLIBC_VER}.apk \
    && rm -rf /var/cache/apk/*

    
WORKDIR /repo

# https://github.com/moby/moby/issues/15858
# Docker will flatten out the file structure on COPY
# We don't want to specify each csproj either - it creates pointless layers and it looks ugly
# https://code-maze.com/aspnetcore-app-dockerfiles/
COPY ./*.sln ./

COPY ./build/ ./build/

COPY ./src/*/*.csproj ./src/
RUN for file in $(ls src/*.csproj); do mkdir -p ./${file%.*}/ && mv $file ./${file%.*}/; done

COPY ./tests/*/*.csproj ./tests/
RUN for file in $(ls tests/*.csproj); do mkdir -p ./${file%.*}/ && mv $file ./${file%.*}/; done

RUN dotnet restore

COPY ./assets ./assets/

COPY ./src ./src/

COPY ./tests ./tests/

WORKDIR /repo/build

COPY ./build/build.csproj .

RUN dotnet restore

COPY ./build .

WORKDIR /repo
