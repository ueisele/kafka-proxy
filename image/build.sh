#!/usr/bin/env bash
set -e
SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})

PUSH=false
BUILD=false

DOCKERREGISTRY_USER="docker.io/ueisele"

function usage () {
    echo "$0: $1" >&2
    echo
    echo "Usage: $0 [--build] [--push] [--user docker.io/ueisele]"
    echo
    return 1
}

function build_image () {  
    docker build \
        -t "${DOCKERREGISTRY_USER}/kafka-proxy:${VERSION}" \
        -f ${SCRIPT_DIR}/Dockerfile ${SCRIPT_DIR}/..
}

function build () {
    echo "Building Docker image for Kafka-Proxy with version ${VERSION}."
    build_image
}

function push_image () {
  docker push "${DOCKERREGISTRY_USER}/kafka-proxy:${VERSION}"
}

function push () {
    echo "Pushing Docker images for Kafka-Proxy version ${VERSION}."
    push_image
}

function parseCmd () {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --build)
                BUILD=true
                shift
                ;;
            --push)
                PUSH=true
                shift
                ;;
            --user)
                shift
                case "$1" in
                    ""|--*)
                        usage "Requires Docker registry user name"
                        return 1
                        ;;
                    *)
                        DOCKERREGISTRY_USER="$1"
                        shift
                        ;;
                esac
                ;;
            *)
                local param="$1"
                shift
                case "$1" in
                    ""|--*)
                        echo "WARN: Unknown option: ${param}"
                        ;;
                    *)
                        echo "WARN: Unknown option: ${param} $1"
                        shift
                        ;;
                esac
                ;;
        esac
    done
    
    VERSION=$(${SCRIPT_DIR}/../sbtx -batch "show version" | tail -n1 | sed -e "s/^\[info] //")

    return 0
}

function main () {
    parseCmd "$@"
    local retval=$?
    if [ $retval != 0 ]; then
        exit $retval
    fi

    if [ "$BUILD" = true ]; then
        build
    fi
    if [ "$PUSH" = true ]; then
        push
    fi
}

main "$@"