FROM debian:bookworm-slim

# Install dependencies if needed
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl-dev \
    libsqlite3-dev \
    curl \
    build-essential \
    libclang-dev \
    clang \
    sudo \
    pkg-config

RUN apt update && apt install -y dpkg-dev

# install rust
ENV PATH="/root/.cargo/bin:${PATH}"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN . $HOME/.cargo/env

# Build miden-node
COPY . /miden-node
WORKDIR /miden-node

# BUILD PACKAGE

# Create package directories
RUN mkdir -p packaging/deb/miden-node/lib/systemd/system/ \
    packaging/deb/miden-node/DEBIAN/ \
    packaging/deb/miden-faucet/lib/systemd/system/ \
    packaging/deb/miden-faucet/DEBIAN/ \
    packaging/deb/miden-node/usr/bin/ \
    packaging/deb/miden-faucet/usr/bin/

# Copy package install scripts
RUN cp packaging/node/miden-node.service packaging/deb/miden-node/lib/systemd/system/ && \
    cp packaging/node/postinst packaging/deb/miden-node/DEBIAN/ && \
    cp packaging/node/postrm packaging/deb/miden-node/DEBIAN/ && \
    cp packaging/faucet/miden-faucet.service packaging/deb/miden-faucet/lib/systemd/system/ && \
    cp packaging/faucet/postinst packaging/deb/miden-faucet/DEBIAN/ && \
    cp packaging/faucet/postrm packaging/deb/miden-faucet/DEBIAN/

RUN chmod 0775 packaging/deb/miden-node/DEBIAN/postinst && \
    chmod 0775 packaging/deb/miden-node/DEBIAN/postrm && \
    chmod 0775 packaging/deb/miden-faucet/DEBIAN/postinst && \
    chmod 0775 packaging/deb/miden-faucet/DEBIAN/postrm

# Create control files
RUN arch=$(dpkg --print-architecture) && \
    echo "Package: miden-node\nVersion: 0.8\nSection: base\nPriority: optional\nArchitecture: arm64\nMaintainer: Polygon Devops <devops@polygon.technology>\nDescription: miden-node binary package\nHomepage: https://polygon.technology/polygon-miden\nVcs-Git: git@github.com:0xMiden/miden-node.git\nVcs-Browser: https://github.com/0xMiden/miden-node" > packaging/deb/miden-node/DEBIAN/control && \
    echo "Package: miden-faucet\nVersion: 0.8\nSection: base\nPriority: optional\nArchitecture: arm64\nMaintainer: Polygon Devops <devops@polygon.technology>\nDescription: miden-faucet binary package\nHomepage: https://polygon.technology/polygon-miden\nVcs-Git: git@github.com:0xMiden/miden-node.git\nVcs-Browser: https://github.com/0xMiden/miden-node" > packaging/deb/miden-faucet/DEBIAN/control

# Build binaries
RUN cargo install miden-node   --root . --locked
RUN cargo install miden-faucet --root . --locked

# Copy binary files
RUN cp -p ./bin/miden-node packaging/deb/miden-node/usr/bin/ && \
    cp -p ./bin/miden-faucet packaging/deb/miden-faucet/usr/bin/

# Build packages
RUN dpkg-deb --build --root-owner-group packaging/deb/miden-node && \
    dpkg-deb --build --root-owner-group packaging/deb/miden-faucet && \
    mv packaging/deb/*.deb . && \
    rm -rf packaging

# DEPLOY

# sudo systemctl stop miden-node || true
# sudo systemctl stop miden-faucet || true
# RUN sudo apt remove miden-node miden-faucet -y || true
# RUN sudo dpkg -i miden-node.deb
# RUN sudo dpkg -i miden-faucet.deb
# RUN if [ ! -f /opt/miden-node/miden-store.sqlite3 ]; then \
#         sudo /usr/bin/miden-node bundled bootstrap --data-directory /opt/miden-node --accounts-directory /opt/miden-faucet; \
#         sudo /usr/bin/miden-faucet init -c /etc/opt/miden-faucet/miden-faucet.toml -f /opt/miden-faucet/account.mac; \
#     fi
# RUN sudo chown -R miden-node /opt/miden-node
# RUN sudo chown -R miden-faucet /opt/miden-faucet

CMD ["miden-node"]
