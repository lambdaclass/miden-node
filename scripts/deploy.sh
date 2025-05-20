!/bin/bash

# sudo systemctl stop miden-node || true
# sudo systemctl stop miden-faucet || true

# this script reinstalls the node and faucet packages
# bootstraps the node and faucet and runs them
sudo apt remove miden-node miden-faucet -y || true
sudo dpkg -i miden-node.deb
sudo dpkg -i miden-faucet.deb

if [ $reset == "true" ]; then \
    echo "Resetting the database"
    sudo rm -rf /opt/miden-faucet/*; \
    sudo rm -rf /opt/miden-node/*; \
fi; \
if [ ! -f /opt/miden-node/miden-store.sqlite3 ]; then \
    echo "Bootstrapping the database"
    sudo /usr/bin/miden-node bundled bootstrap --data-directory /opt/miden-node --accounts-directory /opt/miden-faucet; \
fi; \
sudo /usr/bin/miden-faucet init -c /etc/opt/miden-faucet/miden-faucet.toml -f /opt/miden-faucet/account.mac; \
sudo chown -R miden-node /opt/miden-node; \
sudo chown -R miden-faucet /opt/miden-faucet;

miden-node bundled start --rpc.url http://0.0.0.0:57291 --data-directory /opt/miden-node & 
miden-faucet start --config /etc/opt/miden-faucet/miden-faucet.toml
