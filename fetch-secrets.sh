#!/bin/sh
# Set Vault address and token
VAULT_ADDR="http://192.168.56.1:8200"
VAULT_TOKEN="rahul"

export VAULT_ADDR
export VAULT_TOKEN

username=$(vault kv get -field=username secret/info)
password=$(vault kv get -field=password secret/info)

echo "USERNAME=$username" > secrets-output/secrets.env
echo "PASSWORD=$password" >> secrets-output/secrets.env
chmod 600 secrets-output/secrets.env

# Debugging: List the contents of the secrets directory
echo "Contents of secrets-output directory:"
ls -l secrets-output

# Debugging: Print the contents of the secrets file
echo "Contents of secrets.env:"
cat secrets-output/secrets.env
