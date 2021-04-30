pip install --upgrade pip
pip install --user -e .
pip install -r .devcontainer/requirements.txt

mkdir -p /home/vscode/Documents/Encryption-keys
cp /home/kieran/Documents/Encryption-keys/ngeniusadmin.pem /home/vscode/Documents/Encryption-keys/ngeniusadmin.pem