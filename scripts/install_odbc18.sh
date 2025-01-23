#Debian 9-11
#!/bin/bash

# Exit on any error
set -e

# Check if ODBC driver is already installed
check_driver() {
    if odbcinst -q -d | grep -q "ODBC Driver 18 for SQL Server"; then
        return 0  # Driver found
    else
        return 1  # Driver not found
    fi
}

echo "🔍 Checking if ODBC Driver is already installed..."
if check_driver; then
    echo "✅ ODBC Driver 18 is already installed!"
    echo "👇 Current ODBC drivers:"
    odbcinst -q -d
    exit 0
fi

echo "⏳ Installing ODBC Driver..."

echo "📦 Adding Microsoft repository..."
curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list

echo "🔄 Updating package list..."
sudo apt-get update

echo "📥 Installing ODBC Driver..."
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

echo "🛠️ Installing ODBC development tools..."
sudo apt-get install -y unixodbc-dev

echo "✅ ODBC Driver installation complete!"
# Verify installation
echo "👇 Installed ODBC drivers:"
odbcinst -q -d