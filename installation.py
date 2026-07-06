import os
import subprocess
import sys
from pathlib import Path


def run(command, check=True):
    print("\n" + "=" * 80)
    print(f"Running:\n{command}")
    print("=" * 80)

    result = subprocess.run(command, shell=True)

    if check and result.returncode != 0:
        print(f"\nERROR: Command failed ({result.returncode})")
        sys.exit(result.returncode)

    return result


def write_phpmyadmin_conf():
    conf = """Alias /phpmyadmin /usr/share/phpmyadmin

<Directory /usr/share/phpmyadmin>
    Options FollowSymLinks
    DirectoryIndex index.php
    Require all granted
</Directory>
"""

    path = "/etc/apache2/conf-available/phpmyadmin.conf"

    with open(path, "w") as f:
        f.write(conf)

    print(f"Created {path}")


def main():

    if os.geteuid() != 0:
        print("Please run using sudo.")
        sys.exit(1)

    home = Path.home()
    code_dir = home / "code"

    ###################################################
    # 1.2 Create a Python Virtual Environment
    ###################################################

    run("apt install python3-venv -y")
    run(f"python3 -m venv {home}/jarvis")

    ###################################################
    # 2.1 Install MariaDB
    ###################################################

    run("apt update")
    run("apt upgrade -y")
    run("apt install mariadb-server -y")
    run("systemctl status mariadb", check=False)

    ###################################################
    # 2.2 Secure MariaDB (interactive)
    ###################################################

    print("\n")
    print("=" * 80)
    print("MariaDB Secure Installation")
    print("=" * 80)
    print("This step is interactive.")
    print("Recommended responses:")
    print("  - Press Enter (default root login)")
    print("  - Switch to unix_socket authentication -> Y")
    print("  - Change root password -> Y (example: 0SmartPower0)")
    print("  - Remove anonymous users -> Y")
    print("  - Disallow remote root login -> Y")
    print("  - Remove test database -> Y")
    print("  - Reload privilege tables -> Y")
    print("")

    run("mariadb-secure-installation")

    ###################################################
    # 2.3 Install MariaDB Connector & Dependencies
    ###################################################

    run("apt install libmariadb3 libmariadb-dev -y")

    ###################################################
    # 2.4 Install Apache Web Server
    ###################################################

    run("apt install apache2 -y")
    run("systemctl enable --now apache2")
    run("systemctl status apache2", check=False)

    ###################################################
    # 2.5 Install PHP and MySQL Extensions
    ###################################################

    run("apt install php php-mysql -y")
    run("systemctl restart apache2")

    ###################################################
    # 2.6 Install phpMyAdmin
    ###################################################

    run("apt install phpmyadmin -y")

    write_phpmyadmin_conf()

    run("a2enmod alias")
    run("a2enmod php8.4")
    run("a2enconf phpmyadmin")
    run("systemctl reload apache2")

    ###################################################
    # 2.7 Install Additional PHP Extensions
    ###################################################

    run("apt install php-json php-zip php-mbstring php-xml php-curl php-gd libapache2-mod-php -y")
    run("php -v", check=False)
    run("systemctl restart apache2")

    ###################################################
    # 3.1 Install CURL
    ###################################################

    run("apt install curl unzip -y")
    run("curl -V", check=False)

    ###################################################
    # 3.2 Install Composer (PHP Dependency Manager)
    ###################################################

    run("curl -sS https://getcomposer.org/installer | php")
    run("mv composer.phar /usr/local/bin/composer")
    run("composer --version", check=False)

    ###################################################
    # 3.3 Install Git
    ###################################################

    run("apt install git -y")
    run("git -v", check=False)

    ###################################################
    # 3.4 Install Node.js and npm
    ###################################################

    run("curl -fsSL https://deb.nodesource.com/setup_lts.x | bash -")
    run("apt install nodejs -y")
    run("node -v", check=False)
    run("npm -v", check=False)

    # Install npm if missing
    run("apt install npm -y", check=False)

    # Install build tools
    run("apt install build-essential -y")

    ###################################################
    # 4.1 Create Project Directory
    ###################################################

    code_dir.mkdir(exist_ok=True)

    ###################################################
    # 4.2 Gateway Repository (Python)
    ###################################################

    os.chdir(code_dir)

    if not (code_dir / "uratex_gateway").exists():
        run("git clone https://github.com/JKC-Ltd/uratex_gateway.git")

    activate = f"{home}/jarvis/bin/activate"

    run(
        f"bash -c 'source {activate} && "
        "pip install pymodbus[all] mysql-connector-python'"
    )

    ###################################################
    # 4.3 Web Application Repository (Laravel)
    ###################################################

    os.chdir(code_dir)

    if not (code_dir / "uratex").exists():
        run("git clone https://github.com/JKC-Ltd/uratex.git")

    os.chdir(code_dir / "uratex")

    run("cp .env.example .env")
    run("composer install")
    run("npm install")
    run("npm run build")
    run("php artisan key:generate")

    ###################################################
    # 4.5 Run Database Migrations
    ###################################################

    run("php artisan migrate --seed")

    ###################################################
    # 5. Cron Job Configuration
    ###################################################

    cron_job = "*/5 * * * * /bin/bash -c 'source /home/ryan/jarvis/bin/activate && python3 /home/ryan/code/uratex_gateway/index.py'"

    print("\n" + "=" * 80)
    print("Cron Job Setup")
    print("=" * 80)
    print("Adding the following cron job:")
    print(f"  {cron_job}")
    print("")

    # Add cron job (avoids duplicates by checking if it already exists)
    run(
        f"(crontab -l 2>/dev/null | grep -F 'uratex_gateway/index.py') || "
        f"(crontab -l 2>/dev/null; echo '{cron_job}') | crontab -"
    )

    run("crontab -l", check=False)

    ###################################################
    # 5.4 Enable Cron Service
    ###################################################

    run("systemctl start cron")
    run("systemctl enable cron")

    ###################################################
    # 5.5 Make Gateway Script Executable
    ###################################################

    run("chmod +x /home/ryan/code/uratex_gateway/index.py")

    ###################################################
    # 6. ENMMS Gateway systemd Service
    ###################################################

    service_content = """[Unit]
Description=ENMMS Gateway Service
After=network.target

[Service]
ExecStart=/bin/bash -c 'source /home/ryan/jarvis/bin/activate && python3 /home/ryan/code/enmms_gateway/index.py'
WorkingDirectory=/home/ryan/code/enmms_gateway
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"""

    service_path = "/etc/systemd/system/enmms-gateway.service"

    print("\n" + "=" * 80)
    print("ENMMS Gateway systemd Service")
    print("=" * 80)
    print(f"Writing service file to {service_path}")
    print("")

    with open(service_path, "w") as f:
        f.write(service_content)

    print(f"Created {service_path}")

    # Reload systemd to pick up the new service file
    run("systemctl daemon-reload")

    # Enable the service to auto-start on boot
    run("systemctl enable enmms-gateway.service")

    # Start the service now
    run("systemctl start enmms-gateway.service")

    # Confirm it's running
    run("systemctl status enmms-gateway.service", check=False)

    ###################################################
    # Done
    ###################################################

    print("\n")
    print("=" * 80)
    print("Installation Complete!")
    print("=" * 80)
    print("")
    print("Next steps:")
    print("  1. Edit uratex/.env and set DB_PASSWORD=0SmartPower0")
    print("  2. To switch gateway branch: cd code/uratex_gateway && git checkout <branch_name>")
    print("  3. Verify cron is running: sudo systemctl status cron")
    print("")


if __name__ == "__main__":
    main()
