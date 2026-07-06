
import os
import subprocess
import sys
from pathlib import Path


def run(command, check=True, env=None):
    print("\n" + "=" * 80)
    print(f"Running:\n{command}")
    print("=" * 80)

    result = subprocess.run(command, shell=True, env=env)

    if check and result.returncode != 0:
        print(f"\nERROR: Command failed ({result.returncode})")
        sys.exit(result.returncode)

    return result


def get_real_user_home():
    """
    Get the actual (non-root) user's home directory even when running via sudo.
    `Path.home()` returns /root under sudo, which breaks path construction.
    """
    sudo_user = os.environ.get("SUDO_USER")
    if sudo_user:
        return Path(f"/home/{sudo_user}")
    return Path.home()


def get_php_version():
    """Detect the installed PHP major.minor version (e.g., '8.2')."""
    result = subprocess.run(
        "php -r \"echo PHP_MAJOR_VERSION . '.' . PHP_MINOR_VERSION;\"",
        shell=True, capture_output=True, text=True
    )
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    # Fallback: Debian Bookworm default
    return "8.2"


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

    home = get_real_user_home()
    code_dir = home / "code"
    activate = f"{home}/jarvis/bin/activate"

    # Non-interactive apt to prevent prompts from stalling the script
    noninteractive_env = os.environ.copy()
    noninteractive_env["DEBIAN_FRONTEND"] = "noninteractive"

    ###################################################
    # 1.1 Install prerequisites for venv and pip
    ###################################################

    run("apt update", env=noninteractive_env)
    run("apt upgrade -y", env=noninteractive_env)
    run("apt install -y python3-venv python3-pip", env=noninteractive_env)

    ###################################################
    # 1.2 Create a Python Virtual Environment
    ###################################################

    run(f"python3 -m venv {home}/jarvis")

    ###################################################
    # 2.1 Install MariaDB
    ###################################################

    run("apt install -y mariadb-server", env=noninteractive_env)
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

    run("apt install -y libmariadb3 libmariadb-dev", env=noninteractive_env)

    ###################################################
    # 2.4 Install Apache Web Server
    ###################################################

    run("apt install -y apache2", env=noninteractive_env)
    run("systemctl enable --now apache2")
    run("systemctl status apache2", check=False)

    ###################################################
    # 2.5 Install PHP and MySQL Extensions
    ###################################################

    run("apt install -y php php-mysql", env=noninteractive_env)
    run("systemctl restart apache2")

    ###################################################
    # 2.6 Install phpMyAdmin
    ###################################################

    # Use noninteractive to skip dbconfig-common prompts.
    # Manual DB setup may be needed after install.
    run("apt install -y phpmyadmin", env=noninteractive_env)

    write_phpmyadmin_conf()

    # Detect the installed PHP version dynamically instead of hardcoding
    php_version = get_php_version()
    print(f"Detected PHP version: {php_version}")

    run("a2enmod alias")
    run(f"a2enmod php{php_version}")
    run("a2enconf phpmyadmin")
    run("systemctl reload apache2")

    ###################################################
    # 2.7 Install Additional PHP Extensions
    ###################################################

    run("apt install -y php-json php-zip php-mbstring php-xml php-curl php-gd libapache2-mod-php",
        env=noninteractive_env)
    run("php -v", check=False)
    run("systemctl restart apache2")

    ###################################################
    # 3.1 Install CURL
    ###################################################

    run("apt install -y curl unzip", env=noninteractive_env)
    run("curl -V", check=False)

    ###################################################
    # 3.2 Install Composer (PHP Dependency Manager)
    ###################################################

    run("curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin --filename=composer")
    run("composer --version", check=False)

    ###################################################
    # 3.3 Install Git
    ###################################################

    run("apt install -y git", env=noninteractive_env)
    run("git -v", check=False)

    ###################################################
    # 3.4 Install Node.js and npm
    ###################################################

    # ca-certificates is needed for the NodeSource HTTPS repo on minimal installs
    run("apt install -y ca-certificates gnupg", env=noninteractive_env)
    run("curl -fsSL https://deb.nodesource.com/setup_lts.x | bash -")
    run("apt install -y nodejs", env=noninteractive_env)
    run("node -v", check=False)
    run("npm -v", check=False)

    # Install npm if missing
    run("apt install -y npm", check=False, env=noninteractive_env)

    # Install build tools
    run("apt install -y build-essential", env=noninteractive_env)

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

    sudo_user = os.environ.get("SUDO_USER", "ryan")
    cron_job = f"* * * * * cd {home}/code/uratex_gateway && /usr/bin/git pull"

    print("\n" + "=" * 80)
    print("Cron Job Setup")
    print("=" * 80)
    print("Adding the following cron job:")
    print(f"  {cron_job}")
    print("")

    # Add cron job (avoids duplicates by checking if it already exists)
    run(
        f"(crontab -u {sudo_user} -l 2>/dev/null | grep -F 'uratex_gateway') || "
        f"(crontab -u {sudo_user} -l 2>/dev/null; echo '{cron_job}') | crontab -u {sudo_user} -"
    )

    run(f"crontab -u {sudo_user} -l", check=False)

    ###################################################
    # 5.4 Enable Cron Service
    ###################################################

    run("systemctl start cron")
    run("systemctl enable cron")

    ###################################################
    # 5.5 Make Gateway Script Executable
    ###################################################

    run(f"chmod +x {home}/code/uratex_gateway/index.py")

    ###################################################
    # 6. ENMMS Gateway systemd Service
    ###################################################

    service_content = f"""[Unit]
Description=ENMMS Gateway Service
After=network.target

[Service]
ExecStart=/bin/bash -c 'source {home}/jarvis/bin/activate && python3 {home}/code/uratex_gateway/index.py'
WorkingDirectory={home}/code/uratex_gateway
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
    print("  4. If phpMyAdmin DB config was skipped, run: sudo dpkg-reconfigure phpmyadmin")
    print("")


if __name__ == "__main__":
    main()
