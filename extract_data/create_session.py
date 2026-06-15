from snowflake.snowpark import Session
from snowflake import connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import platform
import os


def get_os_type():
    system = platform.system().lower()

    if system == "windows":
        return "Windows"
    elif system == "linux":
        return "Linux"
    elif system == "darwin":
        return "macOS"
    else:
        return f"其他系统: {system}"


def get_private_key(private_key_path: str, passphrase: str = None):
    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode() if passphrase else None,
            backend=default_backend()
        )

    # 必須轉成 DER 格式給 Snowflake
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )


def create_session(config: dict) -> Session:
    # ==================== 建立 Session ====================
    os_name = get_os_type()

    if os_name == 'Windows':
        key_path = 'D:\\code\\snowflake\\rsa_key.p8'
    else:
        base_dir = os.getcwd()
        key_path = os.path.join(base_dir, "privatekey/rsa_key.p8")
        # key_path = 'rsa_key.p8'
    private_key = get_private_key(
        private_key_path=key_path  # 你的私鑰檔路徑

    )

    connection_parameters = {
        "account": config.get('account'),          # e.g. xy12345.us-east-1
        "user": config.get('user'),
        "private_key": private_key,            # ← 關鍵參數
        # 可選
        "warehouse": "ETL_WH",
        "database": config.get("database"),
        "schema": config.get("schema"),
        "role": "ACCOUNTADMIN",
    }

    session = Session.builder.configs(connection_parameters).create()
    return session


def create_conn(config: dict) -> connector:
    os_name = get_os_type()

    if os_name == 'Windows':
        key_path = 'D:\\code\\snowflake\\rsa_key.p8'
    else:
        base_dir = os.getcwd()
        key_path = os.path.join(base_dir, "privatekey/rsa_key.p8")
    private_key = get_private_key(
        private_key_path=key_path  # 你的私鑰檔路徑

    )
    conn = connector.connect(
        user=config.get('user'),
        account=config.get('account'),
        private_key=private_key,
        warehouse='ETL_WH',
        database=config.get("database"),
        schema=config.get("schema")
    )
    return conn
