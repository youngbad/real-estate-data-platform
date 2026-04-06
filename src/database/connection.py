import os
import struct
from urllib.parse import quote_plus

import pyodbc
from azure.identity import AzureCliCredential, DefaultAzureCredential, ManagedIdentityCredential
from sqlalchemy import create_engine

SQL_COPT_SS_ACCESS_TOKEN = 1256


def _configure_odbc_paths() -> None:
    odbc_sysini = os.getenv("ODBCSYSINI", "/opt/homebrew/etc")
    odbcinstini = os.getenv("ODBCINSTINI", "odbcinst.ini")

    if os.path.exists(os.path.join(odbc_sysini, odbcinstini)):
        os.environ.setdefault("ODBCSYSINI", odbc_sysini)
        os.environ.setdefault("ODBCINSTINI", odbcinstini)


def _get_sql_server_settings(prefix: str = "DB") -> dict[str, str]:
    _configure_odbc_paths()

    return {
        "user": os.getenv(f"{prefix}_USER", "sqladmin"),
        "password": os.getenv(f"{prefix}_PASSWORD", ""),
        "host": os.getenv(f"{prefix}_HOST", "localhost"),
        "port": os.getenv(f"{prefix}_PORT", "1433"),
        "database": os.getenv(f"{prefix}_NAME", "real_estate_db"),
        "driver": os.getenv(f"{prefix}_DRIVER", "ODBC Driver 18 for SQL Server"),
        "encrypt": os.getenv(f"{prefix}_ENCRYPT", "yes"),
        "trust_server_certificate": os.getenv(f"{prefix}_TRUST_CERT", "no"),
        "auth_mode": os.getenv(f"{prefix}_AUTH_MODE", "sql").lower(),
        "entra_credential": os.getenv(f"{prefix}_ENTRA_CREDENTIAL", "azure_cli").lower(),
    }


def _build_sql_server_odbc_connection_string(prefix: str = "DB", include_credentials: bool = True) -> str:
    settings = _get_sql_server_settings(prefix)
    parts = [
        f"DRIVER={{{settings['driver']}}}",
        f"SERVER={settings['host']},{settings['port']}",
        f"DATABASE={settings['database']}",
        f"Encrypt={settings['encrypt']}",
        f"TrustServerCertificate={settings['trust_server_certificate']}",
        "Connection Timeout=30",
    ]

    if include_credentials:
        parts.append(f"UID={settings['user']}")
        parts.append(f"PWD={settings['password']}")

    return ";".join(parts)


def _get_entra_credential(prefix: str = "DB"):
    credential_name = _get_sql_server_settings(prefix)["entra_credential"]

    if credential_name == "managed_identity":
        return ManagedIdentityCredential()
    if credential_name == "default":
        return DefaultAzureCredential(exclude_interactive_browser_credential=False)
    return AzureCliCredential()


def _get_entra_access_token(prefix: str = "DB") -> bytes:
    credential = _get_entra_credential(prefix)
    token = credential.get_token("https://database.windows.net/.default").token
    token_bytes = token.encode("utf-16-le")
    return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)


def create_sql_server_engine(prefix: str = "DB"):
    settings = _get_sql_server_settings(prefix)
    if settings["auth_mode"] != "entra":
        return create_engine(build_sql_server_url(prefix), pool_pre_ping=True)

    connection_string = _build_sql_server_odbc_connection_string(prefix, include_credentials=False)

    def connect_with_token():
        token = _get_entra_access_token(prefix)
        return pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token})

    return create_engine("mssql+pyodbc://", creator=connect_with_token, pool_pre_ping=True)


def build_sql_server_url(prefix: str = "DB") -> str:
    """Build a SQL Server connection URL from environment variables."""
    settings = _get_sql_server_settings(prefix)
    user = quote_plus(settings["user"])
    password = quote_plus(settings["password"])
    host = settings["host"]
    port = settings["port"]
    database = quote_plus(settings["database"])
    driver = quote_plus(settings["driver"])
    encrypt = settings["encrypt"]
    trust_server_certificate = settings["trust_server_certificate"]

    return (
        f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}"
        f"?driver={driver}&Encrypt={encrypt}"
        f"&TrustServerCertificate={trust_server_certificate}"
    )