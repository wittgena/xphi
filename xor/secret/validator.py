# xphi.xor.secret.validator
## @lineage: xphi.arch.xor.secret.validator
## @lineage: arch.xor.secret.validator
## @lineage: mesh.bound.secure.secret.validator
from pydantic import SecretStr
from typing import List
import hashlib
from base64 import b64encode
from pydantic import BaseModel
from cryptography.fernet import Fernet

from xphi.arch.model.config import config
from xphi.xor.secret.cipher import Cipher
from xphi.watcher.plane.emitter import get_logger

class CredentialBase(BaseModel):
    credential_name: str
    credential_info: dict

class CredentialItem(CredentialBase):
    credential_values: dict

class CredentialAccessor:
    @staticmethod
    def get_credential_values(credential_name: str) -> dict:
        """Safe accessor for credentials."""

        if not config.credential_list:
            return {}
        for credential in config.credential_list:
            if credential.credential_name == credential_name:
                return credential.credential_values.copy()
        return {}

    @staticmethod
    def upsert_credentials(credentials: List[CredentialItem]):
        """Add a credential to the list of credentials."""
        credential_names = [cred.credential_name for cred in config.credential_list]
        for credential in credentials:
            if credential.credential_name in credential_names:
                # Find and replace the existing credential in the list
                for i, existing_cred in enumerate(config.credential_list):
                    if existing_cred.credential_name == credential.credential_name:
                        config.credential_list[i] = credential
                        break
            else:
                config.credential_list.append(credential)


def serialize_secret(v: SecretStr | None, info):
    if v is None:
        return None

    if info.context and info.context.get("cipher"):
        cipher: Cipher = info.context.get("cipher")
        return cipher.encrypt(v)

    if info.context and info.context.get("expose_secrets"):
        return v.get_secret_value()

    return v


def validate_secret(v: str | SecretStr | None, info) -> SecretStr | None:
    if v is None:
        return None

    if isinstance(v, SecretStr):
        secret_value = v.get_secret_value()
    else:
        secret_value = v

    if not secret_value or not secret_value.strip() or secret_value == "**********":
        return None

    if info.context and info.context.get("cipher"):
        cipher: Cipher = info.context.get("cipher")
        return cipher.decrypt(secret_value)

    if isinstance(v, SecretStr):
        return v
    else:
        return SecretStr(secret_value)
