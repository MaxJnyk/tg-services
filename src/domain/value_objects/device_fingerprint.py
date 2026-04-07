from dataclasses import dataclass


@dataclass(frozen=True)
class DeviceFingerprint:
    device_model: str
    system_version: str
    app_version: str
    api_id: int
    api_hash: str
    lang_code: str = "en"

    @classmethod
    def from_json(cls, data: dict):
        required = ["device", "sdk", "app_version", "app_id", "app_hash"]
        missing = [f for f in required if f not in data]
        if missing:
            raise ValueError(f"Missing required fields: {missing}")
        return cls(
            device_model=data["device"],
            system_version=data["sdk"],
            app_version=data["app_version"],
            api_id=data["app_id"],
            api_hash=data["app_hash"],
            lang_code=data.get("lang_code", "en"),
        )
