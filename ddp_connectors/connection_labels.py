"""Non-sensitive PostgreSQL session labels shared by connector consumers."""


def normalize_application_name(value, default="ddp:source") -> str:
    """Return at most 63 printable ASCII bytes, suitable for libpq startup.

    Callers must supply operation names, never SQL, credentials or personal data.
    The fallback follows the same normalization as an explicit label.
    """
    def normalize(label):
        return "".join(
            char if 32 <= ord(char) <= 126 else "_"
            for char in str(label or "").strip()
        )[:63]

    return normalize(value) or normalize(default) or "ddp:source"
