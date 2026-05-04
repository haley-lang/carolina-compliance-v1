import os
from dotenv import load_dotenv

load_dotenv()

# Email / IMAP
IMAP_HOST = os.getenv("IMAP_HOST", "imap.gmail.com")
IMAP_PORT = int(os.getenv("IMAP_PORT", 993))
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
REQUIRED_IMAP_MAILBOX = "coi-intake@carolinacompliancesolutions.com"

# Airtable
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Incoming Documents")

# Email addresses
OWNER_EMAIL = os.getenv("HALEY_EMAIL", "haley@carolinacompliancesolutions.com")
FROM_EMAIL = os.getenv("SENDGRID_FROM_EMAIL", "compliance@carolinacompliancesolutions.com")
INBOUND_EMAIL = os.getenv("INBOUND_EMAIL", "coi@carolinacompliancesolutions.com")


def reply_to_for(audience: str) -> str:
    """Pick the right Reply-To header per email audience.

    vendor   → INBOUND_EMAIL (automated COI inbox; replies feed the IMAP poller)
    client   → OWNER_EMAIL   (Haley reads + responds personally)
    internal → OWNER_EMAIL   (system→owner notifications)
    """
    if audience == "vendor":
        return INBOUND_EMAIL
    return OWNER_EMAIL

# Local storage
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "uploads")

# File types accepted as COI attachments
ALLOWED_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg", ".tiff", ".tif"}


def validate_config():
    required = {
        "EMAIL_ADDRESS": EMAIL_ADDRESS,
        "EMAIL_PASSWORD": EMAIL_PASSWORD,
        "AIRTABLE_API_KEY": AIRTABLE_API_KEY,
        "AIRTABLE_BASE_ID": AIRTABLE_BASE_ID,
    }
    missing = [key for key, value in required.items() if not value]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            "Copy .env.example to .env and fill in your values."
        )

    if EMAIL_ADDRESS != REQUIRED_IMAP_MAILBOX:
        raise EnvironmentError(
            "EMAIL_ADDRESS must be the real IMAP mailbox: "
            f"{REQUIRED_IMAP_MAILBOX}. Do not use alias addresses for IMAP login."
        )

    # Gmail app passwords are 16 characters (users may paste with spaces).
    normalized_password = EMAIL_PASSWORD.replace(" ", "")
    if len(normalized_password) != 16:
        raise EnvironmentError(
            "EMAIL_PASSWORD must be a Google App Password (16 characters, spaces optional)."
        )
