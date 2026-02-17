from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SUPABASE_URL: str
    SUPABASE_SERVICE_ROLE_KEY: str

    # Redis / ARQ
    REDIS_URL: str = ""

    # SendGrid
    SENDGRID_API_KEY: str = ""
    SENDGRID_FROM_EMAIL: str = "noreply@navarii.com"
    SENDGRID_CONFIRMATION_TEMPLATE_ID: str = ""
    SENDGRID_CANCELLATION_TEMPLATE_ID: str = ""
    SENDGRID_REMINDER_TEMPLATE_ID: str = ""
    SENDGRID_FOLLOWUP_TEMPLATE_ID: str = ""

    # Notifications
    NOTIFICATIONS_ENABLED: bool = True

    # App URL for email links (deep link or web fallback)
    NAVARII_APP_URL: str = ""

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
