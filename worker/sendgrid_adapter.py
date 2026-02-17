import logging

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, To, From

from worker.config import settings

logger = logging.getLogger(__name__)


class SendGridAdapter:
    """SendGrid email wrapper."""

    def __init__(self):
        self.client = SendGridAPIClient(settings.SENDGRID_API_KEY)
        self.from_email = settings.SENDGRID_FROM_EMAIL

    def send_template_email(
        self, to_email: str, template_id: str, dynamic_data: dict
    ) -> str:
        """
        Send a dynamic template email via SendGrid.
        Returns the SendGrid message ID.
        Raises on failure so ARQ can retry.
        """
        message = Mail(
            from_email=From(self.from_email, "Navarii"),
            to_emails=To(to_email),
        )
        message.template_id = template_id
        message.dynamic_template_data = dynamic_data

        print(f"📧 Sending email: {self.from_email} → {to_email} (template={template_id})")

        response = self.client.send(message)

        if response.status_code >= 400:
            print(f"❌ SendGrid error {response.status_code}: {response.body}")
            raise RuntimeError(
                f"SendGrid error {response.status_code}: {response.body}"
            )

        message_id = response.headers.get("X-Message-Id", "")
        print(f"✔ Email accepted by SendGrid (msg_id={message_id}, status={response.status_code})")
        return message_id
