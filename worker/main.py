"""
ARQ worker entrypoint.

Run with:  arq worker.main.WorkerSettings
"""

import logging
import sys

from datetime import datetime, timezone
from urllib.parse import quote

from arq.connections import RedisSettings

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

from worker.config import settings
from worker.supabase_client import get_supabase_service
from worker.sendgrid_adapter import SendGridAdapter

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _format_currency(cents: int, currency: str = "USD") -> str:
    symbols = {"USD": "$", "EUR": "\u20ac", "GBP": "\u00a3"}
    symbol = symbols.get(currency.upper(), "$")
    return f"{symbol}{cents / 100:.2f}"


def _parse_dt(val: str) -> datetime:
    return datetime.fromisoformat(str(val).replace("Z", "+00:00"))


def _format_date(dt_str: str) -> str:
    """'Saturday, February 22, 2026'"""
    try:
        dt = _parse_dt(dt_str)
        return f"{dt.strftime('%A, %B')} {dt.day}, {dt.year}"
    except Exception:
        return str(dt_str)


def _format_time(dt_str: str) -> str:
    """'10:00 AM'"""
    try:
        dt = _parse_dt(dt_str)
        hour = dt.hour % 12 or 12
        return f"{hour}:{dt.strftime('%M %p')}"
    except Exception:
        return ""


def _format_time_range(start_str: str, end_str: str) -> str:
    """'10:00 AM - 11:30 AM'"""
    s, e = _format_time(start_str), _format_time(end_str)
    return f"{s} \u2013 {e}" if s and e else s or e or ""


def _format_duration(start_str: str, end_str: str) -> str:
    """'1h 30min'"""
    try:
        total_min = int((_parse_dt(end_str) - _parse_dt(start_str)).total_seconds() / 60)
        if total_min < 60:
            return f"{total_min}min"
        h, m = divmod(total_min, 60)
        return f"{h}h {m}min" if m else f"{h}h"
    except Exception:
        return ""


def _format_short_date(dt_str: str) -> str:
    """'Feb 17, 2026'"""
    try:
        dt = _parse_dt(dt_str)
        return f"{dt.strftime('%b')} {dt.day}, {dt.year}"
    except Exception:
        return str(dt_str)


def _make_calendar_link(title: str, start_str: str, end_str: str, location: str = "") -> str:
    try:
        s = _parse_dt(start_str).astimezone(timezone.utc)
        e = _parse_dt(end_str).astimezone(timezone.utc)
        dates = f"{s.strftime('%Y%m%dT%H%M%SZ')}/{e.strftime('%Y%m%dT%H%M%SZ')}"
        return (
            f"https://calendar.google.com/calendar/render?action=TEMPLATE"
            f"&text={quote(title)}&dates={dates}&location={quote(location)}"
        )
    except Exception:
        return ""


def _make_directions_url(lat=None, lng=None, address: str = "") -> str:
    if lat and lng:
        return f"https://www.google.com/maps/dir/?api=1&destination={lat},{lng}"
    if address:
        return f"https://www.google.com/maps/dir/?api=1&destination={quote(address)}"
    return ""


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_booking(supabase, booking_id: str) -> dict | None:
    result = (
        supabase.table("bookings")
        .select("*")
        .eq("id", booking_id)
        .maybe_single()
        .execute()
    )
    return result.data if result else None


def _get_seeker_email(supabase, booking: dict) -> str | None:
    sp = (
        supabase.table("seeker_profiles")
        .select("user_id")
        .eq("id", booking["seeker_profile_id"])
        .maybe_single()
        .execute()
    )
    if not sp or not sp.data:
        return None
    prof = (
        supabase.table("profiles")
        .select("email")
        .eq("user_id", sp.data["user_id"])
        .maybe_single()
        .execute()
    )
    return prof.data.get("email") if prof and prof.data else None


def _get_seeker_user_id(supabase, booking: dict) -> str | None:
    sp = (
        supabase.table("seeker_profiles")
        .select("user_id")
        .eq("id", booking["seeker_profile_id"])
        .maybe_single()
        .execute()
    )
    return sp.data["user_id"] if sp and sp.data else None


def _get_provider_name(supabase, booking: dict) -> str:
    pp = (
        supabase.table("provider_profiles")
        .select("business_name, user_id")
        .eq("id", booking["provider_profile_id"])
        .maybe_single()
        .execute()
    )
    if not pp or not pp.data:
        return "Your provider"
    name = pp.data.get("business_name")
    if not name and pp.data.get("user_id"):
        prof = (
            supabase.table("profiles")
            .select("display_name")
            .eq("user_id", pp.data["user_id"])
            .maybe_single()
            .execute()
        )
        name = prof.data.get("display_name") if prof and prof.data else None
    return name or "Your provider"


def _get_seeker_name(supabase, booking: dict) -> str:
    sp = (
        supabase.table("seeker_profiles")
        .select("user_id")
        .eq("id", booking["seeker_profile_id"])
        .maybe_single()
        .execute()
    )
    if not sp or not sp.data:
        return "Guest"
    prof = (
        supabase.table("profiles")
        .select("display_name")
        .eq("user_id", sp.data["user_id"])
        .maybe_single()
        .execute()
    )
    return prof.data.get("display_name") or "Guest" if prof and prof.data else "Guest"


def _get_offering(supabase, offering_id: str) -> dict:
    result = (
        supabase.table("offerings")
        .select("offering_type, image_url, start_datetime, end_datetime, provider_location_id")
        .eq("id", offering_id)
        .maybe_single()
        .execute()
    )
    return result.data if result and result.data else {}


def _get_location(supabase, offering: dict, offering_id: str) -> dict:
    loc_id = offering.get("provider_location_id")
    if loc_id:
        result = (
            supabase.table("provider_locations")
            .select("name, address_formatted, latitude, longitude")
            .eq("id", loc_id)
            .maybe_single()
            .execute()
        )
        if result and result.data:
            return result.data

    # Fallback: offering_locations (events, retreats)
    result = (
        supabase.table("offering_locations")
        .select("venue_name, address_formatted, latitude, longitude")
        .eq("offering_id", offering_id)
        .order("stop_order")
        .limit(1)
        .execute()
    )
    if result and result.data:
        loc = result.data[0]
        return {
            "name": loc.get("venue_name"),
            "address_formatted": loc.get("address_formatted"),
            "latitude": loc.get("latitude"),
            "longitude": loc.get("longitude"),
        }
    return {}


# ---------------------------------------------------------------------------
# Template data
# ---------------------------------------------------------------------------

def _build_template_data(supabase, booking: dict) -> dict:
    """Build dynamic template data matching SendGrid template variables."""
    booking_id = booking["id"]
    offering_id = booking.get("offering_id", "")

    # Fetch related data
    offering = _get_offering(supabase, offering_id) if offering_id else {}
    location = _get_location(supabase, offering, offering_id) if offering_id else {}

    # Time strings from booking (snapshot at booking time)
    start_str = booking.get("offering_start_datetime", "")
    end_str = booking.get("offering_end_datetime", "")

    # Currency
    amount_cents = booking.get("amount_cents", 0)
    currency = booking.get("currency", "USD")

    # Location
    loc_name = location.get("name", "")
    loc_address = location.get("address_formatted", "")
    lat = location.get("latitude")
    lng = location.get("longitude")

    # Links
    directions_url = _make_directions_url(lat, lng, loc_address)
    calendar_link = _make_calendar_link(
        booking.get("offering_title", ""), start_str, end_str,
        loc_address or loc_name,
    )
    app_url = settings.NAVARII_APP_URL

    # Payment date
    booked_at = booking.get("booked_at", "")
    payment_date = _format_short_date(booked_at) if booked_at else ""

    return {
        # Identity
        "seeker_name": _get_seeker_name(supabase, booking),
        "provider_name": _get_provider_name(supabase, booking),

        # Offering
        "offering_title": booking.get("offering_title", ""),
        "offering_type": (offering.get("offering_type") or "session").replace("_", " "),
        "cover_image_url": offering.get("image_url", ""),

        # Date & time
        "event_date": _format_date(start_str) if start_str else "",
        "event_time": _format_time_range(start_str, end_str) if start_str and end_str else "",
        "duration": _format_duration(start_str, end_str) if start_str and end_str else "",

        # Location
        "location_name": loc_name,
        "location_address": loc_address,
        "directions_url": directions_url,
        "map_url": "",  # Needs Google Static Maps API key

        # Calendar
        "calendar_link": calendar_link,

        # Payment
        "booking_id": booking_id,
        "booking_reference": booking_id[:8].upper(),
        "base_price": _format_currency(amount_cents, currency),
        "service_fee": _format_currency(0, currency),
        "total_amount": _format_currency(amount_cents, currency),
        "payment_method": "Card",
        "payment_date": payment_date,
        "amount_cents": amount_cents,
        "currency": currency,

        # Policy
        "cancellation_policy": (
            "Full refund if cancelled more than 48 hours before the experience. "
            "50% refund within 24\u201348 hours. No refund within 24 hours."
        ),

        # Links
        "booking_detail_link": f"{app_url}/bookings/{booking_id}" if app_url else "",
        "contact_provider_link": f"{app_url}/bookings/{booking_id}/contact" if app_url else "",
        "preferences_link": f"{app_url}/settings/notifications" if app_url else "",
    }


def _log_delivery(
    supabase,
    *,
    booking_id: str,
    user_id: str | None,
    template_key: str,
    destination: str,
    status: str,
    message_id: str = "",
    error: str | None = None,
):
    """Insert a notification_deliveries row."""
    try:
        supabase.table("notification_deliveries").insert({
            "channel": "email",
            "status": status,
            "destination": destination,
            "template_key": template_key,
            "payload": {},
            "booking_id": booking_id,
            "user_id": user_id,
            "dedupe_key": f"{booking_id}:{template_key}",
            "sent_at": datetime.now(timezone.utc).isoformat() if status == "sent" else None,
            "last_error": error,
        }).execute()
    except Exception as e:
        logger.warning("Failed to log delivery for booking %s: %s", booking_id, e)


# ---------------------------------------------------------------------------
# Job functions
# ---------------------------------------------------------------------------

async def send_booking_confirmation(ctx: dict, booking_id: str) -> None:
    """Send confirmation email to seeker after successful payment."""
    if not settings.NOTIFICATIONS_ENABLED:
        return

    supabase = get_supabase_service()
    booking = _get_booking(supabase, booking_id)
    if not booking:
        logger.warning("Booking %s not found, skipping confirmation", booking_id)
        return

    if booking.get("confirmation_sent_at"):
        logger.info("Confirmation already sent for booking %s", booking_id)
        return

    if booking["status"] in ("cancelled", "refunded", "failed"):
        return

    email = _get_seeker_email(supabase, booking)
    if not email:
        logger.warning("No email for booking %s seeker", booking_id)
        return

    adapter = SendGridAdapter()
    template_data = _build_template_data(supabase, booking)

    adapter.send_template_email(
        to_email=email,
        template_id=settings.SENDGRID_CONFIRMATION_TEMPLATE_ID,
        dynamic_data=template_data,
    )

    now = datetime.now(timezone.utc).isoformat()
    supabase.table("bookings").update({"confirmation_sent_at": now}).eq("id", booking_id).execute()

    user_id = _get_seeker_user_id(supabase, booking)
    _log_delivery(
        supabase,
        booking_id=booking_id,
        user_id=user_id,
        template_key="booking_confirmation",
        destination=email,
        status="sent",
    )


async def send_booking_cancellation(ctx: dict, booking_id: str, cancelled_by: str) -> None:
    """Send cancellation email to seeker."""
    if not settings.NOTIFICATIONS_ENABLED:
        return

    supabase = get_supabase_service()
    booking = _get_booking(supabase, booking_id)
    if not booking:
        return

    email = _get_seeker_email(supabase, booking)
    if not email:
        return

    adapter = SendGridAdapter()
    template_data = _build_template_data(supabase, booking)
    template_data["cancelled_by"] = cancelled_by
    template_data["refund_amount"] = _format_currency(
        booking.get("refund_amount_cents", 0), booking.get("currency", "USD")
    )
    template_data["refund_amount_cents"] = booking.get("refund_amount_cents", 0)

    adapter.send_template_email(
        to_email=email,
        template_id=settings.SENDGRID_CANCELLATION_TEMPLATE_ID,
        dynamic_data=template_data,
    )

    user_id = _get_seeker_user_id(supabase, booking)
    _log_delivery(
        supabase,
        booking_id=booking_id,
        user_id=user_id,
        template_key="booking_cancellation",
        destination=email,
        status="sent",
    )


async def send_reminder_notification(ctx: dict, booking_id: str) -> None:
    """Send T-24h reminder email to seeker."""
    if not settings.NOTIFICATIONS_ENABLED:
        return

    supabase = get_supabase_service()
    booking = _get_booking(supabase, booking_id)
    if not booking:
        return

    if booking.get("reminder_sent_at"):
        return

    if booking["status"] not in ("confirmed", "pending_payout"):
        return

    email = _get_seeker_email(supabase, booking)
    if not email:
        return

    adapter = SendGridAdapter()
    template_data = _build_template_data(supabase, booking)

    adapter.send_template_email(
        to_email=email,
        template_id=settings.SENDGRID_REMINDER_TEMPLATE_ID,
        dynamic_data=template_data,
    )

    now = datetime.now(timezone.utc).isoformat()
    supabase.table("bookings").update({"reminder_sent_at": now}).eq("id", booking_id).execute()

    user_id = _get_seeker_user_id(supabase, booking)
    _log_delivery(
        supabase,
        booking_id=booking_id,
        user_id=user_id,
        template_key="booking_reminder",
        destination=email,
        status="sent",
    )


async def send_followup_notification(ctx: dict, booking_id: str) -> None:
    """Send T+24h followup email to seeker after offering ends."""
    if not settings.NOTIFICATIONS_ENABLED:
        return

    supabase = get_supabase_service()
    booking = _get_booking(supabase, booking_id)
    if not booking:
        return

    if booking.get("followup_sent_at"):
        return

    if booking["status"] in ("cancelled", "refunded", "failed"):
        return

    email = _get_seeker_email(supabase, booking)
    if not email:
        return

    adapter = SendGridAdapter()
    template_data = _build_template_data(supabase, booking)

    adapter.send_template_email(
        to_email=email,
        template_id=settings.SENDGRID_FOLLOWUP_TEMPLATE_ID,
        dynamic_data=template_data,
    )

    now = datetime.now(timezone.utc).isoformat()
    supabase.table("bookings").update({"followup_sent_at": now}).eq("id", booking_id).execute()

    user_id = _get_seeker_user_id(supabase, booking)
    _log_delivery(
        supabase,
        booking_id=booking_id,
        user_id=user_id,
        template_key="booking_followup",
        destination=email,
        status="sent",
    )


# ---------------------------------------------------------------------------
# ARQ WorkerSettings
# ---------------------------------------------------------------------------

class WorkerSettings:
    functions = [
        send_booking_confirmation,
        send_booking_cancellation,
        send_reminder_notification,
        send_followup_notification,
    ]
    redis_settings = RedisSettings.from_dsn(settings.REDIS_URL) if settings.REDIS_URL else RedisSettings()
    max_jobs = 10
    job_timeout = 60
    retry_jobs = True
    max_tries = 4
