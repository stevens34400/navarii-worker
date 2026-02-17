from supabase import create_client, Client
from worker.config import settings

_supabase_service: Client | None = None


def get_supabase_service() -> Client:
    global _supabase_service
    if _supabase_service is None:
        _supabase_service = create_client(
            settings.SUPABASE_URL,
            settings.SUPABASE_SERVICE_ROLE_KEY,
        )
    return _supabase_service
