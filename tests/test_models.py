import pytest
from pydantic import ValidationError

from app.models import EventModel


def test_event_model_accepts_contract():
    event = EventModel.model_validate(
        {
            "type": "actions",
            "source": "api",
            "destination": "audit",
            "id": "log_123",
            "data": {"action": "login"},
            "tags": ["auth"],
        }
    )
    assert event.type == "actions"


def test_event_model_rejects_invalid_type_and_extra_fields():
    with pytest.raises(ValidationError):
        EventModel.model_validate(
            {
                "type": "warning",
                "source": "api",
                "destination": "audit",
                "id": "log_123",
                "unexpected": True,
            }
        )
