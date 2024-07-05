from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime
import uuid

@dataclass
class User:
    id: int = field(init=False)
    user_id: str = None
    name: Optional[str] = None
    email: Optional[str] = None
    mobile: Optional[str] = None
    source: Optional[str] = None
    status: int = 1
    type: int = 0
    thumbnail_url: Optional[str] = None
    create_time: datetime = field(default_factory=datetime.utcnow)

    def __post_init__(self):
        self.id = None  # ID is auto-generated and will be set later by the database
        self.user_id = str(uuid.uuid4()) # uuid


# Example of creating a User instance
user = User(user_id="unique_user_id", name="John Doe", email="john.doe@example.com")
print(f"test user: {user}")