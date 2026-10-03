"""Request/response models. FastAPI derives validation and the OpenAPI docs from these."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, StringConstraints

Text = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=5000)]
CommentText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=1000)]
Channel = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=64)]


def _author(value: object) -> str:
    value = value.strip() if isinstance(value, str) else ""
    return value[:40] or "Anonymous"


Author = Annotated[str, BeforeValidator(_author)]
Verdict = Literal["safe", "review", "flagged"]
Source = Literal["api", "analyze", "chat", "composer", "home"]


class AnalyzeRequest(BaseModel):
    text: Text
    source: Source = "api"
    record: bool = Field(True, description="Count this analysis in stats. Pre-send checks pass false.")


class BatchAnalyzeRequest(BaseModel):
    texts: list[Text] = Field(min_length=1, max_length=256)
    source: Source = "api"


class MatchOut(BaseModel):
    term: str
    category: str
    tier: Literal["strong", "contextual", "mild"]
    start: int
    end: int
    directed: bool = False


class AnalysisOut(BaseModel):
    verdict: Verdict
    risk: float = Field(ge=0, le=1)
    categories: list[str]
    reasons: list[str]
    matches: list[MatchOut]
    model_scores: dict[str, float] | None
    engine: str
    latency_ms: float
    chars: int


class BatchAnalysisOut(BaseModel):
    results: list[AnalysisOut]


class MessageIn(BaseModel):
    model_config = ConfigDict(extra="ignore")
    channel: Channel = "default"
    author: Author = "Anonymous"
    text: Text


class MessagesIn(BaseModel):
    messages: list[MessageIn] = Field(min_length=1, max_length=1000)


class MessagesAccepted(BaseModel):
    accepted: int
    ids: list[str]


class PostIn(BaseModel):
    author: Author = "Anonymous"
    text: Text


class CommentIn(BaseModel):
    author: Author = "Anonymous"
    text: CommentText


class ReactionIn(BaseModel):
    kind: Literal["like", "share"]


class SimulateIn(BaseModel):
    messages: int = Field(1000, ge=1, le=50_000)
    channels: int = Field(50, ge=1, le=5_000)
    rate: int = Field(2000, ge=1, le=50_000, description="Messages per second")


class FeedIn(BaseModel):
    messages: int = Field(5000, ge=1, le=50_000, description="How many real posts to pull")
