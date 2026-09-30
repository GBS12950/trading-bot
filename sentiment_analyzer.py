"""
SentimentAnalyzer - Analisi del sentiment delle notizie.

Supporta due modalità:
1. VADER (nltk) - Leggero, no dipendenze esterne, offline
2. LLM esterno - Chiama un'API LLM (es. OpenAI) per sentiment più accurato
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from enum import Enum
from typing import Any, Optional

import httpx

logger = logging.getLogger("sentiment_analyzer")


class SentimentMode(Enum):
    """Modalità di analisi del sentiment."""
    VADER = "vader"
    LLM = "llm"


class Sentiment:
    """Rappresenta il risultato dell'analisi del sentiment."""

    def __init__(self, score: float, label: str, confidence: float = 1.0):
        # Normalizza score a [-1, 1]
        self.score = max(-1.0, min(1.0, float(score)))
        self.label = label  # 'positive', 'neutral', 'negative'
        self.confidence = max(0.0, min(1.0, float(confidence)))

    def __repr__(self) -> str:
        return f"Sentiment(score={self.score:.4f}, label={self.label}, confidence={self.confidence:.2f})"


class SentimentAnalyzer:
    """Gestore dell'analisi del sentiment delle notizie."""

    def __init__(
        self,
        mode: str = "vader",
        llm_api_key: Optional[str] = None,
        llm_model: str = "gpt-3.5-turbo",
        llm_base_url: str = "https://api.openai.com/v1",
    ) -> None:
        self.mode = SentimentMode(mode.lower())
        self.llm_api_key = llm_api_key or os.getenv("LLM_API_KEY")
        self.llm_model = llm_model
        self.llm_base_url = llm_base_url

        if self.mode == SentimentMode.LLM and not self.llm_api_key:
            logger.warning("Modalità LLM selezionata ma LLM_API_KEY non impostata. Fallback a VADER.")
            self.mode = SentimentMode.VADER

        # Carica VADER se necessario (prima richiesta, non al __init__ per evitare import pesante)
        self._vader_analyzer: Optional[Any] = None
        logger.info("SentimentAnalyzer inizializzato in modalità %s", self.mode.value)

    def _init_vader(self) -> None:
        """Carica VADER analyzer (lazy loading)."""
        if self._vader_analyzer is not None:
            return

        try:
            from nltk.sentiment import SentimentIntensityAnalyzer
            import nltk

            # Scarica i dati VADER (se non presenti)
            try:
                nltk.data.find("sentiment/vader_lexicon.zip")
            except LookupError:
                nltk.download("vader_lexicon", quiet=True)

            self._vader_analyzer = SentimentIntensityAnalyzer()
            logger.info("VADER analyzer caricato")
        except ImportError:
            logger.error("nltk non installato. Installa con: pip install nltk")
            raise

    async def analyze(self, text: str, headline: Optional[str] = None) -> Sentiment:
        """
        Analizza il sentiment di un testo.

        Args:
            text: Testo principale (summary della notizia)
            headline: Titolo della notizia (facoltativo, aumenta accuracy)

        Returns:
            Sentiment con score, label e confidence
        """
        if not text or not isinstance(text, str):
            return Sentiment(score=0.0, label="neutral", confidence=0.0)

        if self.mode == SentimentMode.VADER:
            return await self._analyze_vader(text, headline)
        else:  # LLM
            return await self._analyze_llm(text, headline)

    async def _analyze_vader(self, text: str, headline: Optional[str] = None) -> Sentiment:
        """Analisi con VADER (offline, veloce)."""
        self._init_vader()

        # Combina headline e text per migliore accuracy
        combined_text = f"{headline or ''} {text}".strip()

        # VADER ritorna compound score in [-1, 1]
        scores = self._vader_analyzer.polarity_scores(combined_text)
        compound = scores["compound"]

        # Determina il label
        if compound >= 0.05:
            label = "positive"
        elif compound <= -0.05:
            label = "negative"
        else:
            label = "neutral"

        # Confidence basato sulla forza del sentiment
        confidence = abs(compound)

        logger.debug(
            "VADER sentiment: text=%s | score=%.4f | label=%s | confidence=%.2f",
            combined_text[:50], compound, label, confidence,
        )

        return Sentiment(score=compound, label=label, confidence=confidence)

    async def _analyze_llm(self, text: str, headline: Optional[str] = None) -> Sentiment:
        """Analisi con LLM esterno (es. OpenAI, più accurato ma lento e costoso)."""
        if not self.llm_api_key:
            logger.warning("LLM_API_KEY non impostato. Fallback a VADER.")
            return await self._analyze_vader(text, headline)

        combined_text = f"{headline or ''} {text}".strip()

        prompt = f"""Analizza il sentiment della seguente notizia finanziaria.
Rispondi in JSON con i campi: "score" (da -1 a 1), "label" (positive/neutral/negative), "confidence" (0 a 1).

Testo: {combined_text}

Rispondi SOLO con JSON valido, nessun'altra spiegazione."""

        try:
            headers = {
                "Authorization": f"Bearer {self.llm_api_key}",
                "Content-Type": "application/json",
            }
            payload = {
                "model": self.llm_model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0,
                "max_tokens": 100,
            }

            async with httpx.AsyncClient(timeout=30.0) as client:
                res = await client.post(f"{self.llm_base_url}/chat/completions", json=payload, headers=headers)
                res.raise_for_status()
                data = res.json()

            # Estrai la risposta
            response_text = data["choices"][0]["message"]["content"].strip()

            # Parse JSON
            result = json.loads(response_text)
            sentiment = Sentiment(
                score=float(result.get("score", 0)),
                label=str(result.get("label", "neutral")).lower(),
                confidence=float(result.get("confidence", 0.5)),
            )

            logger.debug("LLM sentiment: %s", sentiment)
            return sentiment

        except (httpx.HTTPError, json.JSONDecodeError, KeyError, ValueError) as exc:
            logger.warning("Errore nell'analisi LLM (%s): %s. Fallback a VADER.", type(exc).__name__, exc)
            return await self._analyze_vader(text, headline)

    async def batch_analyze(
        self,
        items: list[dict[str, str]],
        max_concurrent: int = 5,
    ) -> list[tuple[str, Sentiment]]:
        """
        Analizza più notizie concorrentemente.

        Args:
            items: Lista di dict con chiavi 'id', 'text', 'headline' (facoltativo)
            max_concurrent: Numero massimo di richieste simultanee

        Returns:
            Lista di tuple (item_id, Sentiment)
        """
        semaphore = asyncio.Semaphore(max_concurrent)

        async def analyze_one(item: dict[str, str]) -> tuple[str, Sentiment]:
            async with semaphore:
                sentiment = await self.analyze(item.get("text", ""), item.get("headline"))
                return item["id"], sentiment

        tasks = [analyze_one(item) for item in items]
        return await asyncio.gather(*tasks)


# ---------------------------------------------------------------------- smoke test
async def _smoke_test() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    analyzer_vader = SentimentAnalyzer(mode="vader")
    analyzer_llm = SentimentAnalyzer(mode="llm")  # Fallback a VADER se no key

    test_texts = [
        {
            "id": "1",
            "headline": "Apple announces record Q4 earnings",
            "text": "Apple Inc. reported record quarterly earnings, beating analyst expectations.",
        },
        {
            "id": "2",
            "headline": "Tech stocks plunge amid recession fears",
            "text": "Major tech companies saw significant declines as investors worry about economic slowdown.",
        },
        {
            "id": "3",
            "headline": "Neutral market update",
            "text": "Market remains relatively stable with mixed trading signals.",
        },
    ]

    logger.info("=== VADER Analysis ===")
    results_vader = await analyzer_vader.batch_analyze(test_texts)
    for item_id, sentiment in results_vader:
        logger.info("Item %s: %s", item_id, sentiment)

    logger.info("\n=== LLM Analysis (with fallback) ===")
    results_llm = await analyzer_llm.batch_analyze(test_texts[:1])
    for item_id, sentiment in results_llm:
        logger.info("Item %s: %s", item_id, sentiment)


if __name__ == "__main__":
    asyncio.run(_smoke_test())
