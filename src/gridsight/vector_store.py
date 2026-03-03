from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from qdrant_client import QdrantClient, models

from gridsight.config import Settings



def _to_payload(row: pd.Series) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in row.to_dict().items():
        if pd.isna(value):
            continue
        if hasattr(value, "item"):
            value = value.item()
        payload[key] = value
    return payload


class PlayVectorStore:
    def __init__(
        self,
        collection: str,
        client: QdrantClient,
    ) -> None:
        self.collection = collection
        self.client = client

    @classmethod
    def from_settings(cls, settings: Settings) -> "PlayVectorStore":
        if settings.qdrant_url:
            client = QdrantClient(
                url=settings.qdrant_url,
                api_key=settings.qdrant_api_key,
                timeout=settings.qdrant_timeout_seconds,
            )
        else:
            settings.vector_db_dir.mkdir(parents=True, exist_ok=True)
            client = QdrantClient(path=str(settings.vector_db_dir))

        return cls(collection=settings.qdrant_collection, client=client)

    def recreate(self, vector_size: int) -> None:
        collections = {item.name for item in self.client.get_collections().collections}
        if self.collection in collections:
            self.client.delete_collection(self.collection)

        self.client.create_collection(
            collection_name=self.collection,
            vectors_config=models.VectorParams(
                size=vector_size,
                distance=models.Distance.COSINE,
            ),
        )

    def upsert(self, index_frame: pd.DataFrame, vectors: np.ndarray) -> None:
        points: list[models.PointStruct] = []
        for row, vector in zip(index_frame.itertuples(index=False), vectors, strict=False):
            row_data = pd.Series(row._asdict())
            point_id = int(row_data["vector_id"])
            payload = _to_payload(row_data)
            payload.pop("vector_id", None)

            points.append(
                models.PointStruct(
                    id=point_id,
                    vector=vector.astype(float).tolist(),
                    payload=payload,
                )
            )

        step = 2048
        for i in range(0, len(points), step):
            self.client.upsert(collection_name=self.collection, points=points[i : i + step])

    def search(self, vector: np.ndarray, limit: int = 10) -> list[models.ScoredPoint]:
        query = vector.astype(float).tolist()

        # qdrant-client >=1.17 uses query_points; older versions expose search.
        if hasattr(self.client, "query_points"):
            response = self.client.query_points(
                collection_name=self.collection,
                query=query,
                limit=limit,
                with_payload=True,
            )
            return list(response.points)

        return self.client.search(
            collection_name=self.collection,
            query_vector=query,
            limit=limit,
            with_payload=True,
        )
