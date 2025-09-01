import os
import random
from fastapi import FastAPI, Request
import httpx
from fastapi.responses import Response

app = FastAPI()

# Читаем переменные окружения
MONOLITH_URL = os.environ["MONOLITH_URL"]
MOVIES_SERVICE_URL = os.environ["MOVIES_SERVICE_URL"]
EVENTS_SERVICE_URL = os.environ["EVENTS_SERVICE_URL"]

GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy(path: str, request: Request):
    """
    Прокси-сервис (API Gateway).
    По умолчанию шлёт в монолит, но если включен фиче-флаг — распределяет трафик.
    """

    target_url = f"{MONOLITH_URL}/{path}"  # по умолчанию монолит

    # 🎬 Логика для /api/movies
    if path.startswith("api/movies"):
        if GRADUAL_MIGRATION:
            roll = random.randint(1, 100)
            if roll <= MOVIES_MIGRATION_PERCENT:
                target_url = f"{MOVIES_SERVICE_URL}/{path}"
                print(f"[PROXY] → movies-service ({roll}% <= {MOVIES_MIGRATION_PERCENT}%)")
            else:
                target_url = f"{MONOLITH_URL}/{path}"
                print(f"[PROXY] → monolith ({roll}% > {MOVIES_MIGRATION_PERCENT}%)")
        else:
            print("[PROXY] Migration OFF → monolith")

     📅 Пока events остаётся в монолите (тесты падают на events по условию задания)
        if path.startswith("api/events"):
         target_url = f"{MONOLITH_URL}/{path}"
     print("[PROXY] Events → монолит")

    # Проксируем запрос
    async with httpx.AsyncClient() as client:
        resp = await client.request(
            method=request.method,
            url=target_url,
            headers={k: v for k, v in request.headers.items() if k.lower() != "host"},
            content=await request.body(),
            params=request.query_params
        )

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=dict(resp.headers)
    )