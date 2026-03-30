import asyncio

from fastapi import FastAPI

from config import APP_NAME, APP_VERSION
from mqtt_runtime import ensure_mqtt_started, set_app_loop
from routes.resync import router as resync_router


app = FastAPI(title=APP_NAME, version=APP_VERSION)
app.include_router(resync_router)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.on_event("startup")
async def startup_event():
    set_app_loop(asyncio.get_running_loop())
    ensure_mqtt_started()


def main():
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=9001, reload=True)


if __name__ == "__main__":
    main()
