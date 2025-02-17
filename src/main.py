import asyncio
from config import Config
from pipeline.pipeline import Pipeline


async def main():
    config = Config()
    pipeline = Pipeline(config)
    await pipeline.run(file_prefix="data/", primary_key="id")


if __name__ == "__main__":
    asyncio.run(main())
