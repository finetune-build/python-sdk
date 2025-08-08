import asyncio

from finetune.events.__main__ import EventListener

if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)

    async def main():
        # Optionally initialize Redis
        listener = EventListener()

        try:
            await listener.start()
        except KeyboardInterrupt:
            print("🛑 Received KeyboardInterrupt, shutting down...")
            await listener.stop()

    asyncio.run(main())

