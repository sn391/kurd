import asyncio

from mcp import Client


async def main():
    async with Client("http://127.0.0.1:9200/mcp") as client:
        print("protocol:", client.protocol_version)

        tools = await client.list_tools()
        names = [tool.name for tool in tools.tools]

        print("tools:", names)

        if "add" not in names:
            raise RuntimeError(f"'add' tool not found: {names}")

        result = await client.call_tool(
            "add",
            arguments={
                "a": 20,
                "b": 22,
            },
        )

        print("result:", result)

        text = "".join(
            block.text
            for block in result.content
            if getattr(block, "type", None) == "text"
        )

        if text != "42":
            raise RuntimeError(
                f"Expected 42, got {text!r}"
            )

        print("INTEROP PASS")


asyncio.run(main())