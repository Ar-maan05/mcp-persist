"""Smoke-test for examples/sqlite_server.py — run after starting the server."""
import asyncio
import json

from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client


def parse_tool_result(res) -> list[dict]:  # type: ignore[no-untyped-def]
    """FastMCP serialises list items as one TextContent per element."""
    return [json.loads(c.text) for c in res.content]  # type: ignore[attr-defined]


async def main() -> None:
    async with streamablehttp_client("http://127.0.0.1:8000/mcp") as (r, w, _):
        async with ClientSession(r, w) as session:
            await session.initialize()
            print("✓ session initialized")

            # add a note — returns a single dict
            res = await session.call_tool("add_note", {"title": "Refactored!", "body": "examples/ is clean."})
            data = json.loads(res.content[0].text)  # type: ignore[attr-defined]
            assert data["status"] == "created", data
            note_id = data["note_id"]
            print(f"✓ add_note → {data}")

            # list notes — FastMCP yields one TextContent per list element
            res = await session.call_tool("list_notes", {})
            notes = parse_tool_result(res)
            assert len(notes) >= 1, f"Expected at least 1 note, got {len(notes)}"
            note_ids_found = {n["note_id"] for n in notes}
            assert note_id in note_ids_found, f"Our note {note_id} missing from list"
            print(f"✓ list_notes → {len(notes)} note(s) (our note present)")

            # get_note — returns a single dict
            res = await session.call_tool("get_note", {"note_id": note_id})
            note = json.loads(res.content[0].text)  # type: ignore[attr-defined]
            assert note["title"] == "Refactored!", note
            assert note["body"] == "examples/ is clean.", note
            print(f"✓ get_note → title={note['title']!r}")

            # read resource notes://all
            res = await session.read_resource("notes://all")  # type: ignore[arg-type]
            text = res.contents[0].text  # type: ignore[attr-defined]
            assert "Refactored!" in text, repr(text)
            print(f"✓ notes://all resource → {text.splitlines()[0]!r}")

            # read resource notes://{id}
            res = await session.read_resource(f"notes://{note_id}")  # type: ignore[arg-type]
            text = res.contents[0].text  # type: ignore[attr-defined]
            assert "examples/ is clean." in text, repr(text)
            print(f"✓ notes://{note_id} resource → body correct")

            # slow echo — proves SSE stream holds open during async sleep
            res = await session.call_tool("slow_echo", {"message": "ping", "delay": 0.5})
            echo = json.loads(res.content[0].text)  # type: ignore[attr-defined]
            assert echo["echo"] == "ping", echo
            print(f"✓ slow_echo → {echo}")

    print("\n✓ All checks passed")


if __name__ == "__main__":
    asyncio.run(main())
