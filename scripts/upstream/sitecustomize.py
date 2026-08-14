"""Run third-party asyncio suites against zuvloop without patching their sources."""

import asyncio
from asyncio import events

import zuvloop

asyncio.new_event_loop = zuvloop.new_event_loop
events.new_event_loop = zuvloop.new_event_loop
