"""
trigger.py — Send a trigger to the demo data source.

This simulates the analyst performing an action on the black-box system.
In the real world, this would be clicking a button, sending a request,
toggling a setting — whatever the analyst does that they want to
correlate with objects in the stream.

Usage:
    python trigger.py login
    python trigger.py purchase
    python trigger.py search

The trigger is sent via UDP to the data source. The source then emits
response objects into the stream after a short delay. The analyst
doesn't know what objects will appear — they must discover that
using the analyzer.
"""

import socket
import sys

TRIGGER_HOST = "127.0.0.1"
TRIGGER_PORT = 8766

if len(sys.argv) < 2:
    print("Usage: python trigger.py <action>")
    print("Available actions: login, purchase, search")
    sys.exit(1)

action = sys.argv[1]
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.sendto(action.encode(), (TRIGGER_HOST, TRIGGER_PORT))
print(f"Triggered: {action}")
