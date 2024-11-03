# Use eventlet's monkey patching to work with async I/O
import eventlet
eventlet.monkey_patch()

import socketio
import os


# Create a Socket.IO client instance
sio = socketio.Client()

# Set the server URL (replace with your actual server URL)
SERVER_URL = 'https://message-logger-531584264755.us-central1.run.app'

@sio.event
def connect():
    print('Connected to the server')

@sio.event
def connect_error(data):
    print('Failed to connect to the server:', data)

@sio.event
def disconnect():
    print('Disconnected from the server')

# Define a handler for "new_conversation" events
@sio.on('new_conversation')
def on_new_conversation(data):
    print('New conversation event received:', data)

# Define a handler for "conversation_updated" events
@sio.on('conversation_updated')
def on_conversation_updated(data):
    print('Conversation updated event received:', data)

def main():
    try:
        # Attempt to connect to the Socket.IO server
        print(f'Attempting to connect to {SERVER_URL}...')
        sio.connect(SERVER_URL, transports=['websocket', 'polling'])
        # Keep the app running to listen for events
        sio.wait()
    except socketio.exceptions.ConnectionError as e:
        print(f'Connection error: {e}')
    except Exception as e:
        print(f'An error occurred: {e}')

if __name__ == '__main__':
    main()
