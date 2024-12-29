# server.py

import socket
import threading
import json
import time
import random
import sys

class Article:
    def __init__(self, article_id, parent_id, title, content):
        self.id = article_id
        self.parent_id = parent_id
        self.title = title
        self.content = content

class Server:
    def __init__(self, host, port, coordinator_address, server_addresses, is_coordinator=False):
        self.host = host
        self.port = port
        self.coordinator_address = coordinator_address
        self.server_addresses = server_addresses  # List of (host, port) tuples
        self.is_coordinator = is_coordinator
        self.next_article_id = 1  # Only used by coordinator
        self.articles = []  # List of Article objects
        self.lock = threading.Lock()  # For synchronizing access to articles
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"Server started on {self.host}:{self.port} {'(Coordinator)' if self.is_coordinator else ''}")

    def start(self):
        threading.Thread(target=self.accept_connections).start()

    def accept_connections(self):
        while True:
            client_sock, addr = self.server_socket.accept()
            threading.Thread(target=self.handle_connection, args=(client_sock,)).start()

    def handle_connection(self, conn):
        try:
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                # Introduce random delay to simulate network delay
                time.sleep(random.uniform(0, 2))
                message = json.loads(data.decode())
                # Handle message based on 'type'
                response = self.handle_message(message)
                if response:
                    conn.sendall(json.dumps(response).encode())
        except Exception as e:
            print(f"Exception in handle_connection: {e}")
        finally:
            conn.close()

    def handle_message(self, message):
        msg_type = message.get('type')
        if msg_type == 'post_article':
            return self.handle_post_article(message)
        elif msg_type == 'reply_article':
            return self.handle_reply_article(message)
        elif msg_type == 'read_articles':
            return self.handle_read_articles(message)
        elif msg_type == 'read_article_content':
            return self.handle_read_article_content(message)
        # Handle other message types
        elif msg_type == 'new_article':
            return self.handle_new_article(message)
        else:
            return {'type': 'error', 'message': 'Unknown message type'}

    # Define methods for handling different message types

    def handle_post_article(self, message):
        # Handle posting a new article
        # If coordinator, assign ID and propagate
        # If backup, forward to coordinator
        if self.is_coordinator:
            title = message.get('title')
            content = message.get('content')
            with self.lock:
                article_id = self.next_article_id
                self.next_article_id += 1
                article = Article(article_id, None, title, content)
                self.articles.append(article)
                # Propagate to backups
                self.propagate_article(article)
            return {'type': 'post_success', 'article_id': article_id}
        else:
            # Forward to coordinator
            response = self.forward_to_coordinator(message)
            return response

    def handle_reply_article(self, message):
        # Similar to handle_post_article, but with parent_id
        if self.is_coordinator:
            parent_id = message.get('parent_id')
            title = message.get('title')
            content = message.get('content')
            with self.lock:
                article_id = self.next_article_id
                self.next_article_id += 1
                article = Article(article_id, parent_id, title, content)
                self.articles.append(article)
                # Propagate to backups
                self.propagate_article(article)
            return {'type': 'post_success', 'article_id': article_id}
        else:
            # Forward to coordinator
            response = self.forward_to_coordinator(message)
            return response

    def handle_read_articles(self, message):
        # Return list of articles
        with self.lock:
            articles_list = [{'id': a.id, 'parent_id': a.parent_id, 'title': a.title} for a in self.articles]
        return {'type': 'articles_list', 'articles': articles_list}

    def handle_read_article_content(self, message):
        article_id = message.get('article_id')
        with self.lock:
            article = next((a for a in self.articles if a.id == article_id), None)
        if article:
            return {'type': 'article_content', 'article': {'id': article.id, 'parent_id': article.parent_id, 'title': article.title, 'content': article.content}}
        else:
            return {'type': 'error', 'message': 'Article not found'}

    def propagate_article(self, article):
        # Send article to all backups
        message = {'type': 'new_article', 'article': {'id': article.id, 'parent_id': article.parent_id, 'title': article.title, 'content': article.content}}
        for addr in self.server_addresses:
            if addr != (self.host, self.port):  # Do not send to self
                try:
                    self.send_message(addr, message)
                except Exception as e:
                    print(f"Error propagating article to {addr}: {e}")

    def handle_new_article(self, message):
        article_data = message.get('article')
        article = Article(article_data['id'], article_data['parent_id'], article_data['title'], article_data['content'])
        with self.lock:
            self.articles.append(article)
        return {'type': 'article_ack', 'article_id': article.id}

    def forward_to_coordinator(self, message):
        # Send message to coordinator and wait for response
        try:
            response = self.send_message(self.coordinator_address, message)
            return response
        except Exception as e:
            print(f"Error forwarding to coordinator: {e}")
            return {'type': 'error', 'message': 'Unable to contact coordinator'}

    def send_message(self, addr, message):
        # Send message to addr and return response
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(addr)
        s.sendall(json.dumps(message).encode())
        data = s.recv(4096)
        s.close()
        response = json.loads(data.decode())
        return response

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Usage: python server.py host port is_coordinator server_config_file")
        sys.exit(1)
    host = sys.argv[1]
    port = int(sys.argv[2])
    is_coordinator = sys.argv[3].lower() == 'true'
    config_file = sys.argv[4]
    with open(config_file, 'r') as f:
        config = json.load(f)
    coordinator_address = tuple(config['coordinator_address'])
    server_addresses = [tuple(addr) for addr in config['server_addresses']]
    server = Server(host, port, coordinator_address, server_addresses, is_coordinator)
    server.start()
