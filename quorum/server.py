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
    def __init__(self, host, port, coordinator_address, server_addresses, N, NR, NW, is_coordinator=False):
        self.host = host
        self.port = port
        self.coordinator_address = coordinator_address
        self.server_addresses = server_addresses  # List of (host, port) tuples
        self.is_coordinator = is_coordinator
        self.N = N
        self.NR = NR
        self.NW = NW
        self.next_article_id = 1  # Only used by coordinator
        self.articles = []  # List of Article objects
        self.lock = threading.Lock()  # For synchronizing access to articles
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"Server started on {self.host}:{self.port} {'(Coordinator)' if self.is_coordinator else ''}")

    def start(self):
        threading.Thread(target=self.accept_connections).start()
        threading.Thread(target=self.synchronize).start()  # Start periodic synchronization

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
                messages = data.decode().split('\n')
                for msg in messages:
                    if not msg:
                        continue
                    message = json.loads(msg)
                    # Handle message based on 'type'
                    response = self.handle_message(message)
                    if response:
                        conn.sendall((json.dumps(response) + '\n').encode())
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
        elif msg_type == 'write_article':
            return self.handle_write_article(message)
        elif msg_type == 'get_articles':
            return self.handle_get_articles(message)
        elif msg_type == 'get_article_content':
            return self.handle_get_article_content(message)
        else:
            return {'type': 'error', 'message': 'Unknown message type'}

    def handle_post_article(self, message):
        if self.is_coordinator:
            title = message.get('title')
            content = message.get('content')
            # Assign article ID
            with self.lock:
                article_id = self.next_article_id
                self.next_article_id += 1
            article = Article(article_id, None, title, content)
            # Form write quorum
            write_quorum = self.select_quorum(self.NW)
            # Send write request to write quorum
            write_message = {'type': 'write_article', 'article': {'id': article.id, 'parent_id': article.parent_id, 'title': article.title, 'content': article.content}}
            acknowledgments = []
            for server_addr in write_quorum:
                if server_addr == (self.host, self.port):
                    # Write to self
                    with self.lock:
                        self.articles.append(article)
                    acknowledgments.append(server_addr)
                else:
                    try:
                        response = self.send_message(server_addr, write_message)
                        if response.get('type') == 'write_ack':
                            acknowledgments.append(server_addr)
                    except Exception as e:
                        print(f"Error writing to server {server_addr}: {e}")
            # Check if we received acknowledgments from all NW servers
            if len(acknowledgments) >= self.NW:
                return {'type': 'post_success', 'article_id': article_id}
            else:
                return {'type': 'error', 'message': 'Failed to write to quorum'}
        else:
            # Forward to coordinator
            response = self.forward_to_coordinator(message)
            return response

    def handle_reply_article(self, message):
        if self.is_coordinator:
            parent_id = message.get('parent_id')
            title = message.get('title')
            content = message.get('content')
            # Assign article ID
            with self.lock:
                article_id = self.next_article_id
                self.next_article_id += 1
            article = Article(article_id, parent_id, title, content)
            # Form write quorum
            write_quorum = self.select_quorum(self.NW)
            # Send write request to write quorum
            write_message = {'type': 'write_article', 'article': {'id': article.id, 'parent_id': article.parent_id, 'title': article.title, 'content': article.content}}
            acknowledgments = []
            for server_addr in write_quorum:
                if server_addr == (self.host, self.port):
                    # Write to self
                    with self.lock:
                        self.articles.append(article)
                    acknowledgments.append(server_addr)
                else:
                    try:
                        response = self.send_message(server_addr, write_message)
                        if response.get('type') == 'write_ack':
                            acknowledgments.append(server_addr)
                    except Exception as e:
                        print(f"Error writing to server {server_addr}: {e}")
            # Check if we received acknowledgments from all NW servers
            if len(acknowledgments) >= self.NW:
                return {'type': 'post_success', 'article_id': article_id}
            else:
                return {'type': 'error', 'message': 'Failed to write to quorum'}
        else:
            # Forward to coordinator
            response = self.forward_to_coordinator(message)
            return response

    def handle_write_article(self, message):
        article_data = message.get('article')
        article = Article(article_data['id'], article_data['parent_id'], article_data['title'], article_data['content'])
        with self.lock:
            self.articles.append(article)
        return {'type': 'write_ack'}

    def handle_read_articles(self, message):
        # Form read quorum
        read_quorum = self.select_quorum(self.NR)
        # Collect articles from read quorum
        articles_dict = {}
        for server_addr in read_quorum:
            if server_addr == (self.host, self.port):
                with self.lock:
                    for article in self.articles:
                        articles_dict[article.id] = article
            else:
                try:
                    response = self.send_message(server_addr, {'type': 'get_articles'})
                    if response.get('type') == 'articles_list':
                        articles = response.get('articles', [])
                        for article_data in articles:
                            article_id = article_data['id']
                            if article_id not in articles_dict:
                                article = Article(article_data['id'], article_data['parent_id'], article_data['title'], article_data.get('content', ''))
                                articles_dict[article_id] = article
                    else:
                        print(f"Error reading from server {server_addr}: {response.get('message')}")
                except Exception as e:
                    print(f"Error reading from server {server_addr}: {e}")
        # Prepare articles list
        articles_list = [{'id': a.id, 'parent_id': a.parent_id, 'title': a.title} for a in articles_dict.values()]
        # Sort articles by ID
        articles_list.sort(key=lambda x: x['id'])
        return {'type': 'articles_list', 'articles': articles_list}

    def handle_read_article_content(self, message):
        article_id = message.get('article_id')
        # Form read quorum
        read_quorum = self.select_quorum(self.NR)
        # Collect article content from read quorum
        article = None
        for server_addr in read_quorum:
            if server_addr == (self.host, self.port):
                with self.lock:
                    article = next((a for a in self.articles if a.id == article_id), None)
                    if article:
                        break
            else:
                try:
                    response = self.send_message(server_addr, {'type': 'get_article_content', 'article_id': article_id})
                    if response.get('type') == 'article_content':
                        article_data = response.get('article')
                        article = Article(article_data['id'], article_data['parent_id'], article_data['title'], article_data['content'])
                        break
                except Exception as e:
                    print(f"Error reading article content from server {server_addr}: {e}")
        if article:
            return {'type': 'article_content', 'article': {'id': article.id, 'parent_id': article.parent_id, 'title': article.title, 'content': article.content}}
        else:
            return {'type': 'error', 'message': 'Article not found'}

    def handle_get_articles(self, message):
        with self.lock:
            articles_list = [{'id': a.id, 'parent_id': a.parent_id, 'title': a.title, 'content': a.content} for a in self.articles]
        return {'type': 'articles_list', 'articles': articles_list}

    def handle_get_article_content(self, message):
        article_id = message.get('article_id')
        with self.lock:
            article = next((a for a in self.articles if a.id == article_id), None)
        if article:
            return {'type': 'article_content', 'article': {'id': article.id, 'parent_id': article.parent_id, 'title': article.title, 'content': article.content}}
        else:
            return {'type': 'error', 'message': 'Article not found'}

    def select_quorum(self, quorum_size):
        servers = self.server_addresses.copy()
        # Randomly select quorum_size servers
        quorum = random.sample(servers, quorum_size)
        return quorum

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
        s.sendall((json.dumps(message) + '\n').encode())
        data = s.recv(4096)
        s.close()
        response = json.loads(data.decode())
        return response

    def synchronize(self):
        # Periodically synchronize articles with other servers
        while True:
            time.sleep(30)  # Synchronize every 30 seconds
            for server_addr in self.server_addresses:
                if server_addr != (self.host, self.port):
                    try:
                        response = self.send_message(server_addr, {'type': 'get_articles'})
                        if response.get('type') == 'articles_list':
                            articles = response.get('articles', [])
                            with self.lock:
                                for article_data in articles:
                                    article_id = article_data['id']
                                    existing_article = next((a for a in self.articles if a.id == article_id), None)
                                    if not existing_article:
                                        article = Article(article_data['id'], article_data['parent_id'], article_data['title'], article_data.get('content', ''))
                                        self.articles.append(article)
                        else:
                            print(f"Error during synchronization with {server_addr}: {response.get('message')}")
                    except Exception as e:
                        print(f"Error synchronizing with server {server_addr}: {e}")

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
    N = config.get('N', len(server_addresses))
    NR = config.get('NR', 2)
    NW = config.get('NW', 2)
    server = Server(host, port, coordinator_address, server_addresses, N, NR, NW, is_coordinator)
    server.start()
