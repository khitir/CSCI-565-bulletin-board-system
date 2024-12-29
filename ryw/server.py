# server.py

import socket
import threading
import json
import time
import random
import sys

class Article:
    def __init__(self, article_id, parent_id, title, content, client_id, write_counter):
        self.id = article_id
        self.parent_id = parent_id
        self.title = title
        self.content = content
        self.client_id = client_id
        self.write_counter = write_counter  # The client's write counter when this article was written

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
        # Map from client_id to latest write_counter
        self.client_write_counters = {}
        print(f"Server started on {self.host}:{self.port} {'(Coordinator)' if self.is_coordinator else ''}")

    def start(self):
        threading.Thread(target=self.accept_connections).start()
        threading.Thread(target=self.propagate_articles).start()  # Start thread to propagate articles

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
        if msg_type == 'client_connect':
            client_id = message.get('client_id')
            client_write_counter = message.get('write_counter')
            # Check if we have all writes from this client
            missing_counters = []
            with self.lock:
                latest_counter = self.client_write_counters.get(client_id, 0)
                if latest_counter < client_write_counter:
                    missing_counters = list(range(latest_counter + 1, client_write_counter + 1))
            if missing_counters:
                # Request missing articles from other servers
                self.get_missing_articles(client_id, missing_counters)
            return {'type': 'connect_ack'}
        elif msg_type == 'post_article':
            return self.handle_post_article(message)
        elif msg_type == 'reply_article':
            return self.handle_reply_article(message)
        elif msg_type == 'read_articles':
            return self.handle_read_articles(message)
        elif msg_type == 'read_article_content':
            return self.handle_read_article_content(message)
        elif msg_type == 'new_article':
            return self.handle_new_article(message)
        elif msg_type == 'request_missing_articles':
            return self.handle_request_missing_articles(message)
        elif msg_type == 'send_missing_articles':
            return self.handle_send_missing_articles(message)
        elif msg_type == 'get_next_article_id':
            return self.handle_get_next_article_id(message)
        else:
            return {'type': 'error', 'message': 'Unknown message type'}

    def handle_post_article(self, message):
        # Handle posting a new article
        client_id = message.get('client_id')
        write_counter = message.get('write_counter')
        title = message.get('title')
        content = message.get('content')
        # Get article ID from coordinator
        article_id = self.get_next_article_id()
        with self.lock:
            article = Article(article_id, None, title, content, client_id, write_counter)
            self.articles.append(article)
            # Update client write counter
            self.client_write_counters[client_id] = write_counter
            # Prepare to propagate to other servers
            # Note: We can use a queue or list to store articles to propagate
        return {'type': 'post_success', 'article_id': article_id}

    def handle_reply_article(self, message):
        # Similar to handle_post_article, but with parent_id
        client_id = message.get('client_id')
        write_counter = message.get('write_counter')
        parent_id = message.get('parent_id')
        title = message.get('title')
        content = message.get('content')
        # Get article ID from coordinator
        article_id = self.get_next_article_id()
        with self.lock:
            article = Article(article_id, parent_id, title, content, client_id, write_counter)
            self.articles.append(article)
            # Update client write counter
            self.client_write_counters[client_id] = write_counter
            # Prepare to propagate to other servers
        return {'type': 'post_success', 'article_id': article_id}

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

    def handle_new_article(self, message):
        article_data = message.get('article')
        article = Article(article_data['id'], article_data['parent_id'], article_data['title'], article_data['content'], article_data['client_id'], article_data['write_counter'])
        with self.lock:
            self.articles.append(article)
            # Update client write counter
            client_id = article_data['client_id']
            write_counter = article_data['write_counter']
            self.client_write_counters[client_id] = max(self.client_write_counters.get(client_id, 0), write_counter)
        return {'type': 'article_ack', 'article_id': article.id}

    def propagate_articles(self):
        # Periodically propagate articles to other servers
        while True:
            time.sleep(5)  # Adjust as needed
            # For simplicity, we can send all articles to other servers
            with self.lock:
                articles_data = [{'id': a.id, 'parent_id': a.parent_id, 'title': a.title, 'content': a.content, 'client_id': a.client_id, 'write_counter': a.write_counter} for a in self.articles]
            message = {'type': 'new_articles', 'articles': articles_data}
            for addr in self.server_addresses:
                if addr != (self.host, self.port):  # Do not send to self
                    try:
                        self.send_message(addr, message)
                    except Exception as e:
                        print(f"Error propagating articles to {addr}: {e}")

    def get_missing_articles(self, client_id, missing_counters):
        # Request missing articles from other servers
        message = {'type': 'request_missing_articles', 'client_id': client_id, 'counters': missing_counters}
        for addr in self.server_addresses:
            if addr != (self.host, self.port):  # Do not request from self
                try:
                    response = self.send_message(addr, message)
                    if response['type'] == 'send_missing_articles':
                        articles = response['articles']
                        with self.lock:
                            for article_data in articles:
                                article = Article(article_data['id'], article_data['parent_id'], article_data['title'], article_data['content'], article_data['client_id'], article_data['write_counter'])
                                self.articles.append(article)
                                # Update client write counter
                                client_id = article_data['client_id']
                                write_counter = article_data['write_counter']
                                self.client_write_counters[client_id] = max(self.client_write_counters.get(client_id, 0), write_counter)
                except Exception as e:
                    print(f"Error requesting missing articles from {addr}: {e}")

    def handle_request_missing_articles(self, message):
        client_id = message.get('client_id')
        counters = message.get('counters')
        with self.lock:
            articles = [a for a in self.articles if a.client_id == client_id and a.write_counter in counters]
            articles_data = [{'id': a.id, 'parent_id': a.parent_id, 'title': a.title, 'content': a.content, 'client_id': a.client_id, 'write_counter': a.write_counter} for a in articles]
        return {'type': 'send_missing_articles', 'articles': articles_data}

    def handle_send_missing_articles(self, message):
        # Not needed in this implementation
        pass

    def handle_get_next_article_id(self, message):
        if self.is_coordinator:
            with self.lock:
                article_id = self.next_article_id
                self.next_article_id += 1
            return {'type': 'next_article_id', 'article_id': article_id}
        else:
            return {'type': 'error', 'message': 'Not coordinator'}

    def get_next_article_id(self):
        if self.is_coordinator:
            with self.lock:
                article_id = self.next_article_id
                self.next_article_id += 1
            return article_id
        else:
            # Request next article ID from coordinator
            message = {'type': 'get_next_article_id'}
            try:
                response = self.send_message(self.coordinator_address, message)
                if response['type'] == 'next_article_id':
                    return response['article_id']
                else:
                    raise Exception('Error getting article ID from coordinator')
            except Exception as e:
                print(f"Error getting article ID from coordinator: {e}")
                return None

    def send_message(self, addr, message):
        # Send message to addr and return response
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(addr)
        s.sendall((json.dumps(message) + '\n').encode())
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
