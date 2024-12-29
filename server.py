# server.py

import socket
import threading
import json
import time
import random
import sys

class Article:
    def __init__(self, article_id, parent_id, title, content, client_id=None, write_counter=None):
        self.id = article_id
        self.parent_id = parent_id
        self.title = title
        self.content = content
        self.client_id = client_id  # Used in Read-Your-Writes consistency
        self.write_counter = write_counter  # Used in Read-Your-Writes consistency

class Server:
    def __init__(self, host, port, coordinator_address, server_addresses, consistency_model, N, NR, NW, is_coordinator=False):
        self.host = host
        self.port = port
        self.coordinator_address = coordinator_address
        self.server_addresses = server_addresses  # List of (host, port) tuples
        self.is_coordinator = is_coordinator
        self.consistency_model = consistency_model  # 'sequential', 'read_your_writes', 'quorum'
        self.N = N
        self.NR = NR
        self.NW = NW
        self.next_article_id = 1  # Only used by coordinator
        self.articles = []  # List of Article objects
        self.lock = threading.Lock()  # For synchronizing access to articles
        self.client_write_counters = {}  # Used in Read-Your-Writes consistency
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"Server started on {self.host}:{self.port} {'(Coordinator)' if self.is_coordinator else ''}")
        print(f"Consistency Model: {self.consistency_model}")

    def start(self):
        threading.Thread(target=self.accept_connections).start()
        if self.consistency_model == 'read_your_writes':
            threading.Thread(target=self.propagate_articles).start()
        elif self.consistency_model == 'quorum':
            threading.Thread(target=self.synchronize).start()

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
            if self.consistency_model == 'read_your_writes':
                return self.handle_client_connect(message)
            else:
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
            if self.consistency_model == 'sequential':
                return self.handle_new_article_sequential(message)
            elif self.consistency_model == 'read_your_writes':
                return self.handle_new_article_read_your_writes(message)
            else:
                return {'type': 'error', 'message': 'Invalid message type for this consistency model'}
        elif msg_type == 'write_article':
            if self.consistency_model == 'quorum':
                return self.handle_write_article(message)
            else:
                return {'type': 'error', 'message': 'Invalid message type for this consistency model'}
        elif msg_type == 'get_articles':
            return self.handle_get_articles(message)
        elif msg_type == 'get_article_content':
            return self.handle_get_article_content(message)
        elif msg_type == 'request_missing_articles':
            if self.consistency_model == 'read_your_writes':
                return self.handle_request_missing_articles(message)
            else:
                return {'type': 'error', 'message': 'Invalid message type for this consistency model'}
        elif msg_type == 'send_missing_articles':
            if self.consistency_model == 'read_your_writes':
                return self.handle_send_missing_articles(message)
            else:
                return {'type': 'error', 'message': 'Invalid message type for this consistency model'}
        elif msg_type == 'get_next_article_id':
            return self.handle_get_next_article_id(message)
        elif msg_type == 'new_articles':
            if self.consistency_model == 'read_your_writes':
                return self.handle_new_articles(message)
            else:
                return {'type': 'error', 'message': 'Invalid message type for this consistency model'}
        else:
            return {'type': 'error', 'message': 'Unknown message type'}

    # Sequential Consistency Methods

    def handle_post_article_sequential(self, message):
        if self.is_coordinator:
            title = message.get('title')
            content = message.get('content')
            with self.lock:
                article_id = self.next_article_id
                self.next_article_id += 1
                article = Article(article_id, None, title, content)
                self.articles.append(article)
                # Propagate to backups
                self.propagate_article_sequential(article)
            return {'type': 'post_success', 'article_id': article_id}
        else:
            # Forward to coordinator
            response = self.forward_to_coordinator(message)
            return response

    def handle_reply_article_sequential(self, message):
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
                self.propagate_article_sequential(article)
            return {'type': 'post_success', 'article_id': article_id}
        else:
            # Forward to coordinator
            response = self.forward_to_coordinator(message)
            return response

    def propagate_article_sequential(self, article):
        # Send article to all backups
        message = {'type': 'new_article', 'article': {'id': article.id, 'parent_id': article.parent_id, 'title': article.title, 'content': article.content}}
        for addr in self.server_addresses:
            if addr != (self.host, self.port):  # Do not send to self
                try:
                    self.send_message(addr, message)
                except Exception as e:
                    print(f"Error propagating article to {addr}: {e}")

    def handle_new_article_sequential(self, message):
        article_data = message.get('article')
        article = Article(article_data['id'], article_data['parent_id'], article_data['title'], article_data['content'])
        with self.lock:
            self.articles.append(article)
        return {'type': 'article_ack', 'article_id': article.id}

    def handle_read_articles_sequential(self, message):
        with self.lock:
            articles_list = [{'id': a.id, 'parent_id': a.parent_id, 'title': a.title} for a in self.articles]
        return {'type': 'articles_list', 'articles': articles_list}

    def handle_read_article_content_sequential(self, message):
        article_id = message.get('article_id')
        with self.lock:
            article = next((a for a in self.articles if a.id == article_id), None)
        if article:
            return {'type': 'article_content', 'article': {'id': article.id, 'parent_id': article.parent_id, 'title': article.title, 'content': article.content}}
        else:
            return {'type': 'error', 'message': 'Article not found'}

    # Read-Your-Writes Consistency Methods

    def handle_client_connect(self, message):
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

    def handle_post_article_read_your_writes(self, message):
        client_id = message.get('client_id')
        write_counter = message.get('write_counter')
        title = message.get('title')
        content = message.get('content')
        # Get article ID from coordinator
        article_id = self.get_next_article_id()
        if article_id is None:
            return {'type': 'error', 'message': 'Failed to get article ID from coordinator'}
        with self.lock:
            article = Article(article_id, None, title, content, client_id, write_counter)
            self.articles.append(article)
            # Update client write counter
            self.client_write_counters[client_id] = write_counter
        return {'type': 'post_success', 'article_id': article_id}

    def handle_reply_article_read_your_writes(self, message):
        client_id = message.get('client_id')
        write_counter = message.get('write_counter')
        parent_id = message.get('parent_id')
        title = message.get('title')
        content = message.get('content')
        # Get article ID from coordinator
        article_id = self.get_next_article_id()
        if article_id is None:
            return {'type': 'error', 'message': 'Failed to get article ID from coordinator'}
        with self.lock:
            article = Article(article_id, parent_id, title, content, client_id, write_counter)
            self.articles.append(article)
            # Update client write counter
            self.client_write_counters[client_id] = write_counter
        return {'type': 'post_success', 'article_id': article_id}

    def handle_new_article_read_your_writes(self, message):
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

    def handle_new_articles(self, message):
        articles = message.get('articles')
        with self.lock:
            for article_data in articles:
                article_id = article_data['id']
                if not any(a.id == article_id for a in self.articles):
                    article = Article(article_data['id'], article_data['parent_id'], article_data['title'], article_data['content'], article_data['client_id'], article_data['write_counter'])
                    self.articles.append(article)
                    # Update client write counter
                    client_id = article_data['client_id']
                    write_counter = article_data['write_counter']
                    self.client_write_counters[client_id] = max(self.client_write_counters.get(client_id, 0), write_counter)
        return {'type': 'ack'}

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

    def handle_read_articles_read_your_writes(self, message):
        with self.lock:
            articles_list = [{'id': a.id, 'parent_id': a.parent_id, 'title': a.title} for a in self.articles]
        return {'type': 'articles_list', 'articles': articles_list}

    def handle_read_article_content_read_your_writes(self, message):
        article_id = message.get('article_id')
        with self.lock:
            article = next((a for a in self.articles if a.id == article_id), None)
        if article:
            return {'type': 'article_content', 'article': {'id': article.id, 'parent_id': article.parent_id, 'title': article.title, 'content': article.content}}
        else:
            return {'type': 'error', 'message': 'Article not found'}

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

    def handle_get_next_article_id(self, message):
        if self.is_coordinator:
            with self.lock:
                article_id = self.next_article_id
                self.next_article_id += 1
            return {'type': 'next_article_id', 'article_id': article_id}
        else:
            return {'type': 'error', 'message': 'Not coordinator'}

    # Quorum Consistency Methods

    def handle_post_article_quorum(self, message):
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

    def handle_reply_article_quorum(self, message):
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

    def handle_read_articles_quorum(self, message):
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

    def handle_read_article_content_quorum(self, message):
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

    def select_quorum(self, quorum_size):
        servers = self.server_addresses.copy()
        # Randomly select quorum_size servers
        quorum = random.sample(servers, min(quorum_size, len(servers)))
        return quorum

    # Common Methods

    def handle_post_article(self, message):
        if self.consistency_model == 'sequential':
            return self.handle_post_article_sequential(message)
        elif self.consistency_model == 'read_your_writes':
            return self.handle_post_article_read_your_writes(message)
        elif self.consistency_model == 'quorum':
            return self.handle_post_article_quorum(message)
        else:
            return {'type': 'error', 'message': 'Unknown consistency model'}

    def handle_reply_article(self, message):
        if self.consistency_model == 'sequential':
            return self.handle_reply_article_sequential(message)
        elif self.consistency_model == 'read_your_writes':
            return self.handle_reply_article_read_your_writes(message)
        elif self.consistency_model == 'quorum':
            return self.handle_reply_article_quorum(message)
        else:
            return {'type': 'error', 'message': 'Unknown consistency model'}

    def handle_read_articles(self, message):
        if self.consistency_model == 'sequential':
            return self.handle_read_articles_sequential(message)
        elif self.consistency_model == 'read_your_writes':
            return self.handle_read_articles_read_your_writes(message)
        elif self.consistency_model == 'quorum':
            return self.handle_read_articles_quorum(message)
        else:
            return {'type': 'error', 'message': 'Unknown consistency model'}

    def handle_read_article_content(self, message):
        if self.consistency_model == 'sequential':
            return self.handle_read_article_content_sequential(message)
        elif self.consistency_model == 'read_your_writes':
            return self.handle_read_article_content_read_your_writes(message)
        elif self.consistency_model == 'quorum':
            return self.handle_read_article_content_quorum(message)
        else:
            return {'type': 'error', 'message': 'Unknown consistency model'}

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

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: python server.py host port is_coordinator")
        sys.exit(1)
    host = sys.argv[1]
    port = int(sys.argv[2])
    is_coordinator = sys.argv[3].lower() == 'true'

    # Prompt the user to select the consistency model
    print("Select the consistency model:")
    print("1. Sequential Consistency")
    print("2. Read-Your-Writes Consistency")
    print("3. Quorum Consistency")
    choice = input("Enter your choice (1-3): ")

    if choice == '1':
        consistency_model = 'sequential'
    elif choice == '2':
        consistency_model = 'read_your_writes'
    elif choice == '3':
        consistency_model = 'quorum'
    else:
        print("Invalid choice. Defaulting to Sequential Consistency.")
        consistency_model = 'sequential'

    # For quorum consistency, ask for NR and NW
    if consistency_model == 'quorum':
        N = int(input("Enter the total number of servers (N): "))
        NR = int(input("Enter the read quorum size (NR): "))
        NW = int(input("Enter the write quorum size (NW): "))
        # Validate quorum constraints
        if NR + NW <= N or NW <= N / 2:
            print("Invalid quorum sizes. They must satisfy NR + NW > N and NW > N/2.")
            sys.exit(1)
    else:
        N = 3  # Default value
        NR = 2  # Default value
        NW = 2  # Default value

    # Include the current server's address
    server_addresses = [(host, port)]

    # Collect other server addresses
    print("Enter addresses of other servers (host:port). Type 'done' when finished.")
    while True:
        addr_input = input("Server address: ")
        if addr_input.lower() == 'done':
            break
        try:
            host_port = addr_input.split(':')
            server_host = host_port[0]
            server_port = int(host_port[1])
            if (server_host, server_port) != (host, port):
                server_addresses.append((server_host, server_port))
            else:
                print("This is the current server's address. It is already included.")
        except:
            print("Invalid format. Please enter in host:port format.")

    if len(server_addresses) == 0:
        print("At least one server address is required.")
        sys.exit(1)

    # Set coordinator address
    if is_coordinator:
        coordinator_address = (host, port)
    else:
        # Ask for coordinator address
        coord_input = input("Enter coordinator address (host:port): ")
        try:
            coord_host_port = coord_input.split(':')
            coordinator_address = (coord_host_port[0], int(coord_host_port[1]))
        except:
            print("Invalid format for coordinator address.")
            sys.exit(1)

    server = Server(host, port, coordinator_address, server_addresses, consistency_model, N, NR, NW, is_coordinator)
    server.start()
