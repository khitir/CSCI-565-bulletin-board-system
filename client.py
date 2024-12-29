# client.py

import socket
import json
import random
import time
import sys
import uuid
import datetime

class Client:
    def __init__(self, server_addresses, consistency_model):
        self.server_addresses = server_addresses
        self.current_server = self.choose_server()
        self.client_id = str(uuid.uuid4())
        self.write_counter = 0  # Used in Read-Your-Writes consistency
        self.consistency_model = consistency_model
        print(f"Client ID: {self.client_id}")
        print(f"Consistency Model: {self.consistency_model}")

    def choose_server(self):
        print("Available servers:")
        for idx, addr in enumerate(self.server_addresses):
            print(f"{idx + 1}. {addr}")
        choice = input("Select a server to connect to (by number): ")
        try:
            choice = int(choice) - 1
            if 0 <= choice < len(self.server_addresses):
                selected_server = self.server_addresses[choice]
                print(f"Connecting to server: {selected_server}")
                return selected_server
            else:
                print("Invalid choice. Selecting a random server.")
        except:
            print("Invalid input. Selecting a random server.")
        selected_server = random.choice(self.server_addresses)
        print(f"Connecting to server: {selected_server}")
        return selected_server

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(self.current_server)
        if self.consistency_model == 'read_your_writes':
            # Send client ID and write counter
            message = {'type': 'client_connect', 'client_id': self.client_id, 'write_counter': self.write_counter}
            self.sock.sendall((json.dumps(message) + '\n').encode())
            data = self.sock.recv(4096)
            response = json.loads(data.decode())
            if response['type'] != 'connect_ack':
                print(f"Error connecting to server: {response.get('message')}")

    def send_message(self, message):
        try:
            start_time = datetime.datetime.now()
            # Introduce random delay to simulate network delay
            time.sleep(random.uniform(0, 2))
            self.connect()
            self.sock.sendall((json.dumps(message) + '\n').encode())
            data = self.sock.recv(4096)
            self.sock.close()
            response = json.loads(data.decode())
            end_time = datetime.datetime.now()
            operation_time = (end_time - start_time).total_seconds()
            print(f"Operation took {operation_time} seconds")
            return response
        except Exception as e:
            print(f"Error communicating with server {self.current_server}: {e}")
            return {'type': 'error', 'message': 'Communication error'}

    def post_article(self, title, content):
        if self.consistency_model == 'read_your_writes':
            self.write_counter += 1
            message = {'type': 'post_article', 'client_id': self.client_id, 'write_counter': self.write_counter, 'title': title, 'content': content}
        else:
            message = {'type': 'post_article', 'title': title, 'content': content}
        response = self.send_message(message)
        if response['type'] == 'post_success':
            print(f"Article posted with ID {response['article_id']}")
        else:
            print(f"Error posting article: {response.get('message')}")

    def reply_article(self, parent_id, title, content):
        if self.consistency_model == 'read_your_writes':
            self.write_counter += 1
            message = {'type': 'reply_article', 'client_id': self.client_id, 'write_counter': self.write_counter, 'parent_id': parent_id, 'title': title, 'content': content}
        else:
            message = {'type': 'reply_article', 'parent_id': parent_id, 'title': title, 'content': content}
        response = self.send_message(message)
        if response['type'] == 'post_success':
            print(f"Reply posted with ID {response['article_id']}")
        else:
            print(f"Error posting reply: {response.get('message')}")

    def read_articles(self):
        message = {'type': 'read_articles'}
        response = self.send_message(message)
        if response['type'] == 'articles_list':
            articles = response['articles']
            self.display_articles(articles)
        else:
            print(f"Error reading articles: {response.get('message')}")

    def read_article_content(self, article_id):
        message = {'type': 'read_article_content', 'article_id': article_id}
        response = self.send_message(message)
        if response['type'] == 'article_content':
            article = response['article']
            print(f"ID: {article['id']}")
            print(f"Title: {article['title']}")
            print(f"Content: {article['content']}")
        else:
            print(f"Error reading article content: {response.get('message')}")

    def display_articles(self, articles):
        # Build a mapping from article ID to article data
        article_map = {a['id']: a for a in articles}
        # Build a tree structure
        tree = {}
        for article in articles:
            tree.setdefault(article['parent_id'], []).append(article)
        # Function to print articles recursively
        def print_articles(parent_id, level):
            for article in tree.get(parent_id, []):
                indent = '  ' * level
                print(f"{indent}{article['id']} {article['title']}")
                print_articles(article['id'], level + 1)
        # Start printing from root articles (parent_id=None)
        print_articles(None, 0)

    def menu(self):
        while True:
            print("\nMenu:")
            print("1. Post an article")
            print("2. Read list of articles")
            print("3. Read article content")
            print("4. Reply to an article")
            print("5. Switch server")
            print("6. Quit")
            choice = input("Enter your choice: ")
            if choice == '1':
                title = input("Enter title: ")
                content = input("Enter content: ")
                self.post_article(title, content)
            elif choice == '2':
                self.read_articles()
            elif choice == '3':
                article_id = int(input("Enter article ID: "))
                self.read_article_content(article_id)
            elif choice == '4':
                parent_id = int(input("Enter parent article ID: "))
                title = input("Enter title: ")
                content = input("Enter content: ")
                self.reply_article(parent_id, title, content)
            elif choice == '5':
                self.current_server = self.choose_server()
            elif choice == '6':
                break
            else:
                print("Invalid choice")

if __name__ == '__main__':
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

    # Collect server addresses
    server_addresses = []
    print("Enter server addresses (host:port). Type 'done' when finished.")
    while True:
        addr_input = input("Server address: ")
        if addr_input.lower() == 'done':
            break
        try:
            host_port = addr_input.split(':')
            server_host = host_port[0]
            server_port = int(host_port[1])
            server_addresses.append((server_host, server_port))
        except:
            print("Invalid format. Please enter in host:port format.")

    if len(server_addresses) == 0:
        print("At least one server address is required.")
        sys.exit(1)

    client = Client(server_addresses, consistency_model)
    client.menu()
