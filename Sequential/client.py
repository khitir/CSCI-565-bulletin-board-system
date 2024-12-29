# client.py

import socket
import json
import random
import time
import sys

class Client:
    def __init__(self, server_addresses):
        self.server_addresses = server_addresses
        self.current_server = random.choice(server_addresses)
        print(f"Connecting to server: {self.current_server}")  # Add this line

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(self.current_server)

    def send_message(self, message):
        #print(f"sending from server: {self.current_server}") 
        try:
            # Introduce random delay to simulate network delay
            time.sleep(random.uniform(0, 2))
            self.connect()
            self.sock.sendall(json.dumps(message).encode())
            data = self.sock.recv(4096)
            self.sock.close()
            response = json.loads(data.decode())
            return response
        except Exception as e:
            print(f"Error communicating with server {self.current_server}: {e}")
            return {'type': 'error', 'message': 'Communication error'}

    # Methods for client operations

    def post_article(self, title, content):
        message = {'type': 'post_article', 'title': title, 'content': content}
        response = self.send_message(message)
        if response['type'] == 'post_success':
            print(f"Article posted with ID {response['article_id']}")
        else:
            print(f"Error posting article: {response.get('message')}")

    def reply_article(self, parent_id, title, content):
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
            print("5. Quit")
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
                break
            else:
                print("Invalid choice")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python client.py server_config_file")
        sys.exit(1)
    config_file = sys.argv[1]
    with open(config_file, 'r') as f:
        config = json.load(f)
    server_addresses = [tuple(addr) for addr in config['server_addresses']]
    client = Client(server_addresses)
    client.menu()
