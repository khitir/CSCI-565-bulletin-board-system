import threading
import socket
import json

## Article structure
class Article:
    def __init__(self, id, title, content):
        self.id = id
        self.title = title
        self.content = content

## Server structure
class Server:
    def __init__(self, host, port, coordinator_host, coordinator_port):
        self.host = host
        self.port = port
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
        self.article_list = []
        self.lock = threading.Lock()

    def start(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))
        self.socket.listen()

        print(f"Server started on {self.host}:{self.port}")

        while True:
            client_socket, address = self.socket.accept()
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def handle_client(self, client_socket):
        request = json.loads(client_socket.recv(1024).decode())

        if request["type"] == "post":
            self.post_article(request["title"], request["content"], client_socket)
        elif request["type"] == "read":
            self.read_articles(client_socket)
        elif request["type"] == "reply":
            self.reply_article(request["id"], request["content"], client_socket)

    def post_article(self, title, content, client_socket):
        # Forward request to Coordinator
        coordinator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        coordinator_socket.connect((self.coordinator_host, self.coordinator_port))
        coordinator_socket.send(json.dumps({"type": "post", "title": title, "content": content}).encode())
        response = json.loads(coordinator_socket.recv(1024).decode())
        coordinator_socket.close()

        # Update article list
        with self.lock:
            self.article_list.append(Article(response["id"], title, content))

        client_socket.send(json.dumps({"success": True}).encode())

    def read_articles(self, client_socket):
        with self.lock:
            articles = [{"id": article.id, "title": article.title} for article in self.article_list]

        client_socket.send(json.dumps(articles).encode())

    def reply_article(self, id, content, client_socket):
        # Forward request to Coordinator
        coordinator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        coordinator_socket.connect((self.coordinator_host, self.coordinator_port))
        coordinator_socket.send(json.dumps({"type": "reply", "id": id, "content": content}).encode())
        response = json.loads(coordinator_socket.recv(1024).decode())
        coordinator_socket.close()

        # Update article list
        with self.lock:
            for article in self.article_list:
                if article.id == id:
                    article.content += "\n" + content
                    break

        client_socket.send(json.dumps({"success": True}).encode())

## Coordinator structure
class Coordinator(Server):
    def __init__(self, host, port):
        super().__init__(host, port, None, None)
        self.article_id_counter = 0

    def handle_client(self, client_socket):
        request = json.loads(client_socket.recv(1024).decode())

        if request["type"] == "post":
            self.post_article(request["title"], request["content"], client_socket)
        elif request["type"] == "reply":
            self.reply_article(request["id"], request["content"], client_socket)

    def post_article(self, title, content, client_socket):
        # Generate article ID
        article_id = self.article_id_counter
        self.article_id_counter += 1

        # Update article list
        with self.lock:
            self.article_list.append(Article(article_id, title, content))

        client_socket.send(json.dumps({"id": article_id}).encode())

    def reply_article(self, id, content, client_socket):
        # Update article list
        with self.lock:
            for article in self.article_list:
                if article.id == id:
                    article.content += "\n" + content
                    break

        client_socket.send(json.dumps({"success": True}).encode())



if __name__ == "__main__":
    server = Server("localhost", 6000, "localhost", 6001)
    server.start()